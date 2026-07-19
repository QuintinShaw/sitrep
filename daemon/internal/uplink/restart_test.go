package uplink

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
	rtclient "github.com/QuintinShaw/sitrep/daemon/internal/realtime/client"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/rttest"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// TestOverflowSurvivesProcessRestartAndDeliversInOrder is the reviewer's
// exact combined scenario: a full outbox forces a reliable event into
// overflow, the Uplink (and the realtime Client, and the outbox handle
// underneath it) shut down exactly as a process exit would, a brand new
// process opens a brand new Store handle on the same file (not the same
// open handle — genuinely closed and reopened, so this exercises the real
// on-disk recovery path rather than an in-memory one), capacity frees once
// the still-unacked older event is finally acked, the overflowed event is
// promoted into the outbox in its original offer order, and it is
// delivered over realtime to a mock server — with the /v2 HTTP spy seeing
// nothing at all, at any point in the test (reliable events must never
// fork off the realtime path, restart or not).
func TestOverflowSurvivesProcessRestartAndDeliversInOrder(t *testing.T) {
	var httpCap capture
	httpSrv := httptest.NewServer(httpCap.handler(t))
	defer httpSrv.Close()

	dbPath := filepath.Join(t.TempDir(), "outbox.db")
	const deviceID = "device-1"
	const space = "space-1"

	msg := func(text string) Event {
		return ev(protocol.MessageSend, func(e *Event) { e.Text = text; e.Level = "info" })
	}

	// ---- "process 1": fill the (cap-1) outbox, overflow the second event,
	// then shut down without ever acking the first event — simulating a
	// crash/restart while the server (or the connection to it) is down.
	firstSeen := make(chan wire.MessageEventBody, 4)
	rtSrv1 := rttest.New(func(conn *rttest.Conn) {
		if _, err := conn.HelloAccept("sess-1", 1000); err != nil {
			t.Errorf("phase 1 HelloAccept: %v", err)
			return
		}
		for {
			env, err := conn.ReadEnvelope()
			if err == rttest.ErrPing {
				_ = conn.WritePong()
				continue
			}
			if err != nil {
				return
			}
			if env.Type != wire.TypeMessageEvent {
				continue
			}
			body, derr := wire.DecodeBody(env)
			if derr != nil {
				continue
			}
			select {
			case firstSeen <- body.(wire.MessageEventBody):
			default:
			}
			// Deliberately never ack: the event must still be sitting
			// unacked in the outbox when process 1 shuts down.
		}
	})

	store1, err := outbox.OpenWithMaxRows(dbPath, 1)
	if err != nil {
		t.Fatalf("OpenWithMaxRows: %v", err)
	}

	rt1 := rtclient.New(rtclient.Config{
		URL: rtSrv1.URL(), DeviceID: deviceID, Space: space, Outbox: store1,
	})

	u1 := New(Config{ServerURL: httpSrv.URL, FlushInterval: 15 * time.Millisecond, Realtime: rt1})

	// 1. Fills the cap-1 outbox (device_seq 1), delivered over WS, left
	// unacked on purpose.
	u1.Offer(msg("first"))
	select {
	case mb := <-firstSeen:
		if mb.Text != "first" {
			t.Fatalf("phase 1 saw %q, want %q", mb.Text, "first")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("realtime server never saw the first event")
	}

	// 2. Offer() enqueues synchronously (Enqueue is called directly from
	// the calling goroutine, not the flush loop — see routeToRealtime), so
	// by the time Offer returns the outbox-full check has already run: this
	// event is durably in outbox_overflow, not the outbox, no device_seq
	// allocated yet.
	u1.Offer(msg("second"))
	if n, err := store1.OverflowCount(context.Background()); err != nil || n != 1 {
		t.Fatalf("OverflowCount = %d, err = %v; want exactly 1 overflowed event after the outbox filled", n, err)
	}

	// 3. Shut down exactly as a process exit would: Uplink, then the
	// realtime Client, then the Store handle itself.
	u1.Close()
	rt1.Close()
	rtSrv1.Close()
	if err := store1.Close(); err != nil {
		t.Fatalf("store1.Close: %v", err)
	}

	// ---- "process 2": a brand new Store handle on the same file (never
	// reusing store1/rt1/u1) — this is what makes it a genuine restart test
	// rather than an in-memory one.
	secondSeen := make(chan wire.MessageEventBody, 4)
	rtSrv2 := rttest.New(func(conn *rttest.Conn) {
		if _, err := conn.HelloAccept("sess-2", 1000); err != nil {
			t.Errorf("phase 2 HelloAccept: %v", err)
			return
		}
		for {
			env, err := conn.ReadEnvelope()
			if err == rttest.ErrPing {
				_ = conn.WritePong()
				continue
			}
			if err != nil {
				return
			}
			if env.Type != wire.TypeMessageEvent {
				continue
			}
			body, derr := wire.DecodeBody(env)
			if derr != nil {
				continue
			}
			mb := body.(wire.MessageEventBody)
			select {
			case secondSeen <- mb:
			default:
			}
			// Ack everything this time: this is what frees the cap-1
			// outbox's one slot and lets Store.Ack's own promotion trigger
			// move the overflowed event in.
			_ = conn.Ack(mb.DeviceID, mb.DeviceSeq)
		}
	})
	defer rtSrv2.Close()

	store2, err := outbox.OpenWithMaxRows(dbPath, 1)
	if err != nil {
		t.Fatalf("reopen OpenWithMaxRows: %v", err)
	}
	defer store2.Close()

	// Reopening at the same cap must NOT have promoted the overflowed event
	// on its own: the outbox still holds the never-acked "first" row, so
	// there is no free capacity yet — promotion only happens once that row
	// is finally acked below.
	if n, err := store2.Count(context.Background()); err != nil || n != 1 {
		t.Fatalf("outbox count after reopen = %d, err = %v; want 1 (the still-unacked first event)", n, err)
	}
	if n, err := store2.OverflowCount(context.Background()); err != nil || n != 1 {
		t.Fatalf("overflow count after reopen = %d, err = %v; want 1 (the second event, still not promoted)", n, err)
	}

	rt2 := rtclient.New(rtclient.Config{
		URL: rtSrv2.URL(), DeviceID: deviceID, Space: space, Outbox: store2,
		ResendInterval: 20 * time.Millisecond,
	})
	defer rt2.Close()

	u2 := New(Config{ServerURL: httpSrv.URL, FlushInterval: 15 * time.Millisecond, Realtime: rt2})
	defer u2.Close()

	// 4/5. On reconnect, replayPending resends the still-unacked "first";
	// the mock server acks it, which frees the cap-1 outbox's one slot and
	// promotes "second" out of overflow (Store.Ack's own trigger) with the
	// next device_seq in this space's sequence (2) — never reusing seq 1,
	// and never skipping ahead of it. Assert both are delivered, in that
	// order, entirely over realtime.
	var got []string
	deadline := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case mb := <-secondSeen:
			got = append(got, mb.Text)
		case <-deadline:
			t.Fatalf("timed out waiting for both events after restart; got so far: %v", got)
		}
	}
	if got[0] != "first" || got[1] != "second" {
		t.Fatalf("post-restart delivery order = %v, want [first second] (the promoted event must not jump ahead of the still-unacked older one)", got)
	}

	waitForCond(t, 3*time.Second, func() bool {
		n, err := store2.Count(context.Background())
		return err == nil && n == 0
	})
	if n, err := store2.OverflowCount(context.Background()); err != nil || n != 0 {
		t.Fatalf("overflow count at end = %d, err = %v; want 0 (promoted and delivered)", n, err)
	}

	// Throughout the whole test — fill, overflow, restart, promotion,
	// delivery — the /v2 HTTP spy must never have carried either reliable
	// event: it is not idempotent for a viewer resuming from SpaceHub the
	// way /v3 is, so a fork here would be a silent, permanent miss.
	for _, e := range httpCap.all() {
		if e.Kind == protocol.MessageSend {
			t.Fatalf("HTTP /v2 ingest carried %q; reliable events must travel the realtime path only, restart or not", e.Text)
		}
	}
}
