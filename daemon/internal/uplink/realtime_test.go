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

// TestRealtimeFlagRoutesEventsAwayFromHTTP is the integration check for the
// "flag on -> reliable events go over WS, HTTP ingest stops carrying them;
// flag off -> unchanged" requirement: with Config.Realtime set, task
// lifecycle and message.send events must reach the mock realtime server
// (never the HTTP /v2/ingest capture), while task.log keeps going over
// HTTP exactly as before.
func TestRealtimeFlagRoutesEventsAwayFromHTTP(t *testing.T) {
	var httpCap capture
	httpSrv := httptest.NewServer(httpCap.handler(t))
	defer httpSrv.Close()

	received := make(chan wire.Envelope, 16)
	rtSrv := rttest.New(func(conn *rttest.Conn) {
		if _, err := conn.HelloAccept("sess", 1000); err != nil {
			t.Errorf("HelloAccept: %v", err)
			return
		}
		for {
			env, err := conn.ReadEnvelope()
			if err == rttest.ErrPing {
				conn.WritePong()
				continue
			}
			if err != nil {
				return
			}
			received <- env
			if env.Type == wire.TypeTaskEvent || env.Type == wire.TypeMessageEvent {
				body, _ := wire.DecodeBody(env)
				switch b := body.(type) {
				case wire.TaskEventBody:
					conn.Ack(b.DeviceID, b.DeviceSeq)
				case wire.MessageEventBody:
					conn.Ack(b.DeviceID, b.DeviceSeq)
				}
			}
		}
	})
	defer rtSrv.Close()

	store, err := outbox.Open(filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatalf("outbox.Open: %v", err)
	}
	defer store.Close()

	rt := rtclient.New(rtclient.Config{
		URL:      rtSrv.URL(),
		DeviceID: "device-1",
		Space:    "space-1",
		Outbox:   store,
	})
	defer rt.Close()

	u := New(Config{ServerURL: httpSrv.URL, FlushInterval: 20 * time.Millisecond, Realtime: rt})
	defer u.Close()

	u.Offer(ev(protocol.TaskStart, func(e *Event) { e.Title = "Nightly backup" }))
	u.Offer(ev(protocol.MessageSend, func(e *Event) { e.Text = "hi"; e.Level = "info" }))
	u.Offer(ev(protocol.MetricUpdate, func(e *Event) { e.Key = "cpu.load"; e.Value = "0.5" }))
	u.LogLine("s1", "some output line")

	// The realtime server should see the task.event and message.event.
	seenTypes := map[string]bool{}
	deadline := time.After(2 * time.Second)
loop:
	for len(seenTypes) < 2 {
		select {
		case env := <-received:
			seenTypes[env.Type] = true
		case <-deadline:
			break loop
		}
	}
	if !seenTypes[wire.TypeTaskEvent] {
		t.Error("expected the realtime server to see a task.event")
	}
	if !seenTypes[wire.TypeMessageEvent] {
		t.Error("expected the realtime server to see a message.event")
	}

	// Give the HTTP flusher a chance to run, then assert it never carried
	// the task/message events over the telemetry batch (they went to
	// realtime); task.log, which has no realtime equivalent, went to its
	// own POST /v1/tasks/:id/log route instead.
	time.Sleep(100 * time.Millisecond)
	u.Close()

	if evs := httpCap.allEvents(); len(evs) != 0 {
		t.Fatalf("HTTP /v1/events carried telemetry while the realtime flag was on (double write): %+v", evs)
	}
	foundLog := false
	for _, lp := range httpCap.logs() {
		if lp.taskID == "s1" {
			for _, line := range lp.lines {
				if line == "some output line" {
					foundLog = true
				}
			}
		}
	}
	if !foundLog {
		t.Error("expected task.log to flow to POST /v1/tasks/s1/log (its v1 route, no realtime equivalent)")
	}
}

// TestRealtimeFlagOffPreservesExistingBehavior asserts the zero-value
// (Realtime == nil) path posts telemetry over HTTP /v1/events. With the
// local SITREP_REALTIME flag off, cmd/sitrep's newAgentRealtimeUplink never
// constructs an rtclient.Client at all (see its doc comment), so
// Config.Realtime is nil here exactly as it is in that path.
func TestRealtimeFlagOffPreservesExistingBehavior(t *testing.T) {
	var httpCap capture
	httpSrv := httptest.NewServer(httpCap.handler(t))
	defer httpSrv.Close()

	u := New(v1Config(httpSrv.URL, "", "", time.Hour))
	u.Offer(ev(protocol.TaskStart, func(e *Event) { e.Title = "t" }))
	u.Close()

	tasks := httpCap.byType(wire.TypeTaskEvent)
	if len(tasks) != 1 || tasks[0].Task == nil || tasks[0].Task.Kind != "started" {
		t.Fatalf("expected exactly one task.event{started} over /v1/events, got %+v", httpCap.allEvents())
	}
}

// TestOutboxFullNeverForksToHTTPAndRecovers pins the P0 fix: /v2 ingest
// writes UserStore while /v3 viewers resume from SpaceHub, so a reliable
// event (task.event/message.event) diverted to /v2 while the realtime flag
// is on could permanently vanish from a viewer's resume. When the realtime
// outbox hits its row cap, the overflow event must NOT reach the HTTP
// ingest spy at all — it is durably persisted into outbox_overflow (see
// outbox.ErrOverflowed) rather than rejected, and delivered over realtime,
// in the original offer order, once an ack frees outbox capacity and
// promotes it (outbox.Store.PromoteOverflow).
func TestOutboxFullNeverForksToHTTPAndRecovers(t *testing.T) {
	var httpCap capture
	httpSrv := httptest.NewServer(httpCap.handler(t))
	defer httpSrv.Close()

	wsMessages := make(chan wire.MessageEventBody, 16)
	ackRelease := make(chan struct{})
	rtSrv := rttest.New(func(conn *rttest.Conn) {
		if _, err := conn.HelloAccept("sess", 1000); err != nil {
			t.Errorf("HelloAccept: %v", err)
			return
		}
		released := false
		for {
			env, err := conn.ReadEnvelope()
			if err == rttest.ErrPing {
				conn.WritePong()
				continue
			}
			if err != nil {
				return
			}
			if env.Type != wire.TypeMessageEvent {
				continue
			}
			body, _ := wire.DecodeBody(env)
			mb := body.(wire.MessageEventBody)
			select {
			case wsMessages <- mb:
			default:
			}
			if !released {
				select {
				case <-ackRelease:
					released = true
				default:
				}
			}
			if released {
				conn.Ack(mb.DeviceID, mb.DeviceSeq)
			}
		}
	})
	defer rtSrv.Close()

	// Cap of 1: the first unacked reliable event fills the outbox.
	store, err := outbox.OpenWithMaxRows(filepath.Join(t.TempDir(), "outbox.db"), 1)
	if err != nil {
		t.Fatalf("OpenWithMaxRows: %v", err)
	}
	defer store.Close()

	rt := rtclient.New(rtclient.Config{
		URL:            rtSrv.URL(),
		DeviceID:       "device-1",
		Space:          "space-1",
		Outbox:         store,
		ResendInterval: 20 * time.Millisecond,
	})
	defer rt.Close()

	u := New(Config{ServerURL: httpSrv.URL, FlushInterval: 20 * time.Millisecond, Realtime: rt})
	defer u.Close()

	msg := func(text string) Event {
		return ev(protocol.MessageSend, func(e *Event) { e.Text = text; e.Level = "info" })
	}
	waitForWS := func(text string) {
		t.Helper()
		deadline := time.After(3 * time.Second)
		for {
			select {
			case mb := <-wsMessages:
				if mb.Text == text {
					return
				}
			case <-deadline:
				t.Fatalf("realtime server never saw message %q", text)
			}
		}
	}

	// 1. First event: fits in the outbox (row 1/1), goes over WS, unacked.
	u.Offer(msg("first"))
	waitForWS("first")

	// 2. Second event: outbox is full -> durably persisted into
	// outbox_overflow (ErrOverflowed). Per the P0 fix this must NOT fall
	// back to HTTP; it stays in overflow until an ack frees outbox capacity
	// and promotes it. Give it several flush intervals to (wrongly) reach
	// HTTP if the fallback regressed, then assert it never did.
	u.Offer(msg("second"))
	time.Sleep(200 * time.Millisecond)
	for _, e := range httpCap.byType(wire.TypeMessageEvent) {
		if e.Msg != nil && e.Msg.Text == "second" {
			t.Fatalf("HTTP /v1/events carried the overflow event %q while the outbox was full — reliable events must never fork off the realtime path", e.Msg.Text)
		}
	}

	// 3. The server starts acking; the resend loop replays "first", it gets
	// acked, and the outbox drains.
	close(ackRelease)
	waitFor := func(cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("condition not met within 3s")
	}
	waitFor(func() bool {
		n, err := store.Count(context.Background())
		return err == nil && n == 0
	})

	// 4. Once the outbox drains, the locally-queued "second" event's retry
	// succeeds and it is delivered over realtime — never having touched
	// HTTP — in the correct seq order (it was offered before "third").
	waitForWS("second")

	// 5. With capacity restored, the realtime path continues to take
	// subsequent events.
	u.Offer(msg("third"))
	waitForWS("third")

	// None of these reliable events may ever have gone over HTTP.
	if msgs := httpCap.byType(wire.TypeMessageEvent); len(msgs) != 0 {
		t.Fatalf("HTTP /v1/events carried %d message.event(s), which should travel the realtime path only", len(msgs))
	}
}
