package uplink

import (
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
	// the task/message events (only task.log, which has no realtime
	// equivalent).
	time.Sleep(100 * time.Millisecond)
	u.Close()

	for _, e := range httpCap.all() {
		if e.Kind == protocol.TaskStart || e.Kind == protocol.MessageSend || e.Kind == protocol.MetricUpdate {
			t.Fatalf("HTTP ingest carried a %s event while the realtime flag was on (double write): %+v", e.Kind, e)
		}
	}
	foundLog := false
	for _, e := range httpCap.all() {
		if e.Kind == protocol.TaskLog {
			foundLog = true
		}
	}
	if !foundLog {
		t.Error("expected task.log to still flow over HTTP (it has no realtime equivalent)")
	}
}

// TestRealtimeFlagOffPreservesExistingBehavior asserts the zero-value
// (Realtime == nil) path is completely unaffected: everything still goes
// over HTTP, exactly as before this feature existed.
func TestRealtimeFlagOffPreservesExistingBehavior(t *testing.T) {
	var httpCap capture
	httpSrv := httptest.NewServer(httpCap.handler(t))
	defer httpSrv.Close()

	u := New(Config{ServerURL: httpSrv.URL, FlushInterval: time.Hour})
	u.Offer(ev(protocol.TaskStart, func(e *Event) { e.Title = "t" }))
	u.Close()

	got := httpCap.all()
	if len(got) != 1 || got[0].Kind != protocol.TaskStart {
		t.Fatalf("expected exactly one task.start over HTTP, got %+v", got)
	}
}
