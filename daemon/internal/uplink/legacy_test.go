package uplink

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/QuintinShaw/sitrep/daemon/internal/protocol"
	rtclient "github.com/QuintinShaw/sitrep/daemon/internal/realtime/client"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

func waitForCond(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %v", timeout)
	}
}

// TestLegacyModeDrainsPendingAndNewEventsWhenServerDisabled is the P0-1
// integration test: when the server rejects /v3/realtime with HTTP 403
// realtime_disabled at dial time, the whole Uplink session must switch to
// legacy /v2 routing — including draining anything already sitting durably
// in the outbox from before this session even started (simulating a
// previous process run), so a terminal task state doesn't get stranded in a
// store no viewer reads. New events offered while still in legacy mode must
// also reach /v2, after the drained backlog, in order. No realtime frame
// may ever be sent (the dial never completes hello), and the dial must not
// be hammered.
func TestLegacyModeDrainsPendingAndNewEventsWhenServerDisabled(t *testing.T) {
	var httpCap capture
	httpSrv := httptest.NewServer(httpCap.handler(t))
	defer httpSrv.Close()

	var dialCount int32
	rtSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialCount, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"realtime_disabled"}`))
	}))
	defer rtSrv.Close()

	store, err := outbox.Open(filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatalf("outbox.Open: %v", err)
	}
	defer store.Close()

	// Pre-populate the outbox as if a previous process run had already
	// durably enqueued this reliable event before the server was known to
	// be disabled.
	ctx := context.Background()
	if _, err := store.Enqueue(ctx, "space-1", wire.TypeMessageEvent, func(seq int64) (json.RawMessage, error) {
		return json.Marshal(wire.MessageEventBody{
			DeviceID: "device-1", DeviceSeq: seq, MessageID: fmt.Sprintf("device-1:%d", seq),
			Level: "info", Text: "preexisting", OccurredAt: time.Now().UnixMilli(),
		})
	}); err != nil {
		t.Fatalf("pre-populate Enqueue: %v", err)
	}

	rt := rtclient.New(rtclient.Config{
		URL:      "ws" + strings.TrimPrefix(rtSrv.URL, "http"),
		DeviceID: "device-1",
		Space:    "space-1",
		Outbox:   store,
	})
	defer rt.Close()

	u := New(Config{ServerURL: httpSrv.URL, FlushInterval: 15 * time.Millisecond, Realtime: rt})
	defer u.Close()

	waitForCond(t, 2*time.Second, rt.ServerDisabled)

	// New reliable events offered while the session is in legacy mode must
	// also reach /v2, after the drained backlog.
	u.Offer(ev(protocol.MessageSend, func(e *Event) { e.Text = "second"; e.Level = "info" }))
	u.Offer(ev(protocol.MessageSend, func(e *Event) { e.Text = "third"; e.Level = "info" }))

	waitForCond(t, 3*time.Second, func() bool { return len(httpCap.all()) >= 3 })

	got := httpCap.all()
	if len(got) != 3 {
		t.Fatalf("got %d events over /v2, want exactly 3: %+v", len(got), got)
	}
	want := []string{"preexisting", "second", "third"}
	for i, w := range want {
		if got[i].Text != w {
			t.Fatalf("event %d = %q, want %q (full order: %v)", i, got[i].Text, w, got)
		}
	}

	if n := atomic.LoadInt32(&dialCount); n > 8 {
		t.Fatalf("dial count = %d; expected no rapid redial while the server reports realtime disabled", n)
	}
}

// TestLegacyModeResumesRealtimeAfterServerReenables asserts the switch-back
// half: once the realtime client's periodic re-probe succeeds (the server
// re-enables /v3/realtime), the Uplink resumes routing new reliable events
// to realtime and the /v2 spy stops growing.
func TestLegacyModeResumesRealtimeAfterServerReenables(t *testing.T) {
	var httpCap capture
	httpSrv := httptest.NewServer(httpCap.handler(t))
	defer httpSrv.Close()

	var attempts int32
	rtReceived := make(chan wire.Envelope, 16)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n <= 2 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"realtime_disabled"}`))
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close(websocket.StatusNormalClosure, "done")
		wsCtx := r.Context()
		_, data, err := ws.Read(wsCtx)
		if err != nil {
			return
		}
		env, err := wire.DecodeEnvelope(data)
		if err != nil || env.Type != wire.TypeHello {
			return
		}
		accept := wire.HelloAccept{Stage: "accept", ProtocolVersion: 1, SessionID: "sess", HeartbeatIntervalMS: 1000}
		acceptEnv, err := wire.NewEnvelope(wire.TypeHello, "accept-env-1", time.Now().UnixMilli(), wire.HelloBody{Accept: &accept})
		if err != nil {
			return
		}
		encoded, err := acceptEnv.Encode()
		if err != nil {
			return
		}
		if err := ws.Write(wsCtx, websocket.MessageText, encoded); err != nil {
			return
		}
		for {
			_, data, err := ws.Read(wsCtx)
			if err != nil {
				return
			}
			if string(data) == "ping" {
				_ = ws.Write(wsCtx, websocket.MessageText, []byte("pong"))
				continue
			}
			e, derr := wire.DecodeEnvelope(data)
			if derr != nil {
				continue
			}
			rtReceived <- e
			if e.Type == wire.TypeMessageEvent {
				body, berr := wire.DecodeBody(e)
				if berr == nil {
					mb := body.(wire.MessageEventBody)
					ackEnv, aerr := wire.NewEnvelope(wire.TypeAck, "ack-1", time.Now().UnixMilli(),
						wire.AckBody{Acked: []wire.AckedPair{{DeviceID: mb.DeviceID, DeviceSeq: mb.DeviceSeq}}})
					if aerr == nil {
						if encoded, eerr := ackEnv.Encode(); eerr == nil {
							_ = ws.Write(wsCtx, websocket.MessageText, encoded)
						}
					}
				}
			}
		}
	})
	rtSrv := httptest.NewServer(mux)
	defer rtSrv.Close()

	store, err := outbox.Open(filepath.Join(t.TempDir(), "outbox.db"))
	if err != nil {
		t.Fatalf("outbox.Open: %v", err)
	}
	defer store.Close()

	rt := rtclient.New(rtclient.WithTestDisabledRecheckInterval(rtclient.Config{
		URL:            "ws" + strings.TrimPrefix(rtSrv.URL, "http"),
		DeviceID:       "device-1",
		Space:          "space-1",
		Outbox:         store,
		ResendInterval: 20 * time.Millisecond,
	}, 30*time.Millisecond))
	defer rt.Close()

	u := New(Config{ServerURL: httpSrv.URL, FlushInterval: 15 * time.Millisecond, Realtime: rt})
	defer u.Close()

	waitForCond(t, 2*time.Second, rt.ServerDisabled)
	waitForCond(t, 3*time.Second, func() bool { return !rt.ServerDisabled() })
	waitForCond(t, 3*time.Second, rt.Connected)
	// pollRealtimeMode (the Uplink's own tick-driven poll of
	// rt.ServerDisabled()) needs one more flush tick after the client-level
	// signal clears to flip the Uplink's own legacyMode back off; wait for
	// that directly rather than racing it with a fixed sleep.
	waitForCond(t, 2*time.Second, func() bool { return !u.inLegacyMode() })

	baseline := len(httpCap.all())

	u.Offer(ev(protocol.MessageSend, func(e *Event) { e.Text = "afterReenable"; e.Level = "info" }))

	select {
	case e := <-rtReceived:
		if e.Type != wire.TypeMessageEvent {
			t.Fatalf("expected message.event over realtime, got %s", e.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expected the new event to reach realtime once the server re-enabled it")
	}

	time.Sleep(100 * time.Millisecond)
	if got := len(httpCap.all()); got != baseline {
		t.Fatalf("expected the /v2 spy to stop growing once realtime resumed: was %d, now %d", baseline, got)
	}
}
