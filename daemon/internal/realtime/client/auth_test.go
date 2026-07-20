package client

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/rttest"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// TestClientDial401NonRetryableAndHealth pins the MINOR fix: a 401 at the WS
// upgrade (revoked/expired token) is NON-retryable — the reconnect loop
// stops instead of hammering the server on the ordinary transient backoff —
// and it surfaces through OnAuthState so a revoked device becomes visible in
// the menubar's health file.
func TestClientDial401NonRetryableAndHealth(t *testing.T) {
	var dials int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dials, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	var authOK atomic.Bool
	authOK.Store(true)
	var reported atomic.Bool

	store := newTestOutbox(t)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	newTestClient(t, wsURL, store, func(cfg *Config) {
		cfg.OnAuthState = func(ok bool, reason string) {
			authOK.Store(ok)
			if !ok && reason != "" {
				reported.Store(true)
			}
		}
	})

	// The auth failure must be reported.
	waitFor(t, 2*time.Second, func() bool { return !authOK.Load() && reported.Load() })

	// And the reconnect loop must have STOPPED — a tight retry loop with the
	// 5ms/20ms test backoff would produce dozens of dials in this window.
	time.Sleep(300 * time.Millisecond)
	if n := atomic.LoadInt32(&dials); n > 3 {
		t.Fatalf("dial count = %d after a 401; expected the loop to stop (non-retryable), not hammer", n)
	}
}

// TestClientHTTP401ReportsAuthHealth pins that a persistent 401 on the HTTP
// /v1/events fallback (WS unreachable) also routes to OnAuthState — the auth
// failure is visible on the HTTP transport too, not just at WS dial.
func TestClientHTTP401ReportsAuthHealth(t *testing.T) {
	eventsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer eventsSrv.Close()

	var degraded atomic.Bool
	store := newTestOutbox(t)
	c := newTestClient(t, "ws://127.0.0.1:1/unreachable", store, func(cfg *Config) {
		cfg.EventsURL = eventsSrv.URL
		cfg.ResendInterval = 15 * time.Millisecond
		cfg.BackoffBase = 5 * time.Millisecond
		cfg.BackoffMax = 20 * time.Millisecond
		cfg.OnAuthState = func(ok bool, reason string) {
			if !ok {
				degraded.Store(true)
			}
		}
	})

	if err := c.SendTaskEvent(TaskEvent{TaskID: "run-1", Kind: "started", OccurredAt: time.Now()}); err != nil {
		t.Fatalf("SendTaskEvent: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return degraded.Load() })
}

// TestClientForwardedCommandNotConsumed pins the MAJOR's on-the-wire
// behavior: a valid, in-TTL reverse-control command is forwarded to
// OnCommand, and the client sends NOTHING back to the server in response (no
// ack, no consume) — so a handler that chooses to ignore the command (as the
// resident agent does for a task it doesn't run) leaves it available for the
// task owner's HTTP drain. Only the ping/pong heartbeat may cross the wire.
func TestClientForwardedCommandNotConsumed(t *testing.T) {
	var gotAction atomic.Value // string
	var nonHeartbeatFrames int32

	connReady := make(chan *rttest.Conn, 1)
	srv := rttest.New(func(conn *rttest.Conn) {
		if _, err := conn.HelloAccept("sess", 60000); err != nil {
			t.Errorf("HelloAccept: %v", err)
			return
		}
		connReady <- conn
		for {
			env, err := conn.ReadEnvelope()
			if err == rttest.ErrPing || err == rttest.ErrPong {
				continue // heartbeat is allowed
			}
			if err != nil {
				return
			}
			// Any decoded envelope from the client (ack/command/error/...) is
			// a "consume" we must not see for a forwarded command.
			_ = env
			atomic.AddInt32(&nonHeartbeatFrames, 1)
		}
	})
	defer srv.Close()

	store := newTestOutbox(t)
	newTestClient(t, srv.URL(), store, func(cfg *Config) {
		cfg.OnCommand = func(a CommandAction) { gotAction.Store(a.Action) }
	})

	var conn *rttest.Conn
	select {
	case conn = <-connReady:
	case <-time.After(3 * time.Second):
		t.Fatal("server never observed a connection")
	}

	env, err := wire.NewEnvelope(wire.TypeCommand, "cmd-env-1", rttest.NowMS(), wire.CommandBody{
		CommandID: "cmd-pause-1", Origin: "viewer", IssuedByDeviceID: "viewer-1",
		Action: "pause", TaskID: "task-A", TTLMs: 60000,
	})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	if err := conn.WriteEnvelope(env); err != nil {
		t.Fatalf("WriteEnvelope: %v", err)
	}

	// OnCommand receives it...
	waitFor(t, 2*time.Second, func() bool {
		v, _ := gotAction.Load().(string)
		return v == "pause"
	})

	// ...and the client sends nothing back to the server for it.
	time.Sleep(300 * time.Millisecond)
	if n := atomic.LoadInt32(&nonHeartbeatFrames); n != 0 {
		t.Fatalf("client sent %d non-heartbeat frame(s) in response to a forwarded command; want 0 (must not consume/ack)", n)
	}
}
