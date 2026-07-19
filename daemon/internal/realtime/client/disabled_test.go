package client

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// TestClientDetects403AndDoesNotHammer pins the P0-1 daemon-side fix: a
// dial rejected with HTTP 403 before the WebSocket upgrade completes must be
// recognized as ErrRealtimeDisabled (surfaced via ServerDisabled), not
// treated like an ordinary transient dial failure — which would otherwise
// retry on the normal (much shorter) exponential backoff and hammer an
// endpoint whose REALTIME_ENABLED flag cannot flip back on within
// milliseconds.
func TestClientDetects403AndDoesNotHammer(t *testing.T) {
	var dialCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialCount, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"realtime_disabled"}`))
	}))
	defer srv.Close()

	// A recheck interval well above this test client's normal reconnect
	// backoff (BackoffBase 5ms / BackoffMax 20ms, set by newTestClient) so
	// the two are clearly distinguishable: if ErrRealtimeDisabled were
	// (wrongly) treated as an ordinary transient dial failure, the normal
	// backoff would produce roughly 350ms/20ms ~= 17 dials in the window
	// below; obeying the recheck interval instead produces roughly
	// 350ms/100ms ~= 3-4.
	const recheck = 100 * time.Millisecond
	const window = 350 * time.Millisecond

	store := newTestOutbox(t)
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c := newTestClient(t, url, store, func(cfg *Config) {
		*cfg = WithTestDisabledRecheckInterval(*cfg, recheck)
	})

	waitFor(t, 2*time.Second, c.ServerDisabled)

	time.Sleep(window)
	if n := atomic.LoadInt32(&dialCount); n > 8 {
		t.Fatalf("dial count = %d over %v with a %v recheck interval; expected roughly window/interval (~3-4), not the ~17 a tight reconnect-backoff loop would produce", n, window, recheck)
	}
}

// TestClientServerDisabledClearsOnceServerAccepts asserts the other half of
// the truth table: once a dial actually completes hello (the server has
// re-enabled realtime), ServerDisabled must clear so the caller (see
// internal/uplink.Uplink.pollRealtimeMode) resumes realtime routing.
func TestClientServerDisabledClearsOnceServerAccepts(t *testing.T) {
	var attempts int32
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
		ctx := r.Context()
		_, data, err := ws.Read(ctx)
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
		if err := ws.Write(ctx, websocket.MessageText, encoded); err != nil {
			return
		}
		for {
			if _, _, err := ws.Read(ctx); err != nil {
				return
			}
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	store := newTestOutbox(t)
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c := newTestClient(t, url, store, func(cfg *Config) {
		*cfg = WithTestDisabledRecheckInterval(*cfg, 15*time.Millisecond)
	})

	waitFor(t, 2*time.Second, c.ServerDisabled)
	waitFor(t, 2*time.Second, c.Connected)
	if c.ServerDisabled() {
		t.Fatal("expected ServerDisabled to clear once a connection completed hello")
	}
}
