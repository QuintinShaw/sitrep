package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/rttest"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// TestHTTPTransportRetiresViaAckedParity is the coverage item for "HTTP
// /v1/events uplink retiring rows via acked[] parity with WS ack": with no
// reachable WS endpoint at all (dial always fails), every reliable event
// must still be delivered and retired purely over the HTTP fallback,
// using acked[] exactly like the WS ack path (Client.retireAck is the
// single shared code path both call — see client.go).
func TestHTTPTransportRetiresViaAckedParity(t *testing.T) {
	var events rttest.AckingEvents
	eventsSrv := httptest.NewServer(events.Handler())
	defer eventsSrv.Close()

	store := newTestOutbox(t)
	c := newTestClient(t, "ws://127.0.0.1:1/unreachable", store, func(cfg *Config) {
		cfg.EventsURL = eventsSrv.URL
		cfg.ResendInterval = 15 * time.Millisecond
		cfg.BackoffBase = 5 * time.Millisecond
		cfg.BackoffMax = 20 * time.Millisecond
	})

	if err := c.SendTaskEvent(TaskEvent{TaskID: "run-1", Kind: "started", OccurredAt: time.Now()}); err != nil {
		t.Fatalf("SendTaskEvent: %v", err)
	}
	if err := c.SendMessageEvent(MessageEvent{Level: "info", Text: "hi", OccurredAt: time.Now()}); err != nil {
		t.Fatalf("SendMessageEvent: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		pending, err := store.Pending(context.Background(), testSpace)
		return err == nil && len(pending) == 0
	})

	if c.Connected() {
		t.Fatal("WS should never have connected in this test (unreachable dial target)")
	}

	received := events.Received()
	gotTypes := map[string]int{}
	for _, e := range received {
		gotTypes[e.Type]++
	}
	if gotTypes[wire.TypeTaskEvent] == 0 || gotTypes[wire.TypeMessageEvent] == 0 {
		t.Fatalf("expected the HTTP mock to observe both event types at least once, got %+v (full: %+v)", gotTypes, received)
	}
}

// TestWSTransportUnavailable503UsesHTTPWithoutTightRedial covers "503 on
// upgrade -> HTTP transport (no store switch, no tight redial)": a WS
// endpoint that always rejects the upgrade with 503
// {"error":"transport_unavailable"} must not be hammered (the client backs
// off to a long, fixed re-probe interval, not the ordinary short
// exponential backoff), while reliable events continue to reach the SAME
// space over HTTP the entire time — proving this is a transport
// degradation, never a "switch to some other store" fork.
func TestWSTransportUnavailable503UsesHTTPWithoutTightRedial(t *testing.T) {
	var dialCount int32
	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&dialCount, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"transport_unavailable"}`))
	}))
	defer wsSrv.Close()

	var events rttest.AckingEvents
	eventsSrv := httptest.NewServer(events.Handler())
	defer eventsSrv.Close()

	// A recheck interval well above this test client's normal reconnect
	// backoff, so the two are clearly distinguishable — see the original
	// P0-1 dial-count test this supersedes for the 403/legacy case.
	const recheck = 100 * time.Millisecond
	const window = 350 * time.Millisecond

	store := newTestOutbox(t)
	wsURL := "ws" + strings.TrimPrefix(wsSrv.URL, "http")
	c := newTestClient(t, wsURL, store, func(cfg *Config) {
		cfg.EventsURL = eventsSrv.URL
		cfg.ResendInterval = 15 * time.Millisecond
		*cfg = WithTestWSReprobeInterval(*cfg, recheck)
	})

	if err := c.SendTaskEvent(TaskEvent{TaskID: "run-1", Kind: "started", OccurredAt: time.Now()}); err != nil {
		t.Fatalf("SendTaskEvent: %v", err)
	}

	// Delivered over HTTP despite WS never once accepting the upgrade.
	waitFor(t, 3*time.Second, func() bool {
		pending, err := store.Pending(context.Background(), testSpace)
		return err == nil && len(pending) == 0
	})
	if c.Connected() {
		t.Fatal("WS must never report Connected when every dial is rejected with 503")
	}

	time.Sleep(window)
	if n := atomic.LoadInt32(&dialCount); n > 8 {
		t.Fatalf("dial count = %d over %v with a %v recheck interval; expected roughly window/interval (~3-4), not the ~17+ a tight reconnect-backoff loop would produce", n, window, recheck)
	}
}

// TestTransportSwitchSerializesSendsNoOverlapNoReorder is the coverage item
// for "single-writer seq order preserved across a WS->HTTP transport switch
// mid-stream (no double-send/reorder)". The WS mock stalls the first
// connection's hello-accept indefinitely (so the client never reports
// Connected() during the early part of the test and httpFallbackLoop
// carries that traffic), then accepts and acks normally on the next
// attempt. Both the WS mock and the HTTP mock record a [start,end)
// wall-clock window for every send/receive they process; flushMu (guarding
// replayPending and flushHTTP) must make those windows mutually exclusive
// across the two transports, and the combined first-seen arrival order
// across both mocks must cover every device_seq exactly once, strictly
// increasing.
func TestTransportSwitchSerializesSendsNoOverlapNoReorder(t *testing.T) {
	const n = 12
	const processDelay = 8 * time.Millisecond

	type win struct {
		start, end time.Time
		via        string
	}
	var mu sync.Mutex
	var windows []win
	var arrival []int64
	seenSeq := map[int64]bool{}

	record := func(via string, seq int64, fn func()) {
		start := time.Now()
		fn()
		end := time.Now()
		mu.Lock()
		windows = append(windows, win{start, end, via})
		if !seenSeq[seq] {
			seenSeq[seq] = true
			arrival = append(arrival, seq)
		}
		mu.Unlock()
	}

	var wsConnNum int32
	wsSrv := rttest.New(func(conn *rttest.Conn) {
		attemptNum := atomic.AddInt32(&wsConnNum, 1)
		if attemptNum == 1 {
			// Read (and discard) the client's hello offer, then
			// deliberately never send hello accept: the client blocks in
			// awaitHelloAccept (bounded by HelloTimeout) and never reports
			// Connected() while this goroutine holds the connection open,
			// forcing httpFallbackLoop to carry the early traffic. A
			// second read blocks until the client's HelloTimeout elapses
			// and it closes this connection (CloseNow), which is how this
			// handler notices it's time to return.
			_, _ = conn.ReadEnvelope()
			_, _ = conn.ReadEnvelope()
			return
		}
		if _, err := conn.HelloAccept("sess", 60000); err != nil {
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
			if env.Type != wire.TypeTaskEvent {
				continue
			}
			body, derr := wire.DecodeBody(env)
			if derr != nil {
				continue
			}
			tb := body.(wire.TaskEventBody)
			record("ws", tb.DeviceSeq, func() {
				time.Sleep(processDelay)
				_ = conn.Ack(tb.DeviceID, tb.DeviceSeq)
			})
		}
	})
	defer wsSrv.Close()

	var revision int64
	eventsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rttest.EventsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		resp := rttest.EventsResponse{}
		for i, env := range req.Events {
			result := rttest.EventResult{Index: i, Type: env.Type}
			if env.Type == wire.TypeTaskEvent {
				var b wire.TaskEventBody
				if err := json.Unmarshal(env.Body, &b); err == nil {
					record("http", b.DeviceSeq, func() { time.Sleep(processDelay) })
					mu.Lock()
					revision++
					result.Revision = revision
					mu.Unlock()
					result.Status = "applied"
					result.DeviceSeq = b.DeviceSeq
					resp.Acked = append(resp.Acked, rttest.AckedPair{DeviceID: b.DeviceID, DeviceSeq: b.DeviceSeq})
				} else {
					result.Status = "rejected"
				}
			}
			resp.Results = append(resp.Results, result)
		}
		mu.Lock()
		resp.SpaceRevision = revision
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer eventsSrv.Close()

	store := newTestOutbox(t)
	c := newTestClient(t, wsSrv.URL(), store, func(cfg *Config) {
		cfg.EventsURL = eventsSrv.URL
		cfg.ResendInterval = 10 * time.Millisecond
		cfg.HelloTimeout = 200 * time.Millisecond
		cfg.BackoffBase = 5 * time.Millisecond
		cfg.BackoffMax = 15 * time.Millisecond
	})

	sendEvent := func(i int) {
		percent := i * 100 / n
		if err := c.SendTaskEvent(TaskEvent{
			TaskID:     "run-1",
			Kind:       "progress",
			OccurredAt: time.Now(),
			Percent:    &percent,
		}); err != nil {
			t.Fatalf("SendTaskEvent(%d): %v", i, err)
		}
	}

	// First half: sent while WS is still stalled on its first (never-
	// completing) connection attempt, so these must travel over HTTP.
	for i := 0; i < n/2; i++ {
		sendEvent(i)
	}
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(arrival) >= n/2
	})

	// Wait for WS to actually come up (its first connection's
	// HelloTimeout elapses, it reconnects, and the mock's second attempt
	// completes hello normally) before sending the rest — guaranteeing,
	// rather than racing, that the second half travels over WS.
	waitFor(t, 3*time.Second, c.Connected)

	for i := n / 2; i < n; i++ {
		sendEvent(i)
	}

	waitFor(t, 8*time.Second, func() bool {
		pending, err := store.Pending(context.Background(), testSpace)
		return err == nil && len(pending) == 0
	})

	mu.Lock()
	gotWindows := append([]win(nil), windows...)
	gotArrival := append([]int64(nil), arrival...)
	mu.Unlock()

	if len(gotArrival) != n {
		t.Fatalf("observed %d distinct device_seq delivered, want %d: %v", len(gotArrival), n, gotArrival)
	}
	for i := 1; i < len(gotArrival); i++ {
		if gotArrival[i] <= gotArrival[i-1] {
			t.Fatalf("device_seq out of order across the transport switch: %v", gotArrival)
		}
	}

	// No two transports' send/receive windows may overlap in wall-clock
	// time — this is what proves flushMu actually serialized WS and HTTP,
	// not merely that the end result happened to be correct.
	sorted := append([]win(nil), gotWindows...)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i].start.After(sorted[j].start) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	for i := 1; i < len(sorted); i++ {
		if sorted[i].start.Before(sorted[i-1].end) {
			t.Fatalf("transport windows overlapped: %+v immediately followed by %+v — WS and HTTP sent concurrently", sorted[i-1], sorted[i])
		}
	}

	sawWS, sawHTTP := false, false
	for _, w := range gotWindows {
		if w.via == "ws" {
			sawWS = true
		}
		if w.via == "http" {
			sawHTTP = true
		}
	}
	if !sawHTTP {
		t.Error("expected at least one delivery over the HTTP fallback (WS stalls its first connection)")
	}
	if !sawWS {
		t.Error("expected at least one delivery over WS once it came up")
	}
}
