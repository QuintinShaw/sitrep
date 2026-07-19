package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// TestDrainToLegacyConcurrentCallersDeliverEachRowExactlyOnce is the
// regression test for the MAJOR concurrency defect introduced by commit
// 1114ff9: cmd/sitrep/agent.go builds exactly one *rtclient.Client for the
// whole resident agent process, but spawns a fresh uplink.Uplink — each
// running its own pollRealtimeMode poll loop — per concurrently-running
// automation. Before DrainToLegacy moved drain ownership (and its locking)
// onto this shared Client, every one of those Uplinks drained the SAME
// outbox directly: two callers could both read the same unretired row via
// Pending()/OverflowPending() before either Ack'd/DeleteOverflow'd it, and
// both would "deliver" it — a duplicate reliable event reaching the server
// in production.
//
// This test drives that exact shape at the layer the fix lives in: many
// goroutines call DrainToLegacy concurrently against one Client/outbox
// pre-loaded with a deterministic backlog (simulating several Uplinks'
// pollRealtimeMode loops firing at once, which is the normal case — the
// scheduler in cmd/sitrep/agent.go polls every 2s and multiple automations
// can easily be mid-run together). It asserts every row is observed by the
// sink EXACTLY ONCE, in device_seq order, across all callers combined — the
// order assertion holds only because legacyDrainMu is held across a whole
// row's read+sink+retire, so at any instant the row with the lowest
// remaining device_seq is always the next (and only) one any caller can be
// looking at, regardless of how the goroutines are scheduled. Must be
// meaningful under -race (run with -race -count=20).
func TestDrainToLegacyConcurrentCallersDeliverEachRowExactlyOnce(t *testing.T) {
	const rows = 40
	const drainers = 8

	// A permanently-403 endpoint: DrainToLegacy only acts while
	// ServerDisabled() is true (see drainOneToLegacy), matching the
	// production precondition (the server has rejected /v3/realtime).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"realtime_disabled"}`))
	}))
	defer srv.Close()

	store := newTestOutbox(t)
	ctx := context.Background()

	// Deterministic backlog, oldest-first by construction (Enqueue is
	// called sequentially, not concurrently, so device_seq order is fixed
	// and known ahead of time).
	for i := 0; i < rows; i++ {
		if _, err := store.Enqueue(ctx, testSpace, wire.TypeMessageEvent, func(seq int64) (json.RawMessage, error) {
			return json.Marshal(wire.MessageEventBody{
				DeviceID: testDeviceID, DeviceSeq: seq,
				MessageID: "m", Level: "info", Text: "unused", OccurredAt: time.Now().UnixMilli(),
				AutomationID: strconv.Itoa(i),
			})
		}); err != nil {
			t.Fatalf("pre-populate Enqueue %d: %v", i, err)
		}
	}

	url := "ws" + srv.URL[len("http"):]
	c := newTestClient(t, url, store, nil)
	waitFor(t, 2*time.Second, c.ServerDisabled)

	var mu sync.Mutex
	var delivered []int64   // device_seq, in the order the sink observed them
	seen := map[int64]int{} // device_seq -> times observed

	sink := func(kind string, body json.RawMessage) bool {
		var b wire.MessageEventBody
		if err := json.Unmarshal(body, &b); err != nil {
			t.Errorf("sink: decode body: %v", err)
			return true
		}
		mu.Lock()
		delivered = append(delivered, b.DeviceSeq)
		seen[b.DeviceSeq]++
		mu.Unlock()
		return true
	}

	var wg sync.WaitGroup
	wg.Add(drainers)
	for i := 0; i < drainers; i++ {
		go func() {
			defer wg.Done()
			c.DrainToLegacy(ctx, sink)
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(delivered) != rows {
		t.Fatalf("sink observed %d rows, want %d (delivered=%v)", len(delivered), rows, delivered)
	}
	for seq, n := range seen {
		if n != 1 {
			t.Fatalf("device_seq %d delivered %d times, want exactly 1 (this is the duplicate-delivery bug the fix closes)", seq, n)
		}
	}
	for i, seq := range delivered {
		want := int64(i + 1) // device_seq starts at 1 (SPEC.md section 5.1)
		if seq != want {
			t.Fatalf("delivery order[%d] = device_seq %d, want %d (out of order): full order %v", i, seq, want, delivered)
		}
	}

	pending, err := store.Pending(ctx, testSpace)
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected outbox fully drained, %d rows remain", len(pending))
	}
}
