package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func open(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "outbox.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func bodyFor(seq int64) BuildBody {
	return func(s int64) (json.RawMessage, error) {
		return json.RawMessage(fmt.Sprintf(`{"device_seq":%d}`, s)), nil
	}
}

func TestEnqueueAllocatesMonotonicSeq(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	for want := int64(1); want <= 5; want++ {
		item, err := s.Enqueue(ctx, "space-a", "task.event", bodyFor(want))
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if item.DeviceSeq != want {
			t.Fatalf("Enqueue seq = %d, want %d", item.DeviceSeq, want)
		}
	}
	// A second space starts its own counter at 1 (device_seq is scoped to
	// one (device, space) pair, SPEC.md section 5.1).
	item, err := s.Enqueue(ctx, "space-b", "task.event", bodyFor(1))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if item.DeviceSeq != 1 {
		t.Fatalf("space-b first seq = %d, want 1", item.DeviceSeq)
	}
}

func TestEnqueueConcurrentIsMonotonicAndUnique(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	const n = 50
	seqs := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			item, err := s.Enqueue(ctx, "space-a", "task.event", bodyFor(0))
			if err != nil {
				t.Errorf("Enqueue: %v", err)
				return
			}
			seqs[i] = item.DeviceSeq
		}(i)
	}
	wg.Wait()
	seen := make(map[int64]bool, n)
	for _, seq := range seqs {
		if seq < 1 {
			t.Fatalf("got invalid seq %d", seq)
		}
		if seen[seq] {
			t.Fatalf("seq %d allocated twice", seq)
		}
		seen[seq] = true
	}
	if len(seen) != n {
		t.Fatalf("got %d unique seqs, want %d", len(seen), n)
	}
}

// TestEnqueueFailureLeavesNoGap asserts the crash-safety property required
// by the work order: allocating a device_seq and durably enqueueing the
// event it belongs to happen in ONE transaction. If building the event body
// fails after the seq was chosen but before the transaction commits, the
// whole transaction MUST roll back — including the counter advance — so the
// next successful Enqueue reuses the same seq rather than skipping it. This
// stands in for an actual process crash between "seq chosen" and "event
// durably stored": from the database's perspective the two are
// indistinguishable, and both must leave the counter exactly where it was.
func TestEnqueueFailureLeavesNoGap(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	boom := errors.New("boom")
	_, err := s.Enqueue(ctx, "space-a", "task.event", func(seq int64) (json.RawMessage, error) {
		if seq != 1 {
			t.Fatalf("expected first attempted seq to be 1, got %d", seq)
		}
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Enqueue error = %v, want wrapping %v", err, boom)
	}

	next, err := s.NextSeq(ctx, "space-a")
	if err != nil {
		t.Fatalf("NextSeq: %v", err)
	}
	if next != 1 {
		t.Fatalf("NextSeq after failed build = %d, want 1 (no gap left by the aborted transaction)", next)
	}

	item, err := s.Enqueue(ctx, "space-a", "task.event", bodyFor(1))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if item.DeviceSeq != 1 {
		t.Fatalf("seq after retry = %d, want 1 (reused, not skipped)", item.DeviceSeq)
	}

	pending, err := s.Pending(ctx, "space-a")
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("Pending returned %d items, want exactly 1 (the failed build must not have left a row)", len(pending))
	}
}

func TestEnqueueAckDeletes(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	item, err := s.Enqueue(ctx, "space-a", "task.event", bodyFor(1))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	pending, err := s.Pending(ctx, "space-a")
	if err != nil || len(pending) != 1 {
		t.Fatalf("Pending = %v, %v; want 1 item", pending, err)
	}
	if err := s.Ack(ctx, "space-a", item.DeviceSeq); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	pending, err = s.Pending(ctx, "space-a")
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("Pending after ack = %v, want empty", pending)
	}
	// Acking an already-acked (or never-enqueued) seq is a no-op, not an
	// error (SPEC.md section 5.2: acking a duplicate is always safe).
	if err := s.Ack(ctx, "space-a", item.DeviceSeq); err != nil {
		t.Fatalf("double Ack: %v", err)
	}
	if err := s.Ack(ctx, "space-a", 999); err != nil {
		t.Fatalf("Ack of unknown seq: %v", err)
	}
}

func TestPendingOrderedOldestFirst(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	for i := int64(1); i <= 5; i++ {
		if _, err := s.Enqueue(ctx, "space-a", "task.event", bodyFor(i)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	// Ack the middle one out of order to make sure ordering survives gaps.
	if err := s.Ack(ctx, "space-a", 3); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	pending, err := s.Pending(ctx, "space-a")
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	want := []int64{1, 2, 4, 5}
	if len(pending) != len(want) {
		t.Fatalf("Pending = %d items, want %d", len(pending), len(want))
	}
	for i, w := range want {
		if pending[i].DeviceSeq != w {
			t.Fatalf("Pending[%d].DeviceSeq = %d, want %d (order: %v)", i, pending[i].DeviceSeq, w, pending)
		}
	}
}

// TestSurvivesRestart simulates a daemon process restart: closing the Store
// and reopening the same database file must preserve both the pending
// queue (for replay) and the device_seq counter (so a restarted source
// never reuses or skips a sequence number).
func TestSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	ctx := context.Background()

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s1.Enqueue(ctx, "space-a", "task.event", bodyFor(1)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := s1.Enqueue(ctx, "space-a", "message.event", bodyFor(2)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// This one gets acked before the "restart" — it must NOT reappear.
	item3, err := s1.Enqueue(ctx, "space-a", "task.event", bodyFor(3))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := s1.Ack(ctx, "space-a", item3.DeviceSeq); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	pending, err := s2.Pending(ctx, "space-a")
	if err != nil {
		t.Fatalf("Pending after restart: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("Pending after restart = %d items, want 2 (got %v)", len(pending), pending)
	}
	if pending[0].DeviceSeq != 1 || pending[1].DeviceSeq != 2 {
		t.Fatalf("Pending after restart out of order: %v", pending)
	}

	next, err := s2.Enqueue(ctx, "space-a", "task.event", bodyFor(4))
	if err != nil {
		t.Fatalf("Enqueue after restart: %v", err)
	}
	if next.DeviceSeq != 4 {
		t.Fatalf("seq after restart = %d, want 4 (counter must survive restart)", next.DeviceSeq)
	}
}

// TestEnqueueFullReturnsErrOutboxFull pins the bounded-outbox policy: at
// the row cap Enqueue fails with ErrOutboxFull, consumes no device_seq
// (the transaction rolls back whole), and capacity freed by an ack makes
// Enqueue work again with the next unconsumed seq.
func TestEnqueueFullReturnsErrOutboxFull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	s, err := OpenWithMaxRows(path, 2)
	if err != nil {
		t.Fatalf("OpenWithMaxRows: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	for i := int64(1); i <= 2; i++ {
		if _, err := s.Enqueue(ctx, "space-a", "task.event", bodyFor(i)); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	_, err = s.Enqueue(ctx, "space-a", "task.event", bodyFor(3))
	if !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("Enqueue at cap = %v, want ErrOutboxFull", err)
	}

	// The failed Enqueue must not have consumed a sequence number.
	next, err := s.NextSeq(ctx, "space-a")
	if err != nil {
		t.Fatalf("NextSeq: %v", err)
	}
	if next != 3 {
		t.Fatalf("NextSeq after full = %d, want 3 (rejected Enqueue must not consume a seq)", next)
	}

	// Freeing one row (an ack arrived) restores capacity.
	if err := s.Ack(ctx, "space-a", 1); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	item, err := s.Enqueue(ctx, "space-a", "task.event", bodyFor(3))
	if err != nil {
		t.Fatalf("Enqueue after ack freed capacity: %v", err)
	}
	if item.DeviceSeq != 3 {
		t.Fatalf("seq after recovery = %d, want 3", item.DeviceSeq)
	}
}
