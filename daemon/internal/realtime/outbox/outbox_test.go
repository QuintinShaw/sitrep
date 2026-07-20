package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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

// TestEnqueueFullOverflows pins the bounded-outbox policy: at the row cap,
// Enqueue does not fail — it durably persists the event into
// outbox_overflow and returns ErrOverflowed, consuming no device_seq (no
// seq is ever allocated for an overflowed event until it is promoted). An
// ack freeing outbox capacity then promotes the overflowed event,
// allocating it the next unconsumed seq at that moment.
func TestEnqueueFullOverflows(t *testing.T) {
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
	if !errors.Is(err, ErrOverflowed) {
		t.Fatalf("Enqueue at cap = %v, want ErrOverflowed", err)
	}

	// The overflowed Enqueue must not have consumed a sequence number: no
	// seq is allocated until promotion.
	next, err := s.NextSeq(ctx, "space-a")
	if err != nil {
		t.Fatalf("NextSeq: %v", err)
	}
	if next != 3 {
		t.Fatalf("NextSeq after overflow = %d, want 3 (an overflowed Enqueue must not consume a seq)", next)
	}
	overflowCount, err := s.OverflowCount(ctx)
	if err != nil {
		t.Fatalf("OverflowCount: %v", err)
	}
	if overflowCount != 1 {
		t.Fatalf("OverflowCount = %d, want 1", overflowCount)
	}

	// Freeing one row (an ack arrived) promotes the overflowed event,
	// allocating it the next unconsumed seq.
	if err := s.Ack(ctx, "space-a", 1); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	pending, err := s.Pending(ctx, "space-a")
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("Pending after ack+promotion = %d items, want 2 (seq 2 and the promoted seq 3): %v", len(pending), pending)
	}
	if pending[len(pending)-1].DeviceSeq != 3 {
		t.Fatalf("promoted item's seq = %d, want 3", pending[len(pending)-1].DeviceSeq)
	}
	overflowCount, err = s.OverflowCount(ctx)
	if err != nil {
		t.Fatalf("OverflowCount after promotion: %v", err)
	}
	if overflowCount != 0 {
		t.Fatalf("OverflowCount after promotion = %d, want 0", overflowCount)
	}
}

// TestEnqueueRoutesToOverflowWhileOlderOverflowPending is the DB-layer
// regression test for the round-1 seq-order-inversion race, reproduced at
// the store instead of the uplink: a fresh Enqueue must never claim a seq
// via a just-freed outbox row while an older event for the same space is
// still waiting in overflow — even when the outbox itself currently has
// room. The check must live inside Enqueue's own transaction (not depend on
// a caller doing the right thing), so this test frees a row by a route
// other than Ack/PromoteOverflow (a direct delete) to isolate that
// invariant from Ack's own promotion behavior.
func TestEnqueueRoutesToOverflowWhileOlderOverflowPending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	s, err := OpenWithMaxRows(path, 2)
	if err != nil {
		t.Fatalf("OpenWithMaxRows: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := s.Enqueue(ctx, "space-a", "task.event", bodyFor(0)); err != nil {
			t.Fatalf("fill Enqueue %d: %v", i, err)
		}
	}
	// A overflows: the outbox is full.
	_, err = s.Enqueue(ctx, "space-a", "task.event", bodyFor(0))
	if !errors.Is(err, ErrOverflowed) {
		t.Fatalf("A's Enqueue at cap = %v, want ErrOverflowed", err)
	}

	// Free BOTH filler rows (enough room for both A and B to eventually
	// promote) WITHOUT going through Ack, so no promotion runs yet —
	// simulates "capacity freed" independent of this store's own
	// ack-triggers-promotion wiring, isolating the Enqueue-side guard.
	if _, err := s.db.ExecContext(ctx, `DELETE FROM outbox WHERE space = ?`, "space-a"); err != nil {
		t.Fatalf("simulate freed rows: %v", err)
	}

	// B is offered now: the outbox is completely empty (full room), but A
	// is still waiting in overflow. B must join it there — claiming a row
	// directly would hand B a lower seq than A gets once A is finally
	// promoted, inverting wire order.
	_, err = s.Enqueue(ctx, "space-a", "task.event", bodyFor(0))
	if !errors.Is(err, ErrOverflowed) {
		t.Fatalf("B's Enqueue while outbox has room but overflow is non-empty = %v, want ErrOverflowed", err)
	}

	if _, err := s.PromoteOverflow(ctx, 2); err != nil {
		t.Fatalf("PromoteOverflow: %v", err)
	}
	pending, err := s.Pending(ctx, "space-a")
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	// A and B, promoted in FIFO order.
	if len(pending) != 2 {
		t.Fatalf("Pending after promotion = %d items, want 2: %v", len(pending), pending)
	}
	seqA, seqB := pending[0].DeviceSeq, pending[1].DeviceSeq
	if !(seqA < seqB) {
		t.Fatalf("wire order inverted: expected A's seq < B's seq (A overflowed first), got A=%d B=%d", seqA, seqB)
	}
}

// TestOverflowPromotesOnOpenWhenCapacityFree pins the second required
// promotion trigger (the first is Ack, above): a fresh Store.Open must
// itself notice a waiting overflow row that has room to be promoted into
// and do so immediately, rather than waiting for the next ack — otherwise a
// daemon that restarts with an already-drained outbox would leave a
// perfectly promotable event stranded until unrelated future traffic
// happened to trigger an ack.
func TestOverflowPromotesOnOpenWhenCapacityFree(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	s1, err := OpenWithMaxRows(path, 1)
	if err != nil {
		t.Fatalf("OpenWithMaxRows: %v", err)
	}
	ctx := context.Background()

	filler, err := s1.Enqueue(ctx, "space-a", "task.event", bodyFor(0))
	if err != nil {
		t.Fatalf("filler Enqueue: %v", err)
	}
	_, err = s1.Enqueue(ctx, "space-a", "task.event", bodyFor(0))
	if !errors.Is(err, ErrOverflowed) {
		t.Fatalf("overflow Enqueue = %v, want ErrOverflowed", err)
	}

	// Free the filler's row without running this store's own
	// promote-on-ack wiring, so the only remaining trigger that could
	// notice the freed capacity is the next Open.
	if _, err := s1.db.ExecContext(ctx, `DELETE FROM outbox WHERE space = ? AND device_seq = ?`, "space-a", filler.DeviceSeq); err != nil {
		t.Fatalf("simulate freed row: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := OpenWithMaxRows(path, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	pending, err := s2.Pending(ctx, "space-a")
	if err != nil {
		t.Fatalf("Pending after reopen: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected Open to promote the waiting overflow row into freed capacity, got %d pending: %v", len(pending), pending)
	}
	if n, err := s2.OverflowCount(ctx); err != nil || n != 0 {
		t.Fatalf("OverflowCount after reopen-promotion = %d, %v, want 0", n, err)
	}
}

// TestOverflowSurvivesCloseAndReopen is the outbox-layer half of the
// reviewer's exact scenario (the uplink/client-level version lives in
// internal/uplink): an overflowed event, and the outbox rows that keep it
// overflowed, must both survive a process restart (Close + reopen the same
// database file) unchanged, and only promote once an ack actually frees
// capacity on the reopened store.
func TestOverflowSurvivesCloseAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.db")
	s1, err := OpenWithMaxRows(path, 1)
	if err != nil {
		t.Fatalf("OpenWithMaxRows: %v", err)
	}
	ctx := context.Background()

	filler, err := s1.Enqueue(ctx, "space-a", "task.event", bodyFor(0))
	if err != nil {
		t.Fatalf("filler Enqueue: %v", err)
	}
	_, err = s1.Enqueue(ctx, "space-a", "task.event", bodyFor(0))
	if !errors.Is(err, ErrOverflowed) {
		t.Fatalf("overflow Enqueue = %v, want ErrOverflowed", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := OpenWithMaxRows(path, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	// Nothing to promote yet: the outbox is still full (the filler is
	// still unacked), so Open's promotion attempt is a no-op.
	if n, err := s2.OverflowCount(ctx); err != nil || n != 1 {
		t.Fatalf("OverflowCount after reopen = %d, %v, want 1 (outbox still full, nothing promotable)", n, err)
	}

	if err := s2.Ack(ctx, "space-a", filler.DeviceSeq); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	pending, err := s2.Pending(ctx, "space-a")
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("Pending after ack+promotion = %d items, want 1: %v", len(pending), pending)
	}
	if n, err := s2.OverflowCount(ctx); err != nil || n != 0 {
		t.Fatalf("OverflowCount after ack+promotion = %d, %v, want 0", n, err)
	}
}

// TestEnqueueRecoversFromRealTransientLockContention drives a genuine
// SQLITE_BUSY through enqueueAttempt's own bounded retry loop
// (enqueueMaxAttempts / enqueueRetryDelay) — not a fabricated send/build
// callback error, but an actual second connection holding the write lock a
// real Enqueue call has to wait out. It asserts the event still ends up
// durable and readable (never dropped) once the lock clears within the
// retry budget.
//
// A short busy_timeout (via the openWithBusyTimeoutMS test seam, rather
// than production's 5000ms) keeps this fast: with the competing lock held
// for holdDuration — comfortably inside the window of Enqueue's SECOND
// attempt (busyTimeoutMS + enqueueRetryDelay through 2*busyTimeoutMS +
// enqueueRetryDelay) and comfortably past its first (which must therefore
// see a real SQLITE_BUSY and retry) — the first attempt is guaranteed to
// fail on genuine lock contention, and the second is guaranteed to recover
// once the lock actually releases, with margin on both sides against
// scheduler jitter.
func TestEnqueueRecoversFromRealTransientLockContention(t *testing.T) {
	const busyTimeoutMS = 100
	const holdDuration = 170 * time.Millisecond

	path := filepath.Join(t.TempDir(), "outbox.db")

	s, err := openWithBusyTimeoutMS(path, DefaultMaxRows, busyTimeoutMS)
	if err != nil {
		t.Fatalf("openWithBusyTimeoutMS: %v", err)
	}
	defer s.Close()

	// A second, independent connection to the same database file. Its own
	// BEGIN (immediate, via the same _txlock=immediate DSN param
	// OpenWithMaxRows itself relies on) genuinely acquires SQLite's write
	// lock — this is real lock contention, not a simulated error.
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(%d)&_txlock=immediate", path, busyTimeoutMS)
	contender, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open contender connection: %v", err)
	}
	defer contender.Close()
	contender.SetMaxOpenConns(1)

	lockTx, err := contender.Begin()
	if err != nil {
		t.Fatalf("contender BEGIN IMMEDIATE: %v", err)
	}
	released := make(chan struct{})
	go func() {
		time.Sleep(holdDuration)
		if err := lockTx.Commit(); err != nil {
			t.Errorf("contender commit: %v", err)
		}
		close(released)
	}()
	t.Cleanup(func() { <-released })

	start := time.Now()
	item, err := s.Enqueue(context.Background(), "space-a", "message.event", bodyFor(0))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Enqueue: %v (expected it to recover once the competing lock released after %v; Enqueue took %v)", err, holdDuration, elapsed)
	}
	if item.DeviceSeq != 1 {
		t.Fatalf("Enqueue seq = %d, want 1", item.DeviceSeq)
	}
	if elapsed < holdDuration {
		t.Fatalf("Enqueue returned after %v, before the competing lock even released at %v — contention was never actually exercised", elapsed, holdDuration)
	}

	// Durable and readable, not silently dropped.
	pending, err := s.Pending(context.Background(), "space-a")
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 1 || pending[0].DeviceSeq != 1 {
		t.Fatalf("Pending = %+v, want exactly the one durably-enqueued event", pending)
	}
}

// TestAckLogsPromotionFailureInsteadOfSwallowing is the regression test for
// the round-3 review's other promotion bug: Ack used to discard a failed
// PromoteOverflow attempt with a bare `_, _ =`, so the ack itself succeeded
// (correctly — the event really was acknowledged) but a caller had no way
// to ever learn that the freed capacity's promotion attempt failed. This
// pins that Ack (a) still succeeds even when the following promotion
// attempt fails, and (b) reports that failure through SetLogf rather than
// discarding it. FailNextPromotionForTest is used instead of a real,
// timing-sensitive SQLite lock race to make the failure deterministic.
func TestAckLogsPromotionFailureInsteadOfSwallowing(t *testing.T) {
	s := open(t)
	ctx := context.Background()

	var logs []string
	s.SetLogf(func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})

	// maxRows is DefaultMaxRows here (via open(t)/Open), so nothing
	// overflows on Enqueue; instead this seeds an overflow row directly by
	// filling the outbox down to a 1-row cap first. Simplest: reopen with a
	// 1-row cap so the second Enqueue overflows, matching normal
	// production behavior (Ack only ever attempts a promotion when there
	// is something in outbox_overflow to promote).
	s2, err := OpenWithMaxRows(filepath.Join(t.TempDir(), "outbox2.db"), 1)
	if err != nil {
		t.Fatalf("OpenWithMaxRows: %v", err)
	}
	defer s2.Close()
	s2.SetLogf(func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})

	if _, err := s2.Enqueue(ctx, "space-a", "message.event", bodyFor(0)); err != nil {
		t.Fatalf("Enqueue first: %v", err)
	}
	if _, err := s2.Enqueue(ctx, "space-a", "message.event", bodyFor(0)); !errors.Is(err, ErrOverflowed) {
		t.Fatalf("Enqueue second: err = %v, want ErrOverflowed", err)
	}
	if n, err := s2.OverflowCount(ctx); err != nil || n != 1 {
		t.Fatalf("OverflowCount = %d, err = %v; want 1", n, err)
	}

	s2.FailNextPromotionForTest()

	// Ack must still succeed (the event really was acknowledged) even
	// though the promotion it triggers is about to fail.
	if err := s2.Ack(ctx, "space-a", 1); err != nil {
		t.Fatalf("Ack: %v, want success even though the triggered promotion fails", err)
	}

	// The row is gone (acked)...
	pending, err := s2.Pending(ctx, "space-a")
	if err != nil {
		t.Fatalf("Pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("Pending = %+v, want empty (seq 1 was acked)", pending)
	}
	// ...but the overflowed row was NOT promoted (the injected failure
	// consumed that attempt), and unlike before the fix, this must be
	// visible via the logger rather than silently discarded.
	if n, err := s2.OverflowCount(ctx); err != nil || n != 1 {
		t.Fatalf("OverflowCount after failed promotion = %d, err = %v; want 1 (still stuck)", n, err)
	}
	found := false
	for _, l := range logs {
		if strings.Contains(l, "promote overflow") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected the promotion failure to be logged, got logs: %v", logs)
	}

	// A subsequent, unfailed PromoteOverflow attempt (standing in for the
	// client's own periodic proactive retry, see internal/realtime/client)
	// recovers without a restart.
	promoted, err := s2.PromoteOverflow(ctx, 1)
	if err != nil {
		t.Fatalf("PromoteOverflow retry: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("PromoteOverflow retry promoted %d rows, want 1", promoted)
	}
	if n, err := s2.OverflowCount(ctx); err != nil || n != 0 {
		t.Fatalf("OverflowCount after recovery = %d, err = %v; want 0", n, err)
	}
}

// TestAllocSeqMonotonicPersistsAcrossReopen pins the persistent device_seq
// allocator the daemon's non-realtime /v1/events uplink relies on: AllocSeq
// hands out strictly increasing seqs, shares the one counter with Enqueue,
// and — critically — resumes from the last value after the store is closed
// and reopened on the same file (simulating a fresh `sitrep run` process),
// never resetting to 1. That non-reset is what keeps the server's never-
// pruned (device_id, device_seq) dedup from dropping a new process's events.
func TestAllocSeqMonotonicPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "seq.db")

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Two allocations from "process 1".
	for want := int64(1); want <= 2; want++ {
		got, err := s1.AllocSeq(ctx, "space-a")
		if err != nil {
			t.Fatalf("AllocSeq: %v", err)
		}
		if got != want {
			t.Fatalf("AllocSeq = %d, want %d", got, want)
		}
	}
	// A second space keeps its own counter.
	if got, err := s1.AllocSeq(ctx, "space-b"); err != nil || got != 1 {
		t.Fatalf("space-b first AllocSeq = %d, err = %v; want 1", got, err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// "Process 2": a brand-new handle on the same file must NOT reset — it
	// continues from where process 1 left off.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	got, err := s2.AllocSeq(ctx, "space-a")
	if err != nil {
		t.Fatalf("AllocSeq after reopen: %v", err)
	}
	if got != 3 {
		t.Fatalf("AllocSeq after reopen = %d, want 3 (must not reset to 1)", got)
	}

	// AllocSeq and Enqueue share the one counter: the next Enqueue gets 4.
	item, err := s2.Enqueue(ctx, "space-a", "task.event", bodyFor(0))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if item.DeviceSeq != 4 {
		t.Fatalf("Enqueue after AllocSeq gave seq %d, want 4 (shared counter)", item.DeviceSeq)
	}
}

// TestOverflowCapBackpressureAndHealth pins 5d: outbox_overflow is bounded, a
// reliable event that can't be stored once outbox+overflow are both full is
// refused with ErrBackpressure (not silently written), the health hook fires
// degraded, and a promotion that frees capacity clears it.
func TestOverflowCapBackpressureAndHealth(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cap.db")

	// maxRows 1 so the 2nd event overflows; overflow cap 2 so the 4th event
	// (1 in outbox + 2 in overflow already) hits backpressure.
	s, err := OpenWithMaxRows(path, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	s.SetMaxOverflowRowsForTest(2)

	var healthy = true
	var reason string
	s.SetHealthHook(func(ok bool, r string) { healthy = ok; reason = r })

	// 1 → outbox (seq path).
	if _, err := s.Enqueue(ctx, "sp", "message.event", bodyFor(0)); err != nil {
		t.Fatalf("Enqueue 1: %v", err)
	}
	// 2, 3 → overflow (outbox full). Both ErrOverflowed (durable).
	for i := 2; i <= 3; i++ {
		if _, err := s.Enqueue(ctx, "sp", "message.event", bodyFor(0)); !errors.Is(err, ErrOverflowed) {
			t.Fatalf("Enqueue %d: err = %v, want ErrOverflowed", i, err)
		}
	}
	if n, _ := s.OverflowCount(ctx); n != 2 {
		t.Fatalf("OverflowCount = %d, want 2 (at cap)", n)
	}

	// 4 → overflow is at cap AND not coalescable → ErrBackpressure, NOT stored.
	if _, err := s.Enqueue(ctx, "sp", "message.event", bodyFor(0)); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("Enqueue 4: err = %v, want ErrBackpressure", err)
	}
	if n, _ := s.OverflowCount(ctx); n != 2 {
		t.Fatalf("OverflowCount after backpressure = %d, want 2 (event NOT stored, no unbounded growth)", n)
	}
	if healthy {
		t.Fatal("health hook should have fired degraded on backpressure")
	}
	if reason == "" {
		t.Fatal("degraded health must carry a reason")
	}

	// Ack the outbox row → a promotion frees an overflow slot → health recovers.
	if err := s.Ack(ctx, "sp", 1); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if !healthy {
		t.Fatal("health hook should have recovered after promotion freed capacity")
	}
}

// TestOverflowCoalesceBoundsGrowth pins that a coalescable "metric-kind"
// series (same coalesce key) collapses to one overflow row instead of piling
// up — so it never trips the cap however many frames arrive.
func TestOverflowCoalesceBoundsGrowth(t *testing.T) {
	ctx := context.Background()
	s, err := OpenWithMaxRows(filepath.Join(t.TempDir(), "coalesce.db"), 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	s.SetMaxOverflowRowsForTest(2)

	// Fill the single outbox slot so everything else overflows.
	if _, err := s.Enqueue(ctx, "sp", "message.event", bodyFor(0)); err != nil {
		t.Fatalf("seed outbox: %v", err)
	}

	// 50 progress frames for one task, all sharing a coalesce key. They must
	// collapse to ONE overflow row (last-wins), never hitting the cap-2
	// backpressure.
	seq := 0
	for i := 0; i < 50; i++ {
		seq = i
		_, err := s.EnqueueCoalesce(ctx, "sp", "task.event", "task.progress:t1", func(_ int64) (json.RawMessage, error) {
			return json.RawMessage(fmt.Sprintf(`{"device_seq":1,"n":%d}`, seq)), nil
		})
		if err != nil && !errors.Is(err, ErrOverflowed) {
			t.Fatalf("coalescing Enqueue %d: %v", i, err)
		}
	}
	if n, _ := s.OverflowCount(ctx); n != 1 {
		t.Fatalf("OverflowCount = %d, want 1 (50 progress frames coalesced to one row)", n)
	}

	// The surviving row holds the LATEST value.
	items, err := s.OverflowPending(ctx, "sp")
	if err != nil {
		t.Fatalf("OverflowPending: %v", err)
	}
	if len(items) != 1 || string(items[0].Body) == "" {
		t.Fatalf("expected one coalesced row, got %+v", items)
	}
	var got struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal(items[0].Body, &got); err != nil {
		t.Fatalf("decode coalesced body: %v", err)
	}
	if got.N != 49 {
		t.Fatalf("coalesced row n = %d, want 49 (last value wins)", got.N)
	}
}
