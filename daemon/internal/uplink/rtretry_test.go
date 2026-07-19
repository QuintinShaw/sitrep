package uplink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	rtclient "github.com/QuintinShaw/sitrep/daemon/internal/realtime/client"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/outbox"
)

// TestSendReliablePreservesFIFOAgainstFreshOffer is the regression test for
// the seq-order-inversion BLOCKER: a fresh Offer() must never win a race
// into outbox.Enqueue ahead of an older event still waiting in rtRetry, even
// when a row frees up between the older event's failed attempt and the
// newer one's Offer(). Reproduces the exact interleaving from the bug
// report by driving sendReliable/retryRealtime directly (white-box, same
// package) instead of racing real goroutines against a flush ticker —
// deterministic, no sleeps.
func TestSendReliablePreservesFIFOAgainstFreshOffer(t *testing.T) {
	ctx := context.Background()
	store, err := outbox.OpenWithMaxRows(filepath.Join(t.TempDir(), "outbox.db"), 5)
	if err != nil {
		t.Fatalf("OpenWithMaxRows: %v", err)
	}
	defer store.Close()

	const space = "space-1"
	body := func(seq int64) (json.RawMessage, error) {
		return json.RawMessage(fmt.Sprintf(`{"seq":%d}`, seq)), nil
	}

	// Fill the outbox to its cap of 5 with unacked placeholder rows so the
	// very next Enqueue attempt (A, below) hits ErrOutboxFull.
	var filledSeqs []int64
	for i := 0; i < 5; i++ {
		item, err := store.Enqueue(ctx, space, "message.event", body)
		if err != nil {
			t.Fatalf("prefill Enqueue: %v", err)
		}
		filledSeqs = append(filledSeqs, item.DeviceSeq)
	}

	u := &Uplink{cfg: Config{Logf: func(string, ...any) {}}}

	var seqA, seqB int64
	sendA := func() error {
		item, err := store.Enqueue(ctx, space, "message.event", body)
		if err != nil {
			return err
		}
		seqA = item.DeviceSeq
		return nil
	}
	sendB := func() error {
		item, err := store.Enqueue(ctx, space, "message.event", body)
		if err != nil {
			return err
		}
		seqB = item.DeviceSeq
		return nil
	}

	// Step 1: "outbox full with A in rtRetry". A is offered while completely
	// full, so it must queue rather than deliver.
	u.sendReliable(sendA)
	if len(u.rtRetry) != 1 {
		t.Fatalf("expected A queued for retry, rtRetry has %d items", len(u.rtRetry))
	}
	if seqA != 0 {
		t.Fatalf("A must not have enqueued while the outbox was full; got seq %d", seqA)
	}

	// Step 2: "free exactly one row via ack" — an ack arriving on the
	// rtclient connection goroutine, independent of the 1s flush tick.
	if err := store.Ack(ctx, space, filledSeqs[0]); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Step 3: "Offer(B) BEFORE the next flush tick". With the BLOCKER bug,
	// sendReliable would attempt Enqueue unconditionally here, succeeding
	// into the just-freed row and handing B a lower device_seq than the
	// still-queued A. The fix must instead queue B behind A.
	u.sendReliable(sendB)
	if len(u.rtRetry) != 2 {
		t.Fatalf("expected B queued behind A (FIFO) since rtRetry was non-empty, rtRetry has %d items", len(u.rtRetry))
	}
	if seqB != 0 {
		t.Fatalf("B must not have enqueued directly while A was still queued for retry; got seq %d", seqB)
	}

	// Step 4: the next flush tick drains oldest-first, using the freed row
	// for A.
	u.retryRealtime()
	if seqA == 0 {
		t.Fatal("expected A to have been delivered using the freed row")
	}
	if len(u.rtRetry) != 1 {
		t.Fatalf("expected only B still queued after draining A, rtRetry has %d items", len(u.rtRetry))
	}

	// Free a second row so B's retry can succeed too, and drain again.
	if err := store.Ack(ctx, space, filledSeqs[1]); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	u.retryRealtime()
	if seqB == 0 {
		t.Fatal("expected B to have been delivered on the second drain")
	}
	if len(u.rtRetry) != 0 {
		t.Fatalf("expected rtRetry fully drained, has %d items", len(u.rtRetry))
	}

	// The assertion the BLOCKER violates: chronological order (A offered
	// before B) must match wire order, i.e. strictly increasing device_seq.
	if !(seqA < seqB) {
		t.Fatalf("wire order inverted: expected seqA < seqB (A offered first), got seqA=%d seqB=%d", seqA, seqB)
	}
}

// TestSendReliableRetriesTransientEnqueueError is the regression test for
// the MAJOR: a transient outbox.Enqueue failure that is neither
// ErrOutboxFull nor a validation error (e.g. SQLITE_BUSY or a disk I/O blip
// surfacing from BeginTx/COUNT/INSERT/Commit) must be retried, not dropped
// — losing a reliable task.done/message.send to a momentary disk hiccup is
// not acceptable.
func TestSendReliableRetriesTransientEnqueueError(t *testing.T) {
	ctx := context.Background()
	store, err := outbox.OpenWithMaxRows(filepath.Join(t.TempDir(), "outbox.db"), 10)
	if err != nil {
		t.Fatalf("OpenWithMaxRows: %v", err)
	}
	defer store.Close()

	u := &Uplink{cfg: Config{Logf: func(string, ...any) {}}}

	transientErr := errors.New("outbox: begin: disk I/O error")
	attempts := 0
	var seq int64
	send := func() error {
		attempts++
		if attempts == 1 {
			// Simulate a transient Enqueue failure — not ErrOutboxFull, not
			// a validation error, and not a class this code recognizes at
			// all. isRetryable must default to "retry" for it.
			return transientErr
		}
		item, err := store.Enqueue(ctx, "space-1", "message.event", func(seq int64) (json.RawMessage, error) {
			return json.RawMessage(`{}`), nil
		})
		if err != nil {
			return err
		}
		seq = item.DeviceSeq
		return nil
	}

	handled := u.sendReliable(send)
	if !handled {
		t.Fatal("sendReliable must always report handled=true for a reliable event")
	}
	if len(u.rtRetry) != 1 {
		t.Fatalf("expected the event queued for retry after an unrecognized transient error, rtRetry has %d items", len(u.rtRetry))
	}
	if seq != 0 {
		t.Fatal("event must not have been delivered on the failed first attempt")
	}

	u.retryRealtime()
	if seq == 0 {
		t.Fatal("expected the event to be delivered on retry, not dropped")
	}
	if len(u.rtRetry) != 0 {
		t.Fatalf("expected rtRetry drained after the successful retry, has %d items", len(u.rtRetry))
	}
}

// TestSendReliableDropsPermanentValidationError pins the other half of the
// permanent/transient split: rtclient.ErrInvalidBody (a wire-validation
// failure) is the one Enqueue-adjacent error that must NOT be retried —
// the same malformed body can never validate differently — so it is
// logged and dropped instead of growing rtRetry forever.
func TestSendReliableDropsPermanentValidationError(t *testing.T) {
	u := &Uplink{cfg: Config{Logf: func(string, ...any) {}}}
	send := func() error {
		return fmt.Errorf("%w: task.event: missing task_id", rtclient.ErrInvalidBody)
	}

	handled := u.sendReliable(send)
	if !handled {
		t.Fatal("sendReliable must always report handled=true for a reliable event")
	}
	if len(u.rtRetry) != 0 {
		t.Fatalf("expected a validation failure to be dropped, not queued; rtRetry has %d items", len(u.rtRetry))
	}
}
