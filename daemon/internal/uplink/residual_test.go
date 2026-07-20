package uplink

import (
	"errors"
	"fmt"
	"testing"

	rtclient "github.com/QuintinShaw/sitrep/daemon/internal/realtime/client"
)

// TestSendReliableDropsPermanentValidationError pins the permanent/transient
// split: rtclient.ErrInvalidBody (a wire-validation failure) is never
// retried — the same malformed body can never validate differently — so it
// is logged and dropped instead of being held in the residual queue.
func TestSendReliableDropsPermanentValidationError(t *testing.T) {
	u := &Uplink{cfg: Config{Logf: func(string, ...any) {}}}
	send := func() error {
		return fmt.Errorf("%w: task.event: missing task_id", rtclient.ErrInvalidBody)
	}

	handled := u.sendReliable(send)
	if !handled {
		t.Fatal("sendReliable must always report handled=true for a reliable event")
	}
	if len(u.residual) != 0 {
		t.Fatalf("expected a validation failure to be dropped, not held for retry; residual has %d items", len(u.residual))
	}
}

// TestSendReliableHoldsResidualOnPersistentFailure is the regression test
// for the rare residual-retry path: since outbox.Store.Enqueue now persists
// synchronously (durably, to the outbox or its overflow table, retrying
// transient SQLite failures internally and bounded), the only way send()
// can fail here with something other than rtclient.ErrInvalidBody is a
// genuinely persistent local-disk failure that outlasted Enqueue's own
// bounded retries. In that rare case the event must not be dropped: it is
// held in memory and retried on the next drainResidual call (normally
// driven by the flush loop, once per tick) until it finally persists.
func TestSendReliableHoldsResidualOnPersistentFailure(t *testing.T) {
	u := &Uplink{cfg: Config{Logf: func(string, ...any) {}}}

	persistentErr := errors.New("outbox: enqueue failed after 3 attempts: disk I/O error")
	attempts := 0
	delivered := false
	send := func() error {
		attempts++
		if attempts < 3 {
			return persistentErr
		}
		delivered = true
		return nil
	}

	handled := u.sendReliable(send)
	if !handled {
		t.Fatal("sendReliable must always report handled=true for a reliable event")
	}
	if len(u.residual) != 1 {
		t.Fatalf("expected the event held in residual after a non-validation failure, residual has %d items", len(u.residual))
	}
	if delivered {
		t.Fatal("event must not have been delivered on the failed first attempt")
	}

	u.drainResidual() // still fails (attempts=2)
	if len(u.residual) != 1 {
		t.Fatalf("expected the event to remain in residual after a second failed attempt, residual has %d items", len(u.residual))
	}

	u.drainResidual() // succeeds (attempts=3)
	if !delivered {
		t.Fatal("expected the event to be delivered once send() finally succeeded")
	}
	if len(u.residual) != 0 {
		t.Fatalf("expected residual drained after the successful retry, has %d items", len(u.residual))
	}
}

// TestDrainResidualPreservesFIFOAndDropsPermanentMidQueue asserts
// drainResidual walks the residual queue oldest-first, stopping at the
// first entry that is still failing (so a later entry can never be
// delivered ahead of an earlier one still stuck), while a permanent
// validation failure encountered mid-queue is dropped in place rather than
// blocking everything behind it.
func TestDrainResidualPreservesFIFOAndDropsPermanentMidQueue(t *testing.T) {
	u := &Uplink{cfg: Config{Logf: func(string, ...any) {}}}

	var order []string
	transientErr := errors.New("outbox: enqueue failed after 3 attempts: disk I/O error")

	// A: fails once, then succeeds.
	aAttempts := 0
	sendA := func() error {
		aAttempts++
		if aAttempts == 1 {
			return transientErr
		}
		order = append(order, "A")
		return nil
	}
	// B: permanently invalid.
	sendB := func() error {
		return fmt.Errorf("%w: bad body", rtclient.ErrInvalidBody)
	}
	// C: always succeeds once reached.
	sendC := func() error {
		order = append(order, "C")
		return nil
	}

	u.sendReliable(sendA) // fails -> held
	if len(u.residual) != 1 {
		t.Fatalf("expected A held after its first failed attempt, residual has %d items", len(u.residual))
	}
	// B and C are queued directly (bypassing sendReliable's own send()
	// attempt) to deterministically build a 3-item residual queue without
	// depending on timing.
	u.residualMu.Lock()
	u.residual = append(u.residual, sendB, sendC)
	u.residualMu.Unlock()

	u.drainResidual()

	if len(u.residual) != 0 {
		t.Fatalf("expected residual fully drained (A recovers, B is permanently dropped, C succeeds), has %d items: order so far %v", len(u.residual), order)
	}
	if len(order) != 2 || order[0] != "A" || order[1] != "C" {
		t.Fatalf("delivery order = %v, want [A C] (B dropped in place, not blocking C)", order)
	}
}
