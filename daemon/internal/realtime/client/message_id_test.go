package client

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/rttest"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// TestSendMessageEventOverflowPromotionUniqueMessageIDs is the regression
// test for the round-3 review's message_id collision bug: an
// auto-generated message_id used to be derived from the device_seq passed
// into Outbox.Enqueue's build callback (fmt.Sprintf("%s:%d", deviceID,
// seq)), but an event that overflows (the outbox is at its row cap) is
// built with a PLACEHOLDER seq of 1 — every overflowed message with an
// unspecified MessageID collided on the identical id "<device>:1", and
// outbox.setDeviceSeq only ever patches device_seq at promotion time, never
// message_id, so the collision was permanent once promoted.
//
// This drives the fix end-to-end through the public Client API: a cap-1
// outbox forces all but the first of many SendMessageEvent calls (all with
// an unspecified MessageID) into overflow, then each is promoted into the
// outbox one at a time (via outbox.Store.Ack's own promotion trigger,
// exercised in small batches rather than one bulk call) and inspected
// before being acked. Asserts device_seq is strictly increasing AND every
// message_id is unique across the whole run.
func TestSendMessageEventOverflowPromotionUniqueMessageIDs(t *testing.T) {
	const n = 25

	// A mock server that never acks anything on its own — this test drives
	// promotion/acking deterministically itself, one row at a time, rather
	// than racing the client's own resend/ack cycle.
	srv := rttest.New(func(conn *rttest.Conn) {
		if _, err := conn.HelloAccept("sess", 60000); err != nil {
			return
		}
		for {
			if _, err := conn.ReadEnvelope(); err != nil {
				return
			}
		}
	})
	defer srv.Close()

	store := newTestOutboxWithMaxRows(t, 1)
	c := newTestClient(t, srv.URL(), store, nil)

	for i := 0; i < n; i++ {
		if err := c.SendMessageEvent(MessageEvent{
			Level:      "info",
			Text:       "msg",
			OccurredAt: time.Now(),
		}); err != nil {
			t.Fatalf("SendMessageEvent(%d): %v", i, err)
		}
	}

	ctx := context.Background()
	seenIDs := make(map[string]bool, n)
	var lastSeq int64
	for len(seenIDs) < n {
		pending, err := store.Pending(ctx, testSpace)
		if err != nil {
			t.Fatalf("Pending: %v", err)
		}
		if len(pending) == 0 {
			t.Fatalf("outbox pending is empty after draining %d/%d messages", len(seenIDs), n)
		}
		item := pending[0]
		var b wire.MessageEventBody
		if err := json.Unmarshal(item.Body, &b); err != nil {
			t.Fatalf("decode message.event body: %v", err)
		}
		if b.DeviceSeq <= lastSeq {
			t.Fatalf("device_seq not strictly increasing: got %d after %d", b.DeviceSeq, lastSeq)
		}
		lastSeq = b.DeviceSeq
		if seenIDs[b.MessageID] {
			t.Fatalf("duplicate message_id %q at device_seq %d (this is the collision bug the fix closes)", b.MessageID, b.DeviceSeq)
		}
		if b.MessageID == "" {
			t.Fatalf("message_id is empty at device_seq %d", b.DeviceSeq)
		}
		seenIDs[b.MessageID] = true

		// Acking frees the cap-1 outbox's one slot, which promotes the
		// next overflowed row (Store.Ack's own best-effort trigger) —
		// exercising promotion across many small batches, one row per
		// iteration, rather than a single bulk PromoteOverflow call.
		if err := store.Ack(ctx, testSpace, b.DeviceSeq); err != nil {
			t.Fatalf("Ack seq %d: %v", b.DeviceSeq, err)
		}
	}

	if len(seenIDs) != n {
		t.Fatalf("observed %d unique message_ids, want %d", len(seenIDs), n)
	}
	if n, err := store.OverflowCount(ctx); err != nil || n != 0 {
		t.Fatalf("OverflowCount at end = %d, err = %v; want 0", n, err)
	}
}
