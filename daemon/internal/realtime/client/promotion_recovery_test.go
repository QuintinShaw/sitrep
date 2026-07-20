package client

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/rttest"
	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// TestPromotionRecoversAfterTransientFailureWithoutRestart is the
// regression test for the round-3 review's promotion-stranding bug: before
// the fix, outbox.Store.Ack triggered exactly one best-effort
// PromoteOverflow attempt per ack and silently discarded a failure
// (`_, _ = ...`). If that one attempt failed transiently and nothing else
// was left pending to ack again soon (as is the case here: the outbox is
// empty immediately afterward, with only an unpromoted overflow row left),
// nothing would ever retry it — until either a lucky later ack or a full
// process restart (Store.Open's own promote-on-open).
//
// The fix is two-fold (see outbox.Store.Ack and Client.replayPending/
// flushHTTP): Ack now logs a failed promotion instead of swallowing it, and
// — the actual recovery mechanism — the client's own replay/resend cycle
// proactively retries promotion on every tick, independent of any ack.
//
// This test sets up the failure deterministically (store.Ack called
// directly, once, with FailNextPromotionForTest armed, before any Client
// exists) rather than via a real timing-sensitive SQLite lock race or by
// racing a live client's own ack handling. Only after that forced failure
// does it start a Client against the still-overflowed backlog and assert
// the overflowed event is nonetheless delivered — recovered by the
// client's own next replay/resend tick, with no further ack ever needed to
// unstick it and no process restart.
func TestPromotionRecoversAfterTransientFailureWithoutRestart(t *testing.T) {
	store := newTestOutboxWithMaxRows(t, 1)
	ctx := context.Background()

	if _, err := store.Enqueue(ctx, testSpace, wire.TypeMessageEvent, func(seq int64) (json.RawMessage, error) {
		return json.Marshal(wire.MessageEventBody{
			DeviceID: testDeviceID, DeviceSeq: seq, MessageID: "first",
			Level: "info", Text: "first", OccurredAt: time.Now().UnixMilli(),
		})
	}); err != nil {
		t.Fatalf("Enqueue first: %v", err)
	}
	if _, err := store.Enqueue(ctx, testSpace, wire.TypeMessageEvent, func(seq int64) (json.RawMessage, error) {
		return json.Marshal(wire.MessageEventBody{
			DeviceID: testDeviceID, DeviceSeq: seq, MessageID: "second",
			Level: "info", Text: "second", OccurredAt: time.Now().UnixMilli(),
		})
	}); err == nil {
		t.Fatal("Enqueue second: want ErrOverflowed (outbox cap is 1), got nil error")
	}
	if n, err := store.OverflowCount(ctx); err != nil || n != 1 {
		t.Fatalf("OverflowCount = %d, err = %v; want 1", n, err)
	}

	// Deterministically fail exactly the promotion attempt Ack triggers:
	// capacity is free the instant this Ack's DELETE commits (the outbox
	// becomes empty), so this is precisely "ack succeeds, the promotion it
	// frees up for fails" — no race with anything else, since no Client
	// exists yet.
	store.FailNextPromotionForTest()
	if err := store.Ack(ctx, testSpace, 1); err != nil {
		t.Fatalf("Ack seq 1: %v", err)
	}
	if n, err := store.OverflowCount(ctx); err != nil || n != 1 {
		t.Fatalf("OverflowCount after the forced promotion failure = %d, err = %v; want 1 (still stuck)", n, err)
	}
	if n, err := store.Count(ctx); err != nil || n != 0 {
		t.Fatalf("outbox Count after acking seq 1 = %d, err = %v; want 0", n, err)
	}

	// Now bring up a client against this same store. Nothing is pending to
	// ack anymore (only the stuck overflow row remains) — the ONLY way
	// "second" is ever delivered is the client's own proactive promotion
	// retry on its replay/resend cycle.
	var mu sync.Mutex
	delivered := map[string]bool{}
	srv := rttest.New(func(conn *rttest.Conn) {
		if _, err := conn.HelloAccept("sess", 60000); err != nil {
			t.Errorf("HelloAccept: %v", err)
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
			if env.Type != wire.TypeMessageEvent {
				continue
			}
			body, derr := wire.DecodeBody(env)
			if derr != nil {
				continue
			}
			mb := body.(wire.MessageEventBody)
			mu.Lock()
			delivered[mb.MessageID] = true
			mu.Unlock()
			_ = conn.Ack(mb.DeviceID, mb.DeviceSeq)
		}
	})
	defer srv.Close()

	newTestClient(t, srv.URL(), store, func(cfg *Config) {
		cfg.ResendInterval = 15 * time.Millisecond
	})

	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return delivered["second"]
	})

	waitFor(t, 3*time.Second, func() bool {
		n, err := store.OverflowCount(ctx)
		return err == nil && n == 0
	})
	waitFor(t, 3*time.Second, func() bool {
		n, err := store.Count(ctx)
		return err == nil && n == 0
	})
}
