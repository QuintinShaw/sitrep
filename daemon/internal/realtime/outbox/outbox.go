// Package outbox is the local, crash-safe queue of reliable realtime events
// (task.event / message.event, SPEC.md section 5) awaiting acknowledgement
// from the server. It also owns the per-(device, space) device_seq counter
// (SPEC.md section 5.1): allocating the next sequence number and durably
// enqueueing the event it belongs to happen in a single SQLite transaction,
// so a crash between "I picked seq N" and "event N is safely on disk" is
// impossible — either both happened, or neither did, and the next call sees
// the same N again.
//
// Storage is modernc.org/sqlite, a pure-Go (no cgo) SQLite driver, chosen so
// the daemon keeps its zero-cgo build. The database file lives alongside
// the existing ~/.config/sitrep/config.json (internal/config.Path).
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// DefaultMaxRows bounds the outbox's total row count (across all spaces).
// The cap keeps local disk usage bounded when the server is unreachable for
// a long stretch. Reliability does not degrade at the cap: an Enqueue that
// would exceed it is transparently redirected, in the same transaction, into
// outbox_overflow (see ErrOverflowed) — a second, uncapped durable table in
// this same database file — rather than being rejected. The event is
// durable either way; only its promotion into the seq-bearing outbox is
// deferred until acks free capacity (see PromoteOverflow).
const DefaultMaxRows = 5000

// DefaultMaxOverflowRows bounds outbox_overflow. Without it, overflow was a
// SECOND, unbounded durable table: a server unreachable for a very long
// stretch could grow it (and thus local disk) without limit (5d). With it,
// the TOTAL durable footprint is bounded at DefaultMaxRows + this. When both
// the outbox and overflow are full, a further reliable event is refused with
// ErrBackpressure (an explicit signal, not a silent unbounded write) and the
// store's health hook fires — see SetHealthHook / EnqueueCoalesce.
const DefaultMaxOverflowRows = 5000

// ErrOutboxFull is a legacy sentinel kept for API compatibility; Enqueue no
// longer returns it (a full outbox now overflows instead of failing — see
// ErrOverflowed). Nothing in this package returns it anymore.
var ErrOutboxFull = errors.New("outbox: full")

// ErrBackpressure is returned by Enqueue/EnqueueCoalesce when the event could
// NOT be durably stored because BOTH the outbox (maxRows) and the overflow
// table (maxOverflowRows) are full and the event was not coalescable into an
// existing overflow row. Unlike ErrOverflowed (informational — the event is
// durable), this means the event was NOT persisted: the caller must treat it
// as real backpressure and retry later once capacity frees. The store's
// health hook is also driven to a degraded state when this happens (5d), so
// the condition is visible, not just returned inline. Use errors.Is.
var ErrBackpressure = errors.New("outbox: backpressure — outbox and overflow both at capacity")

// ErrOverflowed is returned (wrapped) by Enqueue when the event was, instead
// of being written straight into the seq-bearing outbox, durably persisted
// into outbox_overflow: the outbox was at its row cap, or an older event for
// the same space was already waiting in overflow (see the FIFO note on
// Enqueue). It is informational, not a failure — by the time it is
// returned, the event has already survived a process crash. Callers must
// not retry or reroute on this error; PromoteOverflow (called on ack-freed
// capacity and at Store open) moves the event into the outbox, allocating
// its device_seq at that moment, once room and turn allow.
var ErrOverflowed = errors.New("outbox: enqueued to overflow (outbox full or an older overflow entry pending)")

const schema = `
CREATE TABLE IF NOT EXISTS space_seq_counters (
	space    TEXT PRIMARY KEY,
	next_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
	space      TEXT NOT NULL,
	device_seq INTEGER NOT NULL,
	kind       TEXT NOT NULL,
	body       TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY (space, device_seq)
);
-- outbox_overflow holds reliable events that arrived while the outbox was
-- at capacity (or while an older overflow entry for the same space was
-- still pending promotion). No device_seq column: a seq is only ever
-- allocated when PromoteOverflow moves a row into outbox, never before, so
-- there is nothing to conflict with the outbox's own counter. "body" holds
-- the pre-seq event material — the same JSON Enqueue would have written,
-- built with a placeholder device_seq of 1 purely to satisfy wire.Validate
-- (which requires device_seq >= 1) — with the real value patched in at
-- promotion time (see setDeviceSeq). id's autoincrement order is the FIFO
-- order promotion must honor.
-- coalesce_key (empty for non-coalescable events) lets a continuous
-- "metric-kind" reliable event (e.g. task.progress for one task) replace an
-- older still-overflowed one for the same key in place, instead of piling up
-- (5d). Only the last value of a coalescable series is worth delivering, so
-- this bounds overflow growth without dropping any lifecycle/message event.
CREATE TABLE IF NOT EXISTS outbox_overflow (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	space        TEXT NOT NULL,
	kind         TEXT NOT NULL,
	body         TEXT NOT NULL,
	created_at   INTEGER NOT NULL,
	coalesce_key TEXT NOT NULL DEFAULT ''
);
`

// Item is one persisted, not-yet-acknowledged reliable event.
type Item struct {
	Space     string
	DeviceSeq int64
	Kind      string // "task.event" | "message.event"
	Body      json.RawMessage
	CreatedAt int64 // unix ms
}

// Store is the outbox's SQLite-backed handle. It is safe for concurrent use
// from multiple goroutines: every method serializes through the underlying
// *sql.DB, and every mutating operation is one transaction.
type Store struct {
	db              *sql.DB
	maxRows         int
	maxOverflowRows int

	logf     Logf                              // pluggable logger; nil is a no-op (see SetLogf)
	onHealth func(healthy bool, reason string) // 5d health hook; nil is a no-op

	// failNextPromotion is a test-only fault-injection seam (same
	// convention as internal/realtime/client's
	// WithTestDisabledRecheckInterval): when true, the next promoteOneTx
	// call consumes the flag and returns a synthetic error instead of
	// attempting its transaction, letting a cross-package test
	// deterministically reproduce "a promotion attempt fails transiently"
	// without winning a real, timing-sensitive SQLite lock race. Always
	// false in production.
	failNextPromotion atomic.Bool
}

// Logf is a pluggable logger; nil (the default) discards.
type Logf func(format string, args ...any)

// SetLogf installs the logger Ack uses to report a promotion attempt that
// failed after the ack itself already succeeded (see Ack's doc comment). A
// nil-safe no-op logger is used until this is called.
func (s *Store) SetLogf(f Logf) { s.logf = f }

func (s *Store) logAt(format string, args ...any) {
	if s.logf != nil {
		s.logf(format, args...)
	}
}

// SetHealthHook installs a callback invoked when the durable footprint hits
// its backpressure cap (healthy=false, with a reason) and when it recovers
// below the cap (healthy=true). The daemon wires this to the health-status
// file the menubar reads (5d, N1). nil (the default) is a no-op.
func (s *Store) SetHealthHook(f func(healthy bool, reason string)) { s.onHealth = f }

func (s *Store) reportHealth(healthy bool, reason string) {
	if s.onHealth != nil {
		s.onHealth(healthy, reason)
	}
}

// SetMaxOverflowRowsForTest overrides the overflow cap so a test can drive
// the backpressure/health path without inserting thousands of rows.
// Production uses DefaultMaxOverflowRows.
func (s *Store) SetMaxOverflowRowsForTest(n int) { s.maxOverflowRows = n }

// FailNextPromotionForTest is a test-only seam: it makes the very next
// PromoteOverflow attempt on this Store fail with a synthetic error,
// exactly once, regardless of cause. It exists so a test (in this package
// or another, e.g. internal/realtime/client) can deterministically
// reproduce "a promotion attempt fails transiently" — the round-3 review's
// promotion-stranding bug — without needing to win a real, timing-sensitive
// SQLite lock race. Production code must never call this.
func (s *Store) FailNextPromotionForTest() { s.failNextPromotion.Store(true) }

// Open creates (if needed) and opens the outbox database at path with the
// DefaultMaxRows cap. Callers should keep one *Store per daemon process
// for a given path.
func Open(path string) (*Store, error) {
	return OpenWithMaxRows(path, DefaultMaxRows)
}

// OpenWithMaxRows is Open with an explicit row cap; maxRows <= 0 falls
// back to DefaultMaxRows.
func OpenWithMaxRows(path string, maxRows int) (*Store, error) {
	return openWithBusyTimeoutMS(path, maxRows, 5000)
}

// openWithBusyTimeoutMS is OpenWithMaxRows with the SQLite busy_timeout
// pragma exposed, in milliseconds. Production always goes through
// OpenWithMaxRows (5000ms, see the _txlock=immediate comment below); this
// exists as a same-package test seam so a test can drive a genuinely fast
// SQLITE_BUSY through enqueueAttempt's own bounded retry loop (see
// enqueueMaxAttempts) with a short busy_timeout, rather than needing to hold
// a competing write lock for the real 5s default just to prove that retry
// path recovers a transient error.
func openWithBusyTimeoutMS(path string, maxRows, busyTimeoutMS int) (*Store, error) {
	if maxRows <= 0 {
		maxRows = DefaultMaxRows
	}
	// _txlock=immediate: acquire the write lock at BEGIN rather than at
	// first write, so two overlapping transactions serialize instead of
	// racing to upgrade a deferred lock (SQLITE_BUSY under concurrent
	// Enqueue calls from multiple goroutines).
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(%d)&_txlock=immediate", path, busyTimeoutMS)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("outbox: open %s: %w", path, err)
	}
	// SQLite allows only one writer at a time; a single connection avoids
	// spurious SQLITE_BUSY errors from the driver opening a second one.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("outbox: init schema: %w", err)
	}
	// Migrate a pre-existing outbox_overflow table (created before coalesce_key
	// existed) by adding the column. A fresh DB already has it via the CREATE
	// above, so this ALTER errors "duplicate column name" — ignored.
	_, _ = db.Exec(`ALTER TABLE outbox_overflow ADD COLUMN coalesce_key TEXT NOT NULL DEFAULT ''`)
	store := &Store{db: db, maxRows: maxRows, maxOverflowRows: DefaultMaxOverflowRows}

	// Restart recovery: anything left in outbox_overflow from a previous
	// process survives on disk exactly where it was, but it only becomes
	// eligible for delivery once promoted into the seq-bearing outbox.
	// Attempt every promotion the current cap allows right away, so a
	// daemon that restarts with free capacity (or with an empty outbox)
	// doesn't wait for the next ack to notice overflowed work is waiting.
	if _, err := store.PromoteOverflow(context.Background(), maxRows); err != nil {
		db.Close()
		return nil, fmt.Errorf("outbox: promote overflow on open: %w", err)
	}
	return store, nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error { return s.db.Close() }

// BuildBody produces the JSON body for a reliable event once its device_seq
// is known — Enqueue calls it inside the allocating transaction so the
// caller never has to allocate a seq itself ahead of time.
type BuildBody func(seq int64) (json.RawMessage, error)

// enqueueMaxAttempts bounds Enqueue's internal retry of a transient SQLite
// failure (BeginTx/COUNT/INSERT/Commit erroring — e.g. SQLITE_BUSY beyond
// what the driver's own busy_timeout pragma already absorbs, or a momentary
// disk I/O blip). The store's DSN already sets busy_timeout(5000), so most
// lock contention resolves inside a single attempt; this loop is a second,
// small layer on top so a transient failure that busy_timeout doesn't cover
// does not surface to the caller as event loss on the first try.
const enqueueMaxAttempts = 3

// enqueueRetryDelay is the (small, fixed) pause between Enqueue's internal
// attempts. Deliberately short: Offer() must not block the caller for long,
// and busy_timeout already handles the common lock-contention case.
const enqueueRetryDelay = 20 * time.Millisecond

// Enqueue durably persists the event BuildBody produces, allocating its
// device_seq atomically with the insert (SPEC.md section 5.1: a crash
// between "seq chosen" and "event durably stored" can never happen — the
// transaction commits both effects or neither).
//
// If the outbox is at its row cap, OR an older event for this same space is
// still sitting in outbox_overflow awaiting promotion, this event is
// persisted into outbox_overflow instead (no device_seq allocated) and
// Enqueue returns ErrOverflowed — the event is durable either way. The
// overflow-nonempty check happens inside the very same transaction as the
// cap check and the insert, which is what prevents the round-1
// seq-order-inversion race from resurfacing at the DB layer: without it, a
// fresh Enqueue could race a still-overflowed older event for a freed
// outbox row and win it a lower seq, inverting wire order once the older
// event is eventually promoted. Routing every new arrival to overflow
// whenever overflow is non-empty, checked under the same lock as the
// promotion path, makes that race impossible.
//
// A transient failure (not ErrOverflowed, not a BuildBody/validation error)
// is retried internally up to enqueueMaxAttempts times before it is
// returned to the caller — see enqueueMaxAttempts.
func (s *Store) Enqueue(ctx context.Context, space, kind string, build BuildBody) (Item, error) {
	return s.EnqueueCoalesce(ctx, space, kind, "", build)
}

// EnqueueCoalesce is Enqueue with an optional coalesceKey. When the event
// must go to overflow (outbox full or an older overflow entry pending) AND a
// non-empty coalesceKey already has a row waiting in overflow for this space,
// that row's body is REPLACED in place (last-value-wins) instead of adding a
// new row — bounding overflow growth for continuous "metric-kind" series
// like task.progress (5d). An empty coalesceKey never coalesces (every
// lifecycle/message event is preserved). If overflow is at its own cap and
// the event cannot coalesce, the event is NOT stored and ErrBackpressure is
// returned (with the health hook driven degraded).
func (s *Store) EnqueueCoalesce(ctx context.Context, space, kind, coalesceKey string, build BuildBody) (Item, error) {
	var lastErr error
	for attempt := 0; attempt < enqueueMaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(enqueueRetryDelay):
			case <-ctx.Done():
				return Item{}, ctx.Err()
			}
		}
		item, terminal, err := s.enqueueAttempt(ctx, space, kind, coalesceKey, build)
		if err == nil {
			return item, nil
		}
		if terminal {
			// ErrOverflowed (success-shaped), ErrBackpressure (real, but
			// retrying immediately won't help), or a BuildBody/validation
			// failure (the same input can never validate differently): none
			// benefits from this loop's retry.
			return item, err
		}
		lastErr = err
	}
	return Item{}, fmt.Errorf("outbox: enqueue failed after %d attempts: %w", enqueueMaxAttempts, lastErr)
}

// enqueueAttempt is one transactional attempt at Enqueue's work. terminal
// reports whether the caller should stop retrying regardless of err being
// non-nil (a BuildBody failure, the informational ErrOverflowed, or
// ErrBackpressure).
func (s *Store) enqueueAttempt(ctx context.Context, space, kind, coalesceKey string, build BuildBody) (item Item, terminal bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Item{}, false, fmt.Errorf("outbox: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit

	// FIFO guard: while an older event for this space is still waiting in
	// overflow, this new one must join it there too, even if the outbox
	// below is not currently at cap (e.g. an ack freed a row a moment ago,
	// but PromoteOverflow hasn't run yet) — never let a fresher event claim
	// a seq ahead of an older still-overflowed one.
	var overflowPending int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_overflow WHERE space = ?`, space).Scan(&overflowPending); err != nil {
		return Item{}, false, fmt.Errorf("outbox: count overflow: %w", err)
	}

	var rows int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&rows); err != nil {
		return Item{}, false, fmt.Errorf("outbox: count rows: %w", err)
	}

	if overflowPending > 0 || rows >= s.maxRows {
		coalesced, err := s.insertOrCoalesceOverflowTx(ctx, tx, space, kind, coalesceKey, build)
		if err != nil {
			if errors.Is(err, ErrBackpressure) {
				// Nothing persisted; leave the tx to roll back and surface the
				// degraded health state so the failure is visible, not silent.
				s.reportHealth(false, fmt.Sprintf("local telemetry storage full (outbox %d + overflow cap %d)", s.maxRows, s.maxOverflowRows))
			}
			return Item{}, true, err
		}
		if err := tx.Commit(); err != nil {
			return Item{}, false, fmt.Errorf("outbox: commit overflow insert: %w", err)
		}
		_ = coalesced
		return Item{}, true, fmt.Errorf("outbox has %d rows (cap %d), %d overflow pending for %s: %w", rows, s.maxRows, overflowPending, space, ErrOverflowed)
	}

	seq, err := nextSeqTx(tx, space)
	if err != nil {
		return Item{}, false, err
	}
	body, err := build(seq)
	if err != nil {
		return Item{}, true, fmt.Errorf("outbox: build body for seq %d: %w", seq, err)
	}
	now := time.Now().UnixMilli()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (space, device_seq, kind, body, created_at) VALUES (?, ?, ?, ?, ?)`,
		space, seq, kind, string(body), now,
	); err != nil {
		return Item{}, false, fmt.Errorf("outbox: insert seq %d: %w", seq, err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO space_seq_counters (space, next_seq) VALUES (?, ?)
		 ON CONFLICT(space) DO UPDATE SET next_seq = excluded.next_seq`,
		space, seq+1,
	); err != nil {
		return Item{}, false, fmt.Errorf("outbox: advance counter: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Item{}, false, fmt.Errorf("outbox: commit: %w", err)
	}
	return Item{Space: space, DeviceSeq: seq, Kind: kind, Body: body, CreatedAt: now}, false, nil
}

// insertOrCoalesceOverflowTx persists the pre-seq event material into
// outbox_overflow within tx. build is called with a placeholder device_seq
// of 1 — the smallest value wire.Validate accepts — purely so validation can
// run; the real device_seq is patched in at promotion (see setDeviceSeq).
//
// If coalesceKey is non-empty and a row already exists for (space,
// coalesceKey), that row's body is replaced in place (coalesced=true) — no
// new row, no growth. Otherwise, if outbox_overflow is already at
// maxOverflowRows, the event is refused with ErrBackpressure (the total
// durable footprint is bounded at maxRows+maxOverflowRows). Only when there
// is room does it insert a new row.
func (s *Store) insertOrCoalesceOverflowTx(ctx context.Context, tx *sql.Tx, space, kind, coalesceKey string, build BuildBody) (coalesced bool, err error) {
	const placeholderSeq = 1
	body, err := build(placeholderSeq)
	if err != nil {
		return false, fmt.Errorf("outbox: build overflow body: %w", err)
	}
	now := time.Now().UnixMilli()

	if coalesceKey != "" {
		res, err := tx.ExecContext(ctx,
			`UPDATE outbox_overflow SET body = ?, kind = ?, created_at = ? WHERE space = ? AND coalesce_key = ?`,
			string(body), kind, now, space, coalesceKey,
		)
		if err != nil {
			return false, fmt.Errorf("outbox: coalesce overflow: %w", err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			return true, nil // replaced in place — no growth
		}
	}

	// No coalesce target: enforce the overflow cap before inserting a new row.
	if s.maxOverflowRows > 0 {
		var overflowRows int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_overflow`).Scan(&overflowRows); err != nil {
			return false, fmt.Errorf("outbox: count overflow (cap): %w", err)
		}
		if overflowRows >= s.maxOverflowRows {
			return false, fmt.Errorf("overflow at cap %d: %w", s.maxOverflowRows, ErrBackpressure)
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox_overflow (space, kind, body, created_at, coalesce_key) VALUES (?, ?, ?, ?, ?)`,
		space, kind, string(body), now, coalesceKey,
	); err != nil {
		return false, fmt.Errorf("outbox: insert overflow: %w", err)
	}
	return false, nil
}

// PromoteOverflow moves up to n of the oldest outbox_overflow rows into the
// seq-bearing outbox, allocating each one's device_seq at the moment it is
// promoted (never before). It stops early — before n promotions — once
// either outbox_overflow is empty or the outbox is at its row cap; n is
// simply an upper bound on the number of attempts, so passing a generous
// value (e.g. the store's own maxRows) is always safe. It reports how many
// rows were actually promoted.
//
// Call sites: the ack path (Ack, below) promotes one row per freed outbox
// slot, and Open/OpenWithMaxRows promote everything they can immediately on
// process start (restart recovery) — both are named in the work order as
// the two triggers that must not let a promotable row sit unnoticed.
func (s *Store) PromoteOverflow(ctx context.Context, n int) (int, error) {
	promoted := 0
	for i := 0; i < n; i++ {
		ok, err := s.promoteOneTx(ctx)
		if err != nil {
			return promoted, err
		}
		if !ok {
			break
		}
		promoted++
	}
	if promoted > 0 {
		// Promotion freed overflow capacity, so any backpressure state has
		// recovered — clear the degraded health signal (idempotent).
		s.reportHealth(true, "")
	}
	return promoted, nil
}

// promoteOneTx promotes exactly the single oldest outbox_overflow row
// (lowest id, across all spaces — id order is overflow's FIFO order) into
// outbox, in one transaction: read the cap, read the oldest row, allocate
// its space's next device_seq, patch that seq into the stored body, insert
// into outbox, advance the counter, and delete the overflow row. ok is
// false (no error) when there was nothing to promote, or no room to promote
// it into.
func (s *Store) promoteOneTx(ctx context.Context) (bool, error) {
	if s.failNextPromotion.CompareAndSwap(true, false) {
		return false, fmt.Errorf("outbox: injected promotion failure (test)")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("outbox: begin promotion: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit

	var rows int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&rows); err != nil {
		return false, fmt.Errorf("outbox: count rows: %w", err)
	}
	if rows >= s.maxRows {
		return false, nil // no room; caller should stop
	}

	var id int64
	var space, kind, body string
	err = tx.QueryRowContext(ctx,
		`SELECT id, space, kind, body FROM outbox_overflow ORDER BY id ASC LIMIT 1`,
	).Scan(&id, &space, &kind, &body)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("outbox: read oldest overflow row: %w", err)
	}

	seq, err := nextSeqTx(tx, space)
	if err != nil {
		return false, err
	}
	finalBody, err := setDeviceSeq(json.RawMessage(body), seq)
	if err != nil {
		return false, fmt.Errorf("outbox: patch device_seq on promotion of overflow id %d: %w", id, err)
	}
	now := time.Now().UnixMilli()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (space, device_seq, kind, body, created_at) VALUES (?, ?, ?, ?, ?)`,
		space, seq, kind, string(finalBody), now,
	); err != nil {
		return false, fmt.Errorf("outbox: insert promoted seq %d: %w", seq, err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO space_seq_counters (space, next_seq) VALUES (?, ?)
		 ON CONFLICT(space) DO UPDATE SET next_seq = excluded.next_seq`,
		space, seq+1,
	); err != nil {
		return false, fmt.Errorf("outbox: advance counter: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM outbox_overflow WHERE id = ?`, id); err != nil {
		return false, fmt.Errorf("outbox: delete promoted overflow id %d: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("outbox: commit promotion: %w", err)
	}
	return true, nil
}

// setDeviceSeq returns body with its top-level "device_seq" field replaced
// by seq, leaving every other field untouched. It works generically across
// both reliable event kinds (task.event/message.event) because both
// wire.TaskEventBody and wire.MessageEventBody serialize a "device_seq"
// field — this package intentionally does not import the wire package to
// do this via its concrete types, so the overflow mechanism stays agnostic
// to any future reliable event kind that follows the same convention.
func setDeviceSeq(body json.RawMessage, seq int64) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, fmt.Errorf("decode body: %w", err)
	}
	seqJSON, err := json.Marshal(seq)
	if err != nil {
		return nil, err
	}
	fields["device_seq"] = seqJSON
	patched, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("re-encode body: %w", err)
	}
	return patched, nil
}

// nextSeqTx reads and does NOT yet persist the next device_seq for space;
// the caller is expected to write space_seq_counters within the same
// transaction once it has successfully built and inserted the event that
// consumes this value (see Enqueue). Device_seq starts at 1 (SPEC.md
// section 5.1).
func nextSeqTx(tx *sql.Tx, space string) (int64, error) {
	var next int64
	err := tx.QueryRow(`SELECT next_seq FROM space_seq_counters WHERE space = ?`, space).Scan(&next)
	switch {
	case err == sql.ErrNoRows:
		return 1, nil
	case err != nil:
		return 0, fmt.Errorf("outbox: read counter: %w", err)
	default:
		return next, nil
	}
}

// NextSeq reports the device_seq the next Enqueue(space, ...) call will
// allocate, without allocating it. Intended for tests and diagnostics.
func (s *Store) NextSeq(ctx context.Context, space string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	return nextSeqTx(tx, space)
}

// AllocSeq atomically allocates and PERSISTS the next device_seq for space,
// advancing the very same `space_seq_counters` counter Enqueue advances —
// but WITHOUT storing an event. It is the persistent, crash-safe,
// cross-process device_seq source for the non-realtime `POST /v1/events`
// telemetry uplink, which (unlike the realtime Client) keeps no resend
// queue and so has nothing to Enqueue. Because both AllocSeq and Enqueue
// draw from the one counter, every reliable event this device emits — over
// the realtime Client or the best-effort uplink, in any process — gets a
// strictly increasing device_seq that never resets across process
// restarts. That is what keeps the server's never-pruned
// (device_id, device_seq) dedup from silently discarding a fresh process's
// events as duplicates. The allocated seq is >= 1 (SPEC.md §5.1).
//
// Cross-process safety comes from SQLite: the store's single connection +
// `_txlock=immediate` + `busy_timeout` serialize concurrent AllocSeq/Enqueue
// callers (even in separate OS processes sharing the file) so no two ever
// observe the same next_seq.
func (s *Store) AllocSeq(ctx context.Context, space string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("outbox: begin alloc seq: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit
	seq, err := nextSeqTx(tx, space)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO space_seq_counters (space, next_seq) VALUES (?, ?)
		 ON CONFLICT(space) DO UPDATE SET next_seq = excluded.next_seq`,
		space, seq+1,
	); err != nil {
		return 0, fmt.Errorf("outbox: advance counter (alloc seq): %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("outbox: commit alloc seq: %w", err)
	}
	return seq, nil
}

// Ack deletes exactly the (space, device_seq) pair, per SPEC.md section 5.3
// ("A source device MUST keep every sent task.event/message.event in a
// local resend queue until it receives an ack that covers its device_seq").
// Acking a seq that is not present (already acked, or never enqueued here)
// is a no-op, not an error — acks are idempotent (SPEC.md section 5.2).
//
// An ack is also one of the two triggers (the other is Store open) that
// must notice freed outbox capacity and promote a waiting overflow row: it
// attempts exactly one promotion afterward, best-effort. A promotion
// failure here does NOT fail the ack itself (the ack succeeded; the event
// really was acknowledged) — but unlike an earlier version of this method,
// it is no longer silently swallowed: it is reported via SetLogf's logger,
// because this is the ONE promotion trigger that fires unconditionally on
// every ack, and a caller that never sees the failure has no signal that
// the freed capacity went unused. This alone is not sufficient recovery,
// though: if this was the last ack the outbox will ever see for a while
// (nothing else pending to send/ack), nothing else would retry promotion
// until the next ack or a process restart. The caller-side fix for that —
// internal/realtime/client's periodic replay/resend tick proactively
// retrying promotion on its own, independent of any ack — is what actually
// closes the stranding hole; this logging is what makes a failure here
// observable instead of invisible.
func (s *Store) Ack(ctx context.Context, space string, seq int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM outbox WHERE space = ? AND device_seq = ?`, space, seq)
	if err != nil {
		return fmt.Errorf("outbox: ack seq %d: %w", seq, err)
	}
	if _, err := s.PromoteOverflow(ctx, 1); err != nil {
		s.logAt("outbox: ack seq %d: promote overflow: %v", seq, err)
	}
	return nil
}

// Pending returns every outstanding (unacknowledged) item for space,
// oldest-first by device_seq — the order SPEC.md section 5.3/5.4 requires
// for reconnect replay ("resend every still-unacked event, oldest first,
// immediately after hello/hello-accept completes on a new connection").
// Because the outbox is durable, this also serves a process restart: the
// queue is exactly as it was left, so a freshly started daemon replays from
// disk with no special-cased recovery path.
func (s *Store) Pending(ctx context.Context, space string) ([]Item, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT space, device_seq, kind, body, created_at FROM outbox WHERE space = ? ORDER BY device_seq ASC`,
		space)
	if err != nil {
		return nil, fmt.Errorf("outbox: query pending: %w", err)
	}
	defer rows.Close()
	var items []Item
	for rows.Next() {
		var it Item
		var body string
		if err := rows.Scan(&it.Space, &it.DeviceSeq, &it.Kind, &body, &it.CreatedAt); err != nil {
			return nil, fmt.Errorf("outbox: scan pending: %w", err)
		}
		it.Body = json.RawMessage(body)
		items = append(items, it)
	}
	return items, rows.Err()
}

// Count returns the number of unacknowledged items across every space,
// mainly for observability/tests.
func (s *Store) Count(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox`).Scan(&n)
	return n, err
}

// OverflowItem is one durably-persisted, not-yet-promoted reliable event
// sitting in outbox_overflow. Unlike Item it carries no DeviceSeq — none has
// been allocated yet — and an ID instead, which is both its promotion-FIFO
// key and the handle DeleteOverflow needs.
type OverflowItem struct {
	ID        int64
	Space     string
	Kind      string
	Body      json.RawMessage // pre-seq material; device_seq is a placeholder until promoted
	CreatedAt int64
}

// OverflowPending returns every overflowed item for space, oldest-first by
// id — the same FIFO order PromoteOverflow honors. Intended for a caller
// that needs to drain overflow through a path other than promotion, e.g.
// the daemon's legacy-HTTP fallback when the server has disabled realtime
// entirely (see internal/uplink).
func (s *Store) OverflowPending(ctx context.Context, space string) ([]OverflowItem, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, space, kind, body, created_at FROM outbox_overflow WHERE space = ? ORDER BY id ASC`,
		space)
	if err != nil {
		return nil, fmt.Errorf("outbox: query overflow pending: %w", err)
	}
	defer rows.Close()
	var items []OverflowItem
	for rows.Next() {
		var it OverflowItem
		var body string
		if err := rows.Scan(&it.ID, &it.Space, &it.Kind, &body, &it.CreatedAt); err != nil {
			return nil, fmt.Errorf("outbox: scan overflow pending: %w", err)
		}
		it.Body = json.RawMessage(body)
		items = append(items, it)
	}
	return items, rows.Err()
}

// DeleteOverflow removes exactly the given overflow row by id, without
// promoting it into outbox or allocating it a device_seq — for a caller
// that has durably delivered the event through some other path entirely
// (the legacy-HTTP fallback) and is retiring it the same way Ack retires a
// promoted item. Deleting an id that is not present is a no-op.
func (s *Store) DeleteOverflow(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM outbox_overflow WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("outbox: delete overflow id %d: %w", id, err)
	}
	return nil
}

// OverflowCount returns the number of items currently sitting in
// outbox_overflow across every space, mainly for observability/tests.
func (s *Store) OverflowCount(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_overflow`).Scan(&n)
	return n, err
}
