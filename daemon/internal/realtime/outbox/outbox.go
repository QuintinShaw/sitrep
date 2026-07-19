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
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

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
	db *sql.DB
}

// Open creates (if needed) and opens the outbox database at path. Callers
// should keep one *Store per daemon process for a given path.
func Open(path string) (*Store, error) {
	// _txlock=immediate: acquire the write lock at BEGIN rather than at
	// first write, so two overlapping transactions serialize instead of
	// racing to upgrade a deferred lock (SQLITE_BUSY under concurrent
	// Enqueue calls from multiple goroutines).
	dsn := path + "?_pragma=busy_timeout(5000)&_txlock=immediate"
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
	return &Store{db: db}, nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error { return s.db.Close() }

// BuildBody produces the JSON body for a reliable event once its device_seq
// is known — Enqueue calls it inside the allocating transaction so the
// caller never has to allocate a seq itself ahead of time.
type BuildBody func(seq int64) (json.RawMessage, error)

// Enqueue allocates the next device_seq for space and durably stores the
// event BuildBody produces for it, atomically: SPEC.md section 5.1 requires
// device_seq to be assigned once, never reused, and strictly increasing per
// (device, space); doing the allocation and the insert in one transaction
// means a process crash between them can never happen — the transaction
// either commits both effects or neither, so a restarted daemon reusing this
// same database file always sees a counter that agrees with what is (or
// isn't) sitting in the outbox.
func (s *Store) Enqueue(ctx context.Context, space, kind string, build BuildBody) (Item, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Item{}, fmt.Errorf("outbox: begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit

	seq, err := nextSeqTx(tx, space)
	if err != nil {
		return Item{}, err
	}
	body, err := build(seq)
	if err != nil {
		return Item{}, fmt.Errorf("outbox: build body for seq %d: %w", seq, err)
	}
	now := time.Now().UnixMilli()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (space, device_seq, kind, body, created_at) VALUES (?, ?, ?, ?, ?)`,
		space, seq, kind, string(body), now,
	); err != nil {
		return Item{}, fmt.Errorf("outbox: insert seq %d: %w", seq, err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO space_seq_counters (space, next_seq) VALUES (?, ?)
		 ON CONFLICT(space) DO UPDATE SET next_seq = excluded.next_seq`,
		space, seq+1,
	); err != nil {
		return Item{}, fmt.Errorf("outbox: advance counter: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Item{}, fmt.Errorf("outbox: commit: %w", err)
	}
	return Item{Space: space, DeviceSeq: seq, Kind: kind, Body: body, CreatedAt: now}, nil
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

// Ack deletes exactly the (space, device_seq) pair, per SPEC.md section 5.3
// ("A source device MUST keep every sent task.event/message.event in a
// local resend queue until it receives an ack that covers its device_seq").
// Acking a seq that is not present (already acked, or never enqueued here)
// is a no-op, not an error — acks are idempotent (SPEC.md section 5.2).
func (s *Store) Ack(ctx context.Context, space string, seq int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM outbox WHERE space = ? AND device_seq = ?`, space, seq)
	if err != nil {
		return fmt.Errorf("outbox: ack seq %d: %w", seq, err)
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
