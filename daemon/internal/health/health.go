// Package health maintains a small, persistent daemon health-status
// directory that the macOS menubar app reads to surface degraded states the
// user should know about — e.g. the device_seq allocator failing on disk (so
// events are being skipped rather than sent with a colliding sequence, N1),
// the durable outbox hitting its backpressure cap (5d), or the outbox
// failing to open at all. A failure that would otherwise be only a log line
// becomes an explicit, visible state.
//
// On-disk contract (docs/design/v1-architecture.md §14 — do not diverge):
//   - Path: ~/.config/sitrep/health.d/<component>.json (config.Dir()/health.d).
//     ONE FILE PER COMPONENT, replacing the earlier single health.json:
//     independent, short-lived daemon processes (`sitrep run`, `sitrep
//     report`, `sitrep agent`) that each hold their own in-memory view of
//     "what's currently wrong" no longer race a read-modify-write on one
//     shared file and clobber each other's component entries — concurrent
//     processes touching DIFFERENT components touch different files, and
//     touching the SAME component is simple last-write-wins on one small
//     file.
//   - Shape: {"ok": bool, "reason": string}; reason explains an ok:false.
//   - Absence == healthy, at both levels: the whole health.d/ directory
//     missing, or one specific component's file missing, both mean healthy.
//     A component is never "unknown/error" by default.
//   - Writes are atomic (temp file + rename) so a reader's poll never
//     observes a half-written file.
//   - Staleness: an aggregating reader (Load, and the menubar) treats a
//     component file as contributing to the combined warning only if it is
//     BOTH ok:false AND not stale (now - mtime <= StaleAfter, 5 minutes) —
//     this lets a short-lived process's failure self-heal without a
//     guaranteed follow-up write.
package health

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/config"
)

// fileStatus is the exact on-disk shape of one health.d/<component>.json.
type fileStatus struct {
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
}

// StaleAfter bounds how old a component file's mtime may get before an
// aggregating reader (Load) treats a still-on-disk ok:false as resolved,
// even with no fresh write — the frozen HEALTH_STALE_AFTER_MS constant
// (v1-architecture.md §14): 300000ms.
const StaleAfter = 5 * time.Minute

var (
	mu    sync.Mutex
	dirFn = defaultDir
)

func defaultDir() string {
	dir := config.Dir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "health.d")
}

// Dir returns the health.d directory location.
func Dir() string { return dirFn() }

// Report atomically writes health.d/<component>.json = {"ok":false,
// "reason":reason}. Re-reporting the identical reason still writes (unlike
// the old single-file design's in-memory dedup) — each component file is
// independent and small, so a redundant write is cheap, and per-process
// in-memory dedup at the CALL SITE (e.g. cmd/sitrep's seqAllocator using an
// atomic.Bool to report only on a transition) is what actually matters for
// avoiding write storms on a hot failing path.
func Report(component, reason string) error {
	return write(component, fileStatus{OK: false, Reason: reason})
}

// Clear removes component's health.d file. Absence means healthy (see the
// package doc comment) — this is the simplest way to recover immediately,
// without waiting out StaleAfter. No-op, not an error, if the directory or
// the file is already gone.
func Clear(component string) error {
	dir := dirFn()
	if dir == "" {
		return nil
	}
	if err := os.Remove(filepath.Join(dir, component+".json")); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func write(component string, s fileStatus) error {
	dir := dirFn()
	if dir == "" {
		return nil
	}
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return atomicWrite(dir, component+".json", append(data, '\n'))
}

// Status is the combined {ok, reason} view Load computes by scanning
// health.d/.
type Status struct {
	OK     bool
	Reason string
}

// Load scans health.d/, discards stale entries (StaleAfter), and combines
// the remaining non-stale ok:false files' reasons into one Status — the
// union of currently-live failures, matching what a reader (the menubar; the
// daemon's own tests) needs. A missing/unreadable directory, or one with no
// live ok:false files, is healthy.
func Load() Status {
	dir := dirFn()
	if dir == "" {
		return Status{OK: true}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return Status{OK: true} // missing/unreadable directory == healthy
	}
	type issue struct{ component, reason string }
	var issues []issue
	now := time.Now()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if now.Sub(info.ModTime()) > StaleAfter {
			continue // stale ok:false == resolved for aggregation (§14)
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var s fileStatus
		if err := json.Unmarshal(data, &s); err != nil {
			continue // malformed == healthy, same as the old single-file design
		}
		if !s.OK {
			issues = append(issues, issue{component: strings.TrimSuffix(e.Name(), ".json"), reason: s.Reason})
		}
	}
	if len(issues) == 0 {
		return Status{OK: true}
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].component < issues[j].component })
	parts := make([]string, len(issues))
	for i, is := range issues {
		parts[i] = is.reason
	}
	return Status{OK: false, Reason: strings.Join(parts, "; ")}
}

// SetDirForTest overrides the health.d directory location. Test-only —
// production uses config.Dir()/health.d.
func SetDirForTest(d string) {
	mu.Lock()
	defer mu.Unlock()
	dirFn = func() string { return d }
}

// atomicWrite writes data to a sibling temp file inside dir and renames it
// over dir/name, so a concurrent reader (the menubar's poll) never observes
// a partial write — the same temp-file-plus-rename pattern the earlier
// single-file design used, now applied per-component-file.
func atomicWrite(dir, name string, data []byte) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".health-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(dir, name))
}
