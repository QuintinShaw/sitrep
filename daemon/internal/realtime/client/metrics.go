package client

import (
	"sync"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

// metricBatcher merges pending metric samples per metric_id (SPEC.md
// section 12: "A sender MUST merge multiple pending updates to the same
// metric_id into a single sample ... before emitting a frame") and gates
// emission at a bounded cadence: no metric_id may be re-included more than
// once per 500ms (the protocol's 2 Hz per-metric ceiling), and Flush is
// itself only "due" at the batcher's current interval (normal or, while
// throttled, the slower one), which — kept at or above 100ms by the
// client's poll loop — also keeps the connection under the 10 frames/sec
// cap (section 11).
//
// It is a pure, clock-driven data structure with no goroutine or timer of
// its own: the caller decides when to call Flush. This makes it directly
// unit-testable (inject instants, no sleeping) and keeps the "when do we
// actually write to the socket" decision in the Client, which knows whether
// a connection currently exists.
//
// metric.frame is never persisted and never acknowledged (SPEC.md section
// 12): a sample that arrives while disconnected simply overwrites the
// pending entry for its metric_id, same as any other merge — nothing is
// queued, and only the latest value survives to be sent after reconnect.
type metricBatcher struct {
	mu sync.Mutex

	pending map[string]wire.MetricSample

	normalInterval    time.Duration
	throttledInterval time.Duration
	throttled         bool

	lastFlushAt time.Time            // wall-clock time of the last non-empty Flush
	lastSentAt  map[string]time.Time // per metric_id: wall-clock time last included in a sent frame
}

func newMetricBatcher(normal, throttled time.Duration) *metricBatcher {
	return &metricBatcher{
		pending:           make(map[string]wire.MetricSample),
		lastSentAt:        make(map[string]time.Time),
		normalInterval:    normal,
		throttledInterval: throttled,
	}
}

// Offer merges one sample in: last value wins per metric_id.
func (b *metricBatcher) Offer(s wire.MetricSample) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending[s.MetricID] = s
}

// SetThrottled switches the batcher between its normal and throttled
// cadence (SPEC.md section 7: command{throttle}/{resume_rate}).
func (b *metricBatcher) SetThrottled(throttled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.throttled = throttled
}

// Throttled reports the batcher's current cadence state, mainly for
// observability and tests.
func (b *metricBatcher) Throttled() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.throttled
}

func (b *metricBatcher) interval() time.Duration {
	if b.throttled {
		return b.throttledInterval
	}
	return b.normalInterval
}

// Flush returns the next metric.frame's worth of samples if the batcher's
// current interval has elapsed since the last emission AND at least one
// pending sample clears its own per-metric_id 500ms gate. Samples that
// don't clear the gate are held back — merged with whatever arrives next —
// rather than dropped; a call that finds nothing eligible returns
// (Body{}, false) and does not disturb any bookkeeping.
func (b *metricBatcher) Flush(now time.Time) ([]wire.MetricSample, bool) {
	const perMetricGate = 500 * time.Millisecond

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.lastFlushAt.IsZero() && now.Sub(b.lastFlushAt) < b.interval() {
		return nil, false
	}
	if len(b.pending) == 0 {
		return nil, false
	}

	var out []wire.MetricSample
	for id, sample := range b.pending {
		if last, ok := b.lastSentAt[id]; ok && now.Sub(last) < perMetricGate {
			continue // held back for a later cycle, not dropped
		}
		out = append(out, sample)
	}
	if len(out) == 0 {
		return nil, false
	}
	for _, s := range out {
		delete(b.pending, s.MetricID)
		b.lastSentAt[s.MetricID] = now
	}
	b.lastFlushAt = now
	return out, true
}
