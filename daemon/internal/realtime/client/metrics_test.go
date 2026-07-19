package client

import (
	"testing"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/realtime/wire"
)

func sample(id, value string) wire.MetricSample {
	return wire.MetricSample{MetricID: id, Value: value, TS: wire.MinUnixMS + 1000}
}

func TestMetricBatcherMergesMultipleSamplesIntoOneFrame(t *testing.T) {
	b := newMetricBatcher(100*time.Millisecond, 30*time.Second)
	base := time.Unix(1700000000, 0)

	// Three updates to the same metric_id before any flush: only the
	// latest must survive (SPEC.md section 12).
	b.Offer(sample("cpu.load", "0.1"))
	b.Offer(sample("cpu.load", "0.2"))
	b.Offer(sample("cpu.load", "0.3"))
	b.Offer(sample("mem.used_gb", "18.4"))

	out, ok := b.Flush(base)
	if !ok {
		t.Fatal("Flush: expected a due frame on first call")
	}
	if len(out) != 2 {
		t.Fatalf("Flush returned %d samples, want 2 (one per metric_id)", len(out))
	}
	byID := map[string]wire.MetricSample{}
	for _, s := range out {
		byID[s.MetricID] = s
	}
	if byID["cpu.load"].Value != "0.3" {
		t.Fatalf("cpu.load = %q, want latest value 0.3", byID["cpu.load"].Value)
	}
}

func TestMetricBatcherRateLimitsPerMetric(t *testing.T) {
	b := newMetricBatcher(10*time.Millisecond, 30*time.Second)
	base := time.Unix(1700000000, 0)

	b.Offer(sample("cpu.load", "0.1"))
	out, ok := b.Flush(base)
	if !ok || len(out) != 1 {
		t.Fatalf("first flush = %v, %v; want 1 sample", out, ok)
	}

	// Immediately offer a new value and try to flush again well within the
	// per-metric 500ms gate (even though the batcher's own interval has
	// elapsed) — the sample must be held back, not dropped.
	b.Offer(sample("cpu.load", "0.9"))
	out, ok = b.Flush(base.Add(50 * time.Millisecond))
	if ok {
		t.Fatalf("expected no due frame within the 500ms per-metric gate, got %v", out)
	}

	// After the gate clears, the latest pending value (0.9) must appear.
	out, ok = b.Flush(base.Add(600 * time.Millisecond))
	if !ok || len(out) != 1 || out[0].Value != "0.9" {
		t.Fatalf("flush after gate = %v, %v; want [0.9]", out, ok)
	}
}

func TestMetricBatcherOverallIntervalGate(t *testing.T) {
	b := newMetricBatcher(200*time.Millisecond, 30*time.Second)
	base := time.Unix(1700000000, 0)

	b.Offer(sample("cpu.load", "0.1"))
	if _, ok := b.Flush(base); !ok {
		t.Fatal("expected first flush to be due")
	}
	b.Offer(sample("mem.used_gb", "1"))
	if _, ok := b.Flush(base.Add(50 * time.Millisecond)); ok {
		t.Fatal("expected flush before the batcher interval elapsed to be a no-op")
	}
	if out, ok := b.Flush(base.Add(250 * time.Millisecond)); !ok || len(out) != 1 {
		t.Fatalf("flush after interval elapsed = %v, %v; want 1 sample", out, ok)
	}
}

func TestMetricBatcherThrottleSwitchesCadence(t *testing.T) {
	b := newMetricBatcher(100*time.Millisecond, 10*time.Second)
	base := time.Unix(1700000000, 0)

	b.Offer(sample("cpu.load", "0.1"))
	if _, ok := b.Flush(base); !ok {
		t.Fatal("expected first flush to be due")
	}

	b.SetThrottled(true)
	b.Offer(sample("cpu.load", "0.2"))
	// Well past the *normal* interval but nowhere near the throttled one.
	if _, ok := b.Flush(base.Add(500 * time.Millisecond)); ok {
		t.Fatal("expected throttled batcher to withhold the frame")
	}
	if out, ok := b.Flush(base.Add(11 * time.Second)); !ok || len(out) != 1 {
		t.Fatalf("flush past throttled interval = %v, %v; want 1 sample", out, ok)
	}

	b.SetThrottled(false)
	b.Offer(sample("cpu.load", "0.3"))
	// +700ms past the last flush at 11s: clears both the batcher's now-fast
	// interval and the per-metric 500ms gate.
	if _, ok := b.Flush(base.Add(11700 * time.Millisecond)); !ok {
		t.Fatal("expected resume_rate to restore the normal (fast) cadence")
	}
}

func TestMetricBatcherEmptyIsNeverDue(t *testing.T) {
	b := newMetricBatcher(time.Millisecond, time.Second)
	if out, ok := b.Flush(time.Now()); ok {
		t.Fatalf("expected no frame with nothing pending, got %v", out)
	}
}
