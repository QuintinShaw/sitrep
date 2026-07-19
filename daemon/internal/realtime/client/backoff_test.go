package client

import (
	"testing"
	"time"
)

func fixedRand(v float64) func() float64 { return func() float64 { return v } }

func TestBackoffDoublesAndCaps(t *testing.T) {
	base := time.Second
	max := 60 * time.Second
	mid := fixedRand(0.5) // r=0.5 => factor=1 => no jitter distortion

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, 60 * time.Second}, // would be 64s, capped
		{7, 60 * time.Second},
		{100, 60 * time.Second},
	}
	for _, c := range cases {
		got := Backoff(c.attempt, base, max, 0.2, mid)
		if got != c.want {
			t.Errorf("Backoff(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	base := time.Second
	max := 60 * time.Second
	attempt := 3 // nominal 8s
	nominal := 8 * time.Second
	jitter := 0.2

	for _, r := range []float64{0, 0.25, 0.5, 0.75, 1} {
		got := Backoff(attempt, base, max, jitter, fixedRand(r))
		lo := time.Duration(float64(nominal) * (1 - jitter))
		hi := time.Duration(float64(nominal) * (1 + jitter))
		if got < lo || got > hi {
			t.Errorf("Backoff with r=%v = %v, want within [%v, %v]", r, got, lo, hi)
		}
	}
}

func TestBackoffZeroJitterIsDeterministic(t *testing.T) {
	got1 := Backoff(2, time.Second, 60*time.Second, 0, fixedRand(0.99))
	got2 := Backoff(2, time.Second, 60*time.Second, 0, fixedRand(0.01))
	if got1 != got2 || got1 != 4*time.Second {
		t.Fatalf("zero jitter should ignore randFn entirely: got %v and %v, want 4s both", got1, got2)
	}
}

func TestBackoffNegativeAttemptClampsToZero(t *testing.T) {
	got := Backoff(-5, time.Second, 60*time.Second, 0, fixedRand(0.5))
	if got != time.Second {
		t.Fatalf("Backoff(-5) = %v, want base (1s)", got)
	}
}

// TestBackoffMonotonicNonDecreasing pins the "1s起, 2倍, 上限60s" shape
// across a full attempt sweep without ever sleeping — the whole point of
// keeping Backoff a pure function.
func TestBackoffMonotonicNonDecreasing(t *testing.T) {
	base, max := time.Second, 60*time.Second
	prev := time.Duration(0)
	for attempt := 0; attempt < 10; attempt++ {
		got := Backoff(attempt, base, max, 0, nil)
		if got < prev {
			t.Fatalf("attempt %d: backoff %v < previous %v (must be non-decreasing)", attempt, got, prev)
		}
		if got > max {
			t.Fatalf("attempt %d: backoff %v exceeds cap %v", attempt, got, max)
		}
		prev = got
	}
}
