package client

import "time"

// Backoff computes the delay before reconnect attempt N (0-based: attempt 0
// is the wait before the first retry after the first connection failure).
// The base delay doubles per attempt and is capped at max, then jittered by
// +/- jitterFrac (proportionally) using randFn, which must return a value
// in [0,1). This is a pure function — no sleeping, no shared state — so it
// can be tested by simply calling it with a fixed randFn and asserting on
// the numbers it returns.
func Backoff(attempt int, base, max time.Duration, jitterFrac float64, randFn func() float64) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
		if d > max {
			d = max
		}
	}
	if d > max {
		d = max
	}
	if jitterFrac <= 0 || randFn == nil {
		return d
	}
	r := randFn()
	if r < 0 {
		r = 0
	}
	if r > 1 {
		r = 1
	}
	// Map r in [0,1) to a factor in [1-jitterFrac, 1+jitterFrac].
	factor := 1 + (r*2-1)*jitterFrac
	return time.Duration(float64(d) * factor)
}
