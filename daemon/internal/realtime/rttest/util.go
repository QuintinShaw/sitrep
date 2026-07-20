package rttest

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// newID returns an opaque envelope id matching common.schema.json's
// envelope_id pattern.
func newID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "rt" + hex.EncodeToString(b)
}

// NowMS returns the current time as Unix milliseconds, always within the
// valid unix_ms_timestamp bound (SPEC.md section 3.1).
func NowMS() int64 { return time.Now().UnixMilli() }
