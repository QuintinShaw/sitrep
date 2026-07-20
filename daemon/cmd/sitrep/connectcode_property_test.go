package main

import (
	"math/rand"
	"strings"
	"testing"
)

// TestDecodeConnectCodePropertyRoundTrip is a self-contained property test
// (no external corpus/fixture files) generating a large number of random
// valid connect codes with the exact v1-architecture.md §10.5 layout —
// 'X' + 10-char space_id + 9-char secret + 'Z', drawn from
// connectCodeAlphabet ("23456789abcdefghjkmnpqrstuvwxyz") — and asserts
// DecodeConnectCode round-trips every one of them, including codes whose
// body happens to contain the 'x' or 'z' alphabet symbols (which are valid
// body characters; only position 0 and position 20 are anchors) and codes
// given in lowercase input form.
func TestDecodeConnectCodePropertyRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const n = 2000

	randBody := func(length int) string {
		b := make([]byte, length)
		for i := range b {
			b[i] = connectCodeAlphabet[rng.Intn(len(connectCodeAlphabet))]
		}
		return string(b)
	}

	for i := 0; i < n; i++ {
		spaceID := randBody(10)
		secret := randBody(9)
		code := strings.ToUpper("X" + spaceID + secret + "Z")

		gotSpace, gotSecret, err := DecodeConnectCode(code)
		if err != nil {
			t.Fatalf("case %d: DecodeConnectCode(%q) failed: %v", i, code, err)
		}
		if gotSpace != spaceID || gotSecret != secret {
			t.Fatalf("case %d: DecodeConnectCode(%q) = (%q, %q), want (%q, %q)", i, code, gotSpace, gotSecret, spaceID, secret)
		}

		// Lowercase input must decode identically (case-insensitive).
		gotSpace2, gotSecret2, err := DecodeConnectCode(strings.ToLower(code))
		if err != nil {
			t.Fatalf("case %d: DecodeConnectCode(lowercase %q) failed: %v", i, strings.ToLower(code), err)
		}
		if gotSpace2 != spaceID || gotSecret2 != secret {
			t.Fatalf("case %d: DecodeConnectCode(lowercase) = (%q, %q), want (%q, %q)", i, gotSpace2, gotSecret2, spaceID, secret)
		}
	}
}

// TestDecodeConnectCodeBodyContainsAnchorLetters pins that 'x'/'X' and
// 'z'/'Z' — both valid connectCodeAlphabet symbols — are accepted when they
// appear WITHIN the space_id/secret body (positions 1-19). Decoding is
// purely positional (only index 0 and index 20 are anchor checks), so an
// 'x' or 'z' inside the body must never be mistaken for a misplaced anchor
// or otherwise rejected.
func TestDecodeConnectCodeBodyContainsAnchorLetters(t *testing.T) {
	// space_id and secret deliberately start, end, and are laced with x/z.
	const spaceID = "xz2x3z4xzx" // 10 chars
	const secret = "zx5z6xzxz"   // 9 chars
	code := strings.ToUpper("X" + spaceID + secret + "Z")

	gotSpace, gotSecret, err := DecodeConnectCode(code)
	if err != nil {
		t.Fatalf("DecodeConnectCode(%q) failed: %v", code, err)
	}
	if gotSpace != spaceID {
		t.Fatalf("decoded space_id = %q, want %q", gotSpace, spaceID)
	}
	if gotSecret != secret {
		t.Fatalf("decoded secret = %q, want %q", gotSecret, secret)
	}
}
