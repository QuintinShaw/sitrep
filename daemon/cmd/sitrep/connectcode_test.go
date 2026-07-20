package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuintinShaw/sitrep/daemon/internal/config"
)

// buildConnectCode assembles a valid 21-char self-routing connect code
// (v1-architecture.md §10.5) from a space_id and secret, uppercased — the
// display/scan form — so tests don't hand-transcribe the layout and risk an
// off-by-one.
func buildConnectCode(spaceID, secret string) string {
	return strings.ToUpper("X" + spaceID + secret + "Z")
}

// TestDecodeConnectCodeRoundTrip pins the exact byte/char layout
// v1-architecture.md §10.5 specifies: 'X' + 10-char space_id + 9-char secret
// + 'Z', 21 characters total, case-insensitive, decoded to lowercase.
func TestDecodeConnectCodeRoundTrip(t *testing.T) {
	const spaceID = "k7m3qzx2vt" // 10 chars, connect-code alphabet only
	const secret = "a2b3c4d5e"   // 9 chars, connect-code alphabet only
	code := buildConnectCode(spaceID, secret)

	if len(code) != 21 {
		t.Fatalf("test fixture code length = %d, want 21: %q", len(code), code)
	}

	gotSpace, gotSecret, err := DecodeConnectCode(code)
	if err != nil {
		t.Fatalf("DecodeConnectCode: %v", err)
	}
	if gotSpace != spaceID {
		t.Fatalf("decoded space_id = %q, want %q", gotSpace, spaceID)
	}
	if gotSecret != secret {
		t.Fatalf("decoded secret = %q, want %q", gotSecret, secret)
	}

	// Lowercase input decodes identically (case-insensitive).
	gotSpace2, gotSecret2, err := DecodeConnectCode(strings.ToLower(code))
	if err != nil {
		t.Fatalf("DecodeConnectCode (lowercase): %v", err)
	}
	if gotSpace2 != spaceID || gotSecret2 != secret {
		t.Fatalf("lowercase decode mismatch: space=%q secret=%q", gotSpace2, gotSecret2)
	}
}

// TestDecodeConnectCodeRejectsMalformed pins the defense-in-depth structural
// checks §10.5 specifies: wrong length, wrong/missing anchors, and an
// out-of-alphabet character are all rejected locally before ever reaching
// the network.
func TestDecodeConnectCodeRejectsMalformed(t *testing.T) {
	valid := buildConnectCode("k7m3qzx2vt", "a2b3c4d5e")

	cases := map[string]string{
		"too short":          valid[:20],
		"too long":           valid + "X",
		"wrong start anchor": "Y" + valid[1:],
		"wrong end anchor":   valid[:20] + "Y",
		// '0', '1', 'I', 'L', 'O' are excluded from the alphabet.
		"out-of-alphabet digit 0":  "X" + "0" + valid[2:],
		"out-of-alphabet letter I": "X" + "I" + valid[2:],
	}
	for name, code := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeConnectCode(code); err == nil {
				t.Fatalf("DecodeConnectCode(%q) succeeded, want a malformed-code error", code)
			}
		})
	}
}

// TestJoinDecodesSpaceFromConnectCode is the P0-6 daemon-side repro: `sitrep
// join --code <21-char self-routing code>` (no --space) decodes space_id
// LOCALLY, in Go, and sends it as `space` in the POST /v1/join body — the
// load-bearing fix that removes the KV round-trip from the join path
// (v1-architecture.md §10.5).
func TestJoinDecodesSpaceFromConnectCode(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	const spaceID = "k7m3qzx2vt"
	code := buildConnectCode(spaceID, "a2b3c4d5e")

	var gotBody struct {
		Space string `json:"space"`
		Code  string `json:"code"`
	}
	var gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"token":"sr1_k7m3qzx2vt_05b971d953ce184904e159f6886091b6216155fc8b35b6e7","device_id":"dev_9c4e2b7a1f","role":"source","space_id":"k7m3qzx2vt"}`))
	}))
	defer srv.Close()

	// Note: no --space flag at all.
	cmdJoin([]string{"--server", srv.URL, "--code", code})

	if gotMethod != http.MethodPost || gotPath != "/v1/join" {
		t.Fatalf("join hit %s %s, want POST /v1/join", gotMethod, gotPath)
	}
	if gotBody.Space != spaceID {
		t.Fatalf("POST /v1/join body space=%q, want the code-decoded %q — space is required on every join call (§10.5) and must be decoded locally when --space is omitted", gotBody.Space, spaceID)
	}
	if gotBody.Code != strings.ToUpper(code) {
		t.Fatalf("POST /v1/join body code=%q, want %q", gotBody.Code, strings.ToUpper(code))
	}
	if cfg := config.Load(); cfg.Space != spaceID {
		t.Fatalf("join did not persist the decoded space: %+v", cfg)
	}
}

// TestJoinExplicitSpaceOverridesDecodedOne pins that an explicitly-passed
// --space (the self-host deep-link path, which already carries `space`
// separately from `code`) is used as-is, without requiring it to match
// whatever the code would decode to — decoding is a convenience for the
// connect-code path, not a validation gate the daemon enforces locally (the
// server still validates the two agree, §10.5 step 3).
func TestJoinExplicitSpaceOverridesDecodedOne(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	// The code decodes to "k7m3qzx2vt", but --space explicitly names a
	// different value — the explicit flag must win (used as-is).
	code := buildConnectCode("k7m3qzx2vt", "a2b3c4d5e")

	var gotBody struct {
		Space string `json:"space"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"token":"sr1_otherspace_05b971d953ce184904e159f6886091b6216155fc8b35b6e7","device_id":"dev_9c4e2b7a1f","role":"source","space_id":"otherspace"}`))
	}))
	defer srv.Close()

	cmdJoin([]string{"--server", srv.URL, "--space", "otherspace", "--code", code})

	if gotBody.Space != "otherspace" {
		t.Fatalf("POST /v1/join body space=%q, want the explicitly-passed \"otherspace\"", gotBody.Space)
	}
}
