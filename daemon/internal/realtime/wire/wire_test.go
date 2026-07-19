package wire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"
)

// fixturesDir locates proto/realtime/fixtures relative to this source file,
// independent of the test runner's working directory.
func fixturesDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file) // .../daemon/internal/realtime/wire
	root := filepath.Join(dir, "..", "..", "..", "..")
	fixtures := filepath.Join(root, "proto", "realtime", "fixtures")
	if _, err := os.Stat(fixtures); err != nil {
		t.Fatalf("fixtures dir not found at %s: %v", fixtures, err)
	}
	return fixtures
}

func listJSONFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".json" {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	sort.Strings(out)
	return out
}

// normalize decodes JSON into a generic interface{} tree so two
// differently-formatted (but semantically identical) documents compare
// equal with reflect.DeepEqual.
func normalize(t *testing.T, data []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	return v
}

// TestValidFixturesRoundTrip decodes every bare-envelope fixture under
// fixtures/valid and fixtures/scenarios, re-encodes it through the Go
// types, and asserts the result is semantically identical to the original.
// This is the "Go types match the schema byte for byte" check called for by
// the realtime daemon work order: if a field were missing, renamed, or
// mistyped in bodies.go, this test would lose or corrupt it on round trip.
func TestValidFixturesRoundTrip(t *testing.T) {
	fixtures := fixturesDir(t)
	dirs := []string{
		filepath.Join(fixtures, "valid"),
		filepath.Join(fixtures, "scenarios"),
	}
	tested := 0
	for _, dir := range dirs {
		for _, path := range listJSONFiles(t, dir) {
			if filepath.Base(path) == "README.md" {
				continue
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			t.Run(relPath(fixtures, path), func(t *testing.T) {
				env, err := DecodeEnvelope(data)
				if err != nil {
					t.Fatalf("DecodeEnvelope: %v", err)
				}
				body, err := DecodeBody(env)
				if err != nil {
					t.Fatalf("DecodeBody: %v", err)
				}
				reEncodedBody, err := json.Marshal(body)
				if err != nil {
					t.Fatalf("re-marshal body: %v", err)
				}
				roundTripped := Envelope{Type: env.Type, ID: env.ID, TS: env.TS, Body: reEncodedBody}
				out, err := roundTripped.Encode()
				if err != nil {
					t.Fatalf("Encode: %v", err)
				}
				want := normalize(t, data)
				got := normalize(t, out)
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("round trip mismatch:\n original: %s\n produced: %s", data, out)
				}
			})
			tested++
		}
	}
	if tested == 0 {
		t.Fatal("no valid fixtures were exercised")
	}
	t.Logf("round-tripped %d fixtures", tested)
}

func relPath(base, path string) string {
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return path
	}
	return rel
}

// roleFixture is the {"sender_role": ..., "frame": {...}} wrapper format
// used by a handful of invalid fixtures to test the section 10.1
// authorization matrix rather than pure schema shape (SPEC.md section 16).
type roleFixture struct {
	SenderRole string          `json:"sender_role"`
	Frame      json.RawMessage `json:"frame"`
}

func isRoleFixture(data []byte) (roleFixture, bool) {
	var rf roleFixture
	if err := json.Unmarshal(data, &rf); err != nil {
		return roleFixture{}, false
	}
	return rf, rf.SenderRole != "" && rf.Frame != nil
}

// TestInvalidFixturesRejected asserts every fixture under fixtures/invalid
// is rejected by this package, either at envelope decode, body decode/
// validate, or (for the sender_role-wrapped fixtures) the authorization
// matrix.
func TestInvalidFixturesRejected(t *testing.T) {
	fixtures := fixturesDir(t)
	dir := filepath.Join(fixtures, "invalid")
	tested := 0
	for _, path := range listJSONFiles(t, dir) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		t.Run(relPath(fixtures, path), func(t *testing.T) {
			if rf, ok := isRoleFixture(data); ok {
				env, err := DecodeEnvelope(rf.Frame)
				if err != nil {
					return // rejected at envelope level: acceptable
				}
				body, err := DecodeBody(env)
				if err != nil {
					return // rejected at body validation: acceptable
				}
				if err := Authorize(Role(rf.SenderRole), env.Type, body); err == nil {
					t.Fatalf("expected rejection for role %q sending %q, got none", rf.SenderRole, env.Type)
				}
				return
			}
			env, err := DecodeEnvelope(data)
			if err != nil {
				return // rejected at envelope level: acceptable
			}
			if _, err := DecodeBody(env); err == nil {
				t.Fatalf("expected DecodeBody to reject fixture, got none")
			}
		})
		tested++
	}
	if tested == 0 {
		t.Fatal("no invalid fixtures were exercised")
	}
	t.Logf("checked %d invalid fixtures", tested)
}

// TestUnixMSBoundary pins the seconds/milliseconds guard from
// common.schema.json#/$defs/unix_ms_timestamp (SPEC.md section 3.1).
func TestUnixMSBoundary(t *testing.T) {
	cases := []struct {
		ts   int64
		want bool
	}{
		{1000000000000, true},   // exact minimum
		{9999999999999, true},   // exact maximum
		{999999999999, false},   // one below minimum
		{10000000000000, false}, // one above maximum
		{1752480006, false},     // a plausible *seconds* value must be rejected
	}
	for _, c := range cases {
		if got := ValidUnixMS(c.ts); got != c.want {
			t.Errorf("ValidUnixMS(%d) = %v, want %v", c.ts, got, c.want)
		}
	}
}
