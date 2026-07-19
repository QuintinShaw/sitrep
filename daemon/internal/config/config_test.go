package config

import "testing"

// TestLoadOutboxMaxRows pins the SITREP_OUTBOX_MAX_ROWS wiring (the P0
// review's "cap isn't wired to config" note): Load must surface it as
// Config.OutboxMaxRows, defaulting to 0 (meaning "use
// outbox.DefaultMaxRows") when unset or invalid.
func TestLoadOutboxMaxRows(t *testing.T) {
	// HOME points at an empty temp dir so Load never reads a real user
	// config.json out from under this test.
	t.Setenv("HOME", t.TempDir())

	cases := []struct {
		name string
		env  string
		want int
	}{
		{name: "unset defaults to zero (outbox.DefaultMaxRows)", env: "", want: 0},
		{name: "explicit value wins", env: "12345", want: 12345},
		{name: "non-numeric is ignored", env: "not-a-number", want: 0},
		{name: "zero is ignored (not a valid cap)", env: "0", want: 0},
		{name: "negative is ignored", env: "-5", want: 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("SITREP_OUTBOX_MAX_ROWS", c.env)
			if got := Load().OutboxMaxRows; got != c.want {
				t.Fatalf("Load().OutboxMaxRows = %d, want %d", got, c.want)
			}
		})
	}
}

// TestRealtimeURLFor pins the agreed cross-implementation realtime
// endpoint path (/v3/realtime) and the http(s) -> ws(s) scheme mapping.
func TestRealtimeURLFor(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "https becomes wss with v3 path",
			cfg:  Config{Server: "https://sitrep.example.com"},
			want: "wss://sitrep.example.com/v3/realtime",
		},
		{
			name: "http becomes ws with v3 path",
			cfg:  Config{Server: "http://localhost:8787"},
			want: "ws://localhost:8787/v3/realtime",
		},
		{
			name: "trailing slash on server is normalized",
			cfg:  Config{Server: "https://sitrep.example.com/"},
			want: "wss://sitrep.example.com/v3/realtime",
		},
		{
			name: "explicit RealtimeURL wins verbatim",
			cfg:  Config{Server: "https://sitrep.example.com", RealtimeURL: "wss://other.example.com/custom"},
			want: "wss://other.example.com/custom",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.cfg.RealtimeURLFor(); got != c.want {
				t.Fatalf("RealtimeURLFor() = %q, want %q", got, c.want)
			}
		})
	}
}
