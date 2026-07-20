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

// TestRealtimeURLFor pins the frozen v1 realtime endpoint path
// (/v1/realtime, docs/design/v1-architecture.md section 2.1) and the
// http(s) -> ws(s) scheme mapping.
func TestRealtimeURLFor(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "https becomes wss with v1 path",
			cfg:  Config{Server: "https://sitrep.example.com"},
			want: "wss://sitrep.example.com/v1/realtime",
		},
		{
			name: "http becomes ws with v1 path",
			cfg:  Config{Server: "http://localhost:8787"},
			want: "ws://localhost:8787/v1/realtime",
		},
		{
			name: "trailing slash on server is normalized",
			cfg:  Config{Server: "https://sitrep.example.com/"},
			want: "wss://sitrep.example.com/v1/realtime",
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

// TestEventsURLFor pins the frozen v1 HTTP ingest endpoint
// (POST /v1/events, docs/design/v1-architecture.md section 4) the realtime
// Client's HTTP transport fallback targets.
func TestEventsURLFor(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "server with no trailing slash",
			cfg:  Config{Server: "https://sitrep.example.com"},
			want: "https://sitrep.example.com/v1/events",
		},
		{
			name: "trailing slash on server is normalized",
			cfg:  Config{Server: "https://sitrep.example.com/"},
			want: "https://sitrep.example.com/v1/events",
		},
		{
			name: "explicit EventsURL wins verbatim",
			cfg:  Config{Server: "https://sitrep.example.com", EventsURL: "https://other.example.com/custom"},
			want: "https://other.example.com/custom",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.cfg.EventsURLFor(); got != c.want {
				t.Fatalf("EventsURLFor() = %q, want %q", got, c.want)
			}
		})
	}
}
