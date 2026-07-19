package config

import "testing"

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
