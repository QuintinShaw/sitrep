package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/QuintinShaw/sitrep/daemon/internal/api"
	"github.com/QuintinShaw/sitrep/daemon/internal/config"
)

// joinRetrySchedule is a brief grace window `sitrep join` waits out on a 404
// before declaring an invite invalid. It is now generic network-retry
// defense only, NOT a correctness requirement: the self-routing connect code
// (P0-6, v1-architecture.md §10.5) means `POST /v1/join` routes on the
// request's own `space` field — resolved directly via
// env.SPACE_HUB.getByName(space), zero KV lookups — so there is no more
// eventually-consistent INVITE_DIR round-trip for a 404 to be racing
// against. Kept anyway (harmless) as ordinary retry-on-transient-error
// hygiene for a brief server-side hiccup right after invite creation.
var joinRetrySchedule = []time.Duration{800 * time.Millisecond, 1500 * time.Millisecond, 2500 * time.Millisecond}

// connectCodeAlphabet is the 31-symbol, confusable-free alphabet
// v1-architecture.md §10.5 shares with SpaceId minting (§10.1
// newSpaceId()): digits 2-9, then a-z excluding i/l/o. Canonical form is
// lowercase; a connect code's display/scan form is the same symbols
// uppercased.
const connectCodeAlphabet = "23456789abcdefghjkmnpqrstuvwxyz"

// connectCodeLen is the full self-routing connect code length (§10.5):
// 'X' + 10-char space_id + 9-char one-time secret + 'Z'.
const connectCodeLen = 21

// DecodeConnectCode decodes a 21-character self-routing connect code
// (v1-architecture.md §10.5: 'X' anchor + 10-char space_id + 9-char secret +
// 'Z' anchor, case-insensitive) into its embedded space_id and one-time
// secret, both lowercased to match the SpaceId token grammar (§10.1). This
// performs the same shape-only structural validation the server repeats
// defense-in-depth (wrong length, missing/wrong anchors, an out-of-alphabet
// character) — a malformed code is rejected locally before ever reaching
// the network. The algorithm must byte-for-byte match Apple's independent
// Swift implementation of the same contract section.
func DecodeConnectCode(code string) (spaceID, secret string, err error) {
	if len(code) != connectCodeLen {
		return "", "", fmt.Errorf("malformed code: want %d characters, got %d", connectCodeLen, len(code))
	}
	upper := strings.ToUpper(code)
	if upper[0] != 'X' || upper[connectCodeLen-1] != 'Z' {
		return "", "", fmt.Errorf("malformed code: missing X…Z anchors")
	}
	for i := 1; i < connectCodeLen-1; i++ {
		if !isConnectCodeChar(upper[i]) {
			return "", "", fmt.Errorf("malformed code: character %q at position %d is not in the connect-code alphabet", upper[i], i)
		}
	}
	lower := strings.ToLower(upper)
	return lower[1:11], lower[11 : connectCodeLen-1], nil
}

// isConnectCodeChar reports whether c (an uppercase ASCII byte) is one of
// the 31 connectCodeAlphabet symbols, case-insensitively.
func isConnectCodeChar(c byte) bool {
	if c >= 'A' && c <= 'Z' {
		c = c - 'A' + 'a'
	}
	return strings.IndexByte(connectCodeAlphabet, c) >= 0
}

// DefaultServer is the official cloud — hidden from users entirely
// (Tailscale-style); self-hosters override with --server / config.
const DefaultServer = "https://sitrep.quintinshaw.com"

// cmdSpaceCreate mints an anonymous space. The menu bar app calls this on
// first launch; nothing to sign up for.
func cmdSpaceCreate(args []string) {
	server := DefaultServer
	for len(args) > 0 {
		if args[0] == "--server" && len(args) > 1 {
			server = strings.TrimRight(args[1], "/")
			args = args[2:]
		} else {
			fatal(fmt.Errorf("unknown flag %q", args[0]))
		}
	}
	var out struct {
		SpaceID    string `json:"space_id"`
		DeviceID   string `json:"device_id"`
		OwnerToken string `json:"owner_token"`
	}
	if err := api.New(server, "").Do(http.MethodPost, "/v1/spaces", map[string]string{"platform": "macos"}, &out); err != nil {
		fatal(err)
	}
	// Persist the owner device_id: device_seq is scoped to (device_id, space),
	// so the creating Mac must know its own device_id to uplink events over
	// /v1/events (the owner token is a capability superset that can report —
	// v1-architecture.md §2.2, P0-1).
	if err := config.Save(config.Config{Server: server, Token: out.OwnerToken, DeviceID: out.DeviceID, Space: out.SpaceID}); err != nil {
		fatal(err)
	}
	fmt.Printf("space %s ready · credentials in %s\n", out.SpaceID, config.Path())
}

// cmdInvite mints an invite code for adding a device.
func cmdInvite(args []string) {
	role := "viewer"
	if len(args) > 0 && args[0] == "--role" && len(args) > 1 {
		role = args[1]
	}
	client, err := api.FromConfig()
	if err != nil {
		fatal(err)
	}
	var out struct {
		Code    string `json:"code"`
		SpaceID string `json:"space_id"`
	}
	if err := client.Do(http.MethodPost, "/v1/invites", map[string]string{"role": role}, &out); err != nil {
		fatal(err)
	}
	// out.Code is now the full self-routing 21-char layout (§10.5) — a
	// receiving `sitrep join` needs only --code; --space is shown here for
	// visibility/manual override, not because the code needs it decoded for
	// it (join decodes it locally).
	fmt.Printf("%s\nspace: %s · expires in 10 min\njoin from another machine:\n  sitrep join --server %s --code %s\n",
		out.Code, out.SpaceID, client.Server, out.Code)
}

// cmdJoin joins an existing space with an invite code (headless machines).
//
// --space is now OPTIONAL (P0-6, v1-architecture.md §10.5): a self-routing
// connect code embeds its own space_id, so the scan/paste path only needs
// --code — space is decoded from it locally, in Go, before ever contacting
// the server (DecodeConnectCode). --space stays available and, when given
// explicitly, is used as-is without requiring it to match the code — this
// is what the self-host deep-link path (sitrep://join?server&space&code)
// relies on, since it already carries space explicitly and unchanged
// (§10.5's "unifies both join paths" collapses to one required wire field,
// not one required CLI flag).
func cmdJoin(args []string) {
	server, space, code := DefaultServer, "", ""
	name, _ := os.Hostname()
	for len(args) > 0 {
		switch {
		case args[0] == "--server" && len(args) > 1:
			server = strings.TrimRight(args[1], "/")
			args = args[2:]
		case args[0] == "--space" && len(args) > 1:
			space = args[1]
			args = args[2:]
		case args[0] == "--code" && len(args) > 1:
			code = args[1]
			args = args[2:]
		case args[0] == "--name" && len(args) > 1:
			name = args[1]
			args = args[2:]
		default:
			fatal(fmt.Errorf("usage: sitrep join [--server url] [--space <id>] --code <invite>"))
		}
	}
	if code == "" {
		fatal(fmt.Errorf("usage: sitrep join [--server url] [--space <id>] --code <invite>"))
	}
	if space == "" {
		// Connect-code path: decode space_id locally rather than requiring
		// the caller to already know it (§10.5).
		decodedSpace, _, err := DecodeConnectCode(strings.ToUpper(code))
		if err != nil {
			fatal(fmt.Errorf("decode connect code: %w", err))
		}
		space = decodedSpace
	}
	platform := "linux"
	if _, err := os.Stat("/System/Library/CoreServices"); err == nil {
		platform = "macos"
	}
	var out struct {
		Token    string `json:"token"`
		DeviceID string `json:"device_id"`
		SpaceID  string `json:"space_id"`
	}
	// `space` (either explicitly passed or decoded from `code` above) is
	// always sent — required on every /v1/join call as of §10.5, unifying
	// the connect-code and self-host-deep-link paths. The server routes
	// directly on it (env.SPACE_HUB.getByName(space)), zero KV lookups; see
	// joinRetrySchedule's doc comment for why the retry-on-404 grace window
	// is now just generic network-retry hygiene, not a correctness need.
	body := map[string]string{"space": space, "code": strings.ToUpper(code), "name": name, "platform": platform}
	if err := joinWithGrace(api.New(server, ""), body, &out); err != nil {
		fatal(err)
	}
	// v1 /join returns the minted device_id; persist it so this device can
	// populate task.event/message.event body.device_id on the /v1/events
	// uplink (the server verifies it matches the authenticated identity,
	// v1-architecture.md §4).
	if err := config.Save(config.Config{Server: server, Token: out.Token, DeviceID: out.DeviceID, Space: out.SpaceID}); err != nil {
		fatal(err)
	}
	fmt.Printf("joined space %s · credentials in %s\n", out.SpaceID, config.Path())
}

// joinWithGrace POSTs /v1/join, retrying a 404 across joinRetrySchedule to
// ride out INVITE_DIR KV eventual consistency (a code minted moments ago may
// not have propagated / may be negatively cached). Any non-404 error, or a
// 404 that outlasts the grace window, is returned to fail the join.
func joinWithGrace(client *api.Client, body map[string]string, out any) error {
	var lastErr error
	for attempt := 0; ; attempt++ {
		lastErr = client.Do(http.MethodPost, "/v1/join", body, out)
		if lastErr == nil {
			return nil
		}
		var apiErr *api.APIError
		if !errors.As(lastErr, &apiErr) || apiErr.Status != http.StatusNotFound {
			return lastErr // not a KV-lag-shaped miss; fail immediately
		}
		if attempt >= len(joinRetrySchedule) {
			return lastErr // 404 persisted past the grace window — genuinely invalid/expired
		}
		time.Sleep(joinRetrySchedule[attempt])
	}
}
