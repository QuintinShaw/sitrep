package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/QuintinShaw/sitrep/daemon/internal/api"
	"github.com/QuintinShaw/sitrep/daemon/internal/config"
)

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
		OwnerToken string `json:"owner_token"`
	}
	if err := api.New(server, "").Do(http.MethodPost, "/v2/spaces", map[string]string{"platform": "macos"}, &out); err != nil {
		fatal(err)
	}
	if err := config.Save(config.Config{Server: server, Token: out.OwnerToken, Space: out.SpaceID}); err != nil {
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
	if err := client.Do(http.MethodPost, "/v2/invites", map[string]string{"role": role}, &out); err != nil {
		fatal(err)
	}
	fmt.Printf("%s\nspace: %s · expires in 10 min\njoin from another machine:\n  sitrep join --server %s --space %s --code %s\n",
		out.Code, out.SpaceID, client.Server, out.SpaceID, out.Code)
}

// cmdJoin joins an existing space with an invite code (headless machines).
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
			fatal(fmt.Errorf("usage: sitrep join [--server url] --space <id> --code <invite>"))
		}
	}
	if space == "" || code == "" {
		fatal(fmt.Errorf("usage: sitrep join [--server url] --space <id> --code <invite>"))
	}
	platform := "linux"
	if _, err := os.Stat("/System/Library/CoreServices"); err == nil {
		platform = "macos"
	}
	var out struct {
		Token   string `json:"token"`
		SpaceID string `json:"space_id"`
	}
	if err := api.New(server, "").Do(http.MethodPost, "/v2/join", map[string]string{
		"space": space, "code": strings.ToUpper(code), "name": name, "platform": platform,
	}, &out); err != nil {
		fatal(err)
	}
	if err := config.Save(config.Config{Server: server, Token: out.Token, Space: out.SpaceID}); err != nil {
		fatal(err)
	}
	fmt.Printf("joined space %s · credentials in %s\n", out.SpaceID, config.Path())
}
