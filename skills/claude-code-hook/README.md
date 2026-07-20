# Claude Code hook adapter

Mirrors your Claude Code sessions onto Sitrep: session starts appear as tasks
on your Dynamic Island / menu bar, "waiting for input" shows as the current
step, and completion collapses the activity.

The hook reports through the unified `sitrep` CLI (`sitrep report`), which
owns device_seq allocation, the durable outbox, and the health signal — it
does not talk to the server directly.

## Install

1. Install the `sitrep` CLI so it is on your `PATH` (or point `SITREP_BIN` at
   the binary). It must be paired: run `sitrep space create` (first Mac) or
   `sitrep join …` (another machine) so `~/.config/sitrep/config.json` holds
   the server, token, and this device's `device_id`.
2. Copy `sitrep-hook.sh` somewhere stable and `chmod +x` it.
3. Add to `~/.claude/settings.json`:

```json
{
  "hooks": {
    "SessionStart": [
      { "hooks": [{ "type": "command", "command": "~/.claude/hooks/sitrep-hook.sh start" }] }
    ],
    "Notification": [
      { "hooks": [{ "type": "command", "command": "~/.claude/hooks/sitrep-hook.sh waiting" }] }
    ],
    "Stop": [
      { "hooks": [{ "type": "command", "command": "~/.claude/hooks/sitrep-hook.sh stop" }] }
    ]
  }
}
```

The hook is fail-open: no config or no network → it exits 0 silently and
never blocks Claude Code.
