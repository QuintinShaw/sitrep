# Claude Code hook adapter

Mirrors your Claude Code sessions onto Sitrep: session starts appear as tasks
on your Dynamic Island / menu bar, "waiting for input" shows as the current
step, and completion collapses the activity.

## Install

1. Make sure `~/.config/sitrep/config.json` exists (the macOS menu bar app
   and this hook share it), or export `SITREP_SERVER` / `SITREP_TOKEN`.
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
