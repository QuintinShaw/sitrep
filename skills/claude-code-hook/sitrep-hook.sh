#!/bin/sh
# Claude Code → Sitrep adapter. Wire into .claude/settings.json hooks; each
# invocation reads the hook payload from stdin and reports session state as a
# Sitrep task, so your phone's Dynamic Island shows what Claude is doing.
#
# Usage: sitrep-hook.sh <start|working|waiting|stop>
#
# It reports THROUGH the unified `sitrep` CLI (`sitrep report`), which owns
# device_seq allocation, the durable outbox, and the health signal — never by
# hand-rolling raw HTTP or reimplementing device_seq in shell (the old
# /v2/ingest path, now deleted). Credentials and the server come from the
# CLI's own config (~/.config/sitrep/config.json or SITREP_SERVER/SITREP_TOKEN),
# so this script needs none.
#
# The `sitrep` binary must be on PATH (or point SITREP_BIN at it). If it is
# missing or not configured, this script exits 0 without blocking Claude Code.

set -eu

PHASE="${1:-working}"
SITREP="${SITREP_BIN:-sitrep}"

# Never block Claude Code: if the CLI isn't installed, do nothing.
command -v "$SITREP" >/dev/null 2>&1 || exit 0

PAYLOAD=$(cat)
SESSION=$(printf '%s' "$PAYLOAD" | python3 -c "import json,sys;print(json.load(sys.stdin).get('session_id','unknown')[:12])" 2>/dev/null || echo unknown)
CWD_NAME=$(printf '%s' "$PAYLOAD" | python3 -c "import json,sys,os;print(os.path.basename(json.load(sys.stdin).get('cwd','')) or 'claude')" 2>/dev/null || echo claude)

TASK="cc-$SESSION"

# `sitrep report` reads its own config and is a no-op when unconfigured, so a
# failure here (unconfigured, offline, whatever) must never fail the hook.
case "$PHASE" in
  start)
    "$SITREP" report --task "$TASK" --kind started --title "Claude Code · $CWD_NAME" >/dev/null 2>&1 || true ;;
  waiting)
    "$SITREP" report --task "$TASK" --kind step --step "⏸ waiting for your input" >/dev/null 2>&1 || true ;;
  stop)
    "$SITREP" report --task "$TASK" --kind done --text "session finished" >/dev/null 2>&1 || true ;;
  working|*)
    "$SITREP" report --task "$TASK" --kind step --step "working…" >/dev/null 2>&1 || true ;;
esac

exit 0
