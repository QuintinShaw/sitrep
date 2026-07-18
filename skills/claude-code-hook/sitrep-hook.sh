#!/bin/sh
# Claude Code → Sitrep adapter. Wire into .claude/settings.json hooks; each
# invocation reads the hook payload from stdin and reports session state as a
# Sitrep task, so your phone's Dynamic Island shows what Claude is doing.
#
# Usage: sitrep-hook.sh <start|working|waiting|stop>
#
# Credentials: SITREP_SERVER / SITREP_TOKEN env vars, falling back to
# ~/.config/sitrep/config.json ({"server": "...", "token": "..."}).

set -eu

PHASE="${1:-working}"
CONFIG="$HOME/.config/sitrep/config.json"

SERVER="${SITREP_SERVER:-}"
TOKEN="${SITREP_TOKEN:-}"
if [ -z "$SERVER" ] && [ -f "$CONFIG" ]; then
  SERVER=$(python3 -c "import json;print(json.load(open('$CONFIG')).get('server',''))" 2>/dev/null || true)
  TOKEN=$(python3 -c "import json;print(json.load(open('$CONFIG')).get('token',''))" 2>/dev/null || true)
fi
[ -z "$SERVER" ] && exit 0 # not configured; never block Claude Code

PAYLOAD=$(cat)
SESSION=$(printf '%s' "$PAYLOAD" | python3 -c "import json,sys;print(json.load(sys.stdin).get('session_id','unknown')[:12])" 2>/dev/null || echo unknown)
CWD_NAME=$(printf '%s' "$PAYLOAD" | python3 -c "import json,sys;import os;print(os.path.basename(json.load(sys.stdin).get('cwd','')) or 'claude')" 2>/dev/null || echo claude)
TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

case "$PHASE" in
  start)
    EV="{\"kind\":\"task.start\",\"title\":\"Claude Code · $CWD_NAME\"}" ;;
  waiting)
    EV="{\"kind\":\"task.step\",\"step\":\"⏸ waiting for your input\"}" ;;
  stop)
    EV="{\"kind\":\"task.done\",\"text\":\"session finished\"}" ;;
  working|*)
    EV="{\"kind\":\"task.step\",\"step\":\"working…\"}" ;;
esac

BODY=$(printf '%s' "$EV" | python3 -c "
import json,sys
ev=json.load(sys.stdin)
ev.update({'source_id':'cc-$SESSION','ts':'$TS'})
print(json.dumps([ev]))")

curl -s -m 5 -X POST "$SERVER/v2/ingest" \
  -H "content-type: application/json" \
  ${TOKEN:+-H "Authorization: Bearer $TOKEN"} \
  -d "$BODY" > /dev/null 2>&1 || true
exit 0
