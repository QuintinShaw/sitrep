#!/bin/sh
set -eu

alert_above="${1:-}"
if [ -z "$alert_above" ]; then
  alert_hint=""
elif ! /usr/bin/awk -v number="$alert_above" \
  'BEGIN { exit !(number ~ /^[0-9]+([.][0-9]+)?$/) }'; then
    printf 'usage: %s [alert-above-number]\n' "$0" >&2
    exit 2
else
  alert_hint="--alert-above=$alert_above "
fi

cpu_usage=$(
  /usr/bin/top -l 2 -n 0 -s 1 |
    /usr/bin/awk '
      /CPU usage/ {
        idle = $7
        gsub("%", "", idle)
        value = 100 - idle
      }
      END {
        if (value == "") exit 1
        printf "%.1f\n", value
      }
    '
)

printf '%s\n' "::sitrep metric.update --icon=cpu --tint=blue --template=gauge --min=0 --max=100 ${alert_hint}system.cpu ${cpu_usage} CPU 使用率"
