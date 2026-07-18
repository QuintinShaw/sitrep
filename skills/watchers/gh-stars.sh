#!/bin/sh
# GitHub stars → menu bar / widget. Needs: REPO=owner/name
# Register with `sitrep automation add --name "gh stars" --executor script --every 15m -- ./gh-stars.sh`.
REPO=${REPO:?set REPO=owner/name}
STARS=$(curl -sf "https://api.github.com/repos/$REPO" | grep '"stargazers_count"' | tr -dc '0-9')
[ -n "$STARS" ] && echo "::sitrep metric.update --icon=star.fill --tint=orange --template=spark gh_stars $STARS \"GitHub ★\""
