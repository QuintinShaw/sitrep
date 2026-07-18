#!/bin/sh
# Cloudflare Workers requests today → widget. Needs: CF_API_TOKEN, CF_ACCOUNT_ID
# (credentials stay on THIS machine; only the number is uploaded)
# Register with `sitrep automation add --name "cf usage" --executor script --every 30m -- ./cloudflare-usage.sh`.
: "${CF_API_TOKEN:?}" "${CF_ACCOUNT_ID:?}"
REQS=$(curl -sf "https://api.cloudflare.com/client/v4/graphql" \
  -H "Authorization: Bearer $CF_API_TOKEN" -H "Content-Type: application/json" \
  --data "{\"query\":\"{viewer{accounts(filter:{accountTag:\\\"$CF_ACCOUNT_ID\\\"}){workersInvocationsAdaptive(limit:1,filter:{date:\\\"$(date +%Y-%m-%d)\\\"}){sum{requests}}}}}\"}" \
  | grep -o '"requests":[0-9]*' | tr -dc '0-9')
[ -n "$REQS" ] && echo "::sitrep metric.update --icon=cloud.fill --tint=orange --template=spark cf_requests $REQS \"CF 今日请求\""
