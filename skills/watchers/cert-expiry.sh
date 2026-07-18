#!/bin/sh
# TLS certificate days-until-expiry. Needs: DOMAIN=example.com
# Register with `sitrep automation add`; set DOMAIN and CERT_WARN_DAYS in its environment.
DOMAIN=${DOMAIN:?set DOMAIN=example.com}
WARN=${CERT_WARN_DAYS:-14}

END=$(echo | openssl s_client -servername "$DOMAIN" -connect "$DOMAIN:443" 2>/dev/null \
  | openssl x509 -noout -enddate | cut -d= -f2)
[ -z "$END" ] && exit 0
DAYS=$(( ($(date -j -f "%b %d %T %Y %Z" "$END" +%s 2>/dev/null || date -d "$END" +%s) - $(date +%s)) / 86400 ))
echo "::sitrep metric.update --icon=lock.shield --tint=green --alert-below=$WARN cert_days $DAYS \"${DOMAIN} 证书剩余天\""
