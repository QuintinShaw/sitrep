# Self-hosting Sitrep

The server is one Hono codebase with two deployment paths. Pick one:

## Path A: your own Cloudflare account (recommended)

Free tier is plenty for personal use; Durable Objects require the (also free)
Workers paid-plan toggle in some regions — check your dash.

```bash
git clone https://github.com/QuintinShaw/sitrep && cd sitrep/server
npm ci
npx wrangler login
npx wrangler deploy                        # prints your workers.dev URL
```

There is **no server-side shared secret to set** (v1 removed the legacy
`AUTH_TOKEN`; see `docs/design/v1-architecture.md` §10.4). Access is by
per-device `sr1_` tokens: on first launch the menu bar app calls
`POST /v1/spaces`, which creates your space and returns its owner device
token; the app stores that token and every other device joins as a real
`viewer`/`source` through the invite/QR flow. Nothing to provision by hand.

Optional but recommended: bind a custom domain (Workers → your worker →
Domains) — `workers.dev` is unreachable from some networks (notably mainland
China without a proxy).

## Path B: Docker / Node

```bash
cd server && npm ci
PORT=8787 npm run dev:node
```

The Node path serves the same `/v1` API; create your space with
`POST /v1/spaces` (the menu bar app does this for you on first launch) and
use the returned owner token as `SITREP_TOKEN` below. There is no
server-side shared secret. Pushes are not available on this path yet — see
below.

## Client configuration

- **daemon / CLI**: `SITREP_SERVER=https://your.domain SITREP_TOKEN=...`
- **macOS menu bar & Claude Code hook**: `~/.config/sitrep/config.json`
  `{"server": "https://your.domain", "token": "..."}`
- **iOS app**: Settings (⚙️) in the app.

## Push notifications & Live Activities (the APNs caveat)

APNs credentials belong to the app's signer. If you build and sign the iOS
app yourself (your own bundle id + APNs key), set these on your worker and
everything—including Dynamic Island push-to-start—runs entirely on your
infrastructure:

```bash
npx wrangler secret put APNS_KEY_P8     # .p8 body, header/footer stripped
npx wrangler secret put APNS_KEY_ID
npx wrangler secret put APNS_TEAM_ID
# wrangler.jsonc vars: APNS_BUNDLE_ID, APNS_HOST (sandbox vs production)
```

If you install the official App Store build (planned), your self-hosted
server will relay Live Activity pushes through the official cloud (which
holds the App Store APNs certificate). Task and metric data stays on your
server; only push payloads transit the relay.
