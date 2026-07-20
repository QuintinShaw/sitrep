# Troubleshooting

Hard-won answers, mostly from building this. Symptoms first.

## The Dynamic Island doesn't appear when a task starts

1. **Push-to-start budget exhausted** (the big one). iOS gives each app a
   device-level budget of remote Live Activity starts (~10/hour). Exceed it
   and pushes are silently dropped — APNs still returns 200. Confirm via
   device syslog (`idevicesyslog | grep liveactivitiesd`):
   `Push-to-start budget exceeded for <bundle>; not starting activity`.
   The budget re-evaluates roughly hourly and replenishes when the user
   interacts with the app. Don't spam test starts; reuse activities.
2. **Stale push-to-start token.** Tokens can rotate after reinstall or
   reboot, and `pushToStartTokenUpdates` does NOT replay the current value
   on a fresh subscription — read `Activity.pushToStartToken` explicitly at
   launch (the app does this) and open the app once after reinstall/reboot.
3. **Low Power Mode** suppresses push-to-start.
4. Check Settings → Sitrep → Live Activities is on.

## The island appears but progress never moves

1. **The app was force-quit.** iOS refuses background launches for
   force-quit apps, so the per-activity update token never reaches the
   server. Symptom on the server: `apns update ...: no-token`. Don't swipe-
   kill the app (same rule as any push-dependent app). The server pushes a
   catch-up the moment the token does register.
2. **Duplicate activities, only one updating**: fixed in current server
   (registrations dedupe by token value), but if you see twin cards from an
   old server, update it and swipe the zombie away.

## Nothing reaches the phone at all

- `workers.dev` domains are blocked on some networks (mainland China without
  a proxy) — the app shows "request timed out". Bind a custom domain.
- Cloudflare deploys + secrets take ~10–30s to propagate; first requests
  after a deploy can 500/1104/1042 transiently.

## Diagnostics toolbox

- Server side: `npx wrangler tail --format pretty` — the DO logs every APNs
  push with its status (`apns update <id> p=45: 200`).
- Registered devices: `GET /v1/devices` (any `viewer`/`owner` device token).
  The legacy `GET /debug/tokens` / `AUTH_TOKEN` admin endpoint is removed in
  v1 (`docs/design/v1-architecture.md` §10.4).
- Device side: `brew install libimobiledevice && idevicesyslog | grep -iE
  "liveactivitiesd|apsd"` — shows budget decisions and push receipt.
- Manual APNs push from a Mac: sign the ES256 JWT with node (`dsaEncoding:
  "ieee-p1363"`), send with `curl --http2` (node's fetch is HTTP/1.1-only
  and APNs requires HTTP/2).

## Live Activity platform limits (design constraints, not bugs)

- 8h of active updates per activity, then it must be re-issued; update
  tokens rotate ~8h (the server re-registers on every rotation).
- High-frequency updates need `NSSupportsLiveActivitiesFrequentUpdates` and
  are still budgeted; the daemon coalesces to 1/s and the server throttles.
