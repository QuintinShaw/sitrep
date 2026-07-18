// APNs-from-Workers probe — the single most important technical validation
// for Sitrep: APNs requires HTTP/2 and there have been community reports of
// Workers' outbound fetch failing against api.push.apple.com. Run this BEFORE
// building anything on the Live Activity pipeline.
//
// Usage:
//   1. Create an APNs auth key (.p8) in the Apple Developer portal.
//   2. wrangler secret put APNS_KEY_P8      (paste the .p8 body, no header lines)
//      wrangler secret put APNS_KEY_ID
//      wrangler secret put APNS_TEAM_ID
//   3. wrangler dev --remote   (must be --remote: local mode won't reproduce
//      Workers' real egress behavior)
//   4. curl 'http://localhost:8787/?token=<device-push-token>&topic=<bundle-id>'
//
// Success = HTTP 200 from APNs (or 400 BadDeviceToken — which still proves
// the HTTP/2 connection works). Failure mode to watch for: fetch throwing or
// APNs resetting the connection.

interface Env {
  APNS_KEY_P8: string;
  APNS_KEY_ID: string;
  APNS_TEAM_ID: string;
}

function b64url(data: ArrayBuffer | string): string {
  const bytes = typeof data === "string" ? new TextEncoder().encode(data) : new Uint8Array(data);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function importP8(p8: string): Promise<CryptoKey> {
  const der = Uint8Array.from(atob(p8.replace(/\s/g, "")), (c) => c.charCodeAt(0));
  return crypto.subtle.importKey(
    "pkcs8",
    der,
    { name: "ECDSA", namedCurve: "P-256" },
    false,
    ["sign"],
  );
}

async function apnsJWT(env: Env): Promise<string> {
  const header = b64url(JSON.stringify({ alg: "ES256", kid: env.APNS_KEY_ID }));
  const claims = b64url(
    JSON.stringify({ iss: env.APNS_TEAM_ID, iat: Math.floor(Date.now() / 1000) }),
  );
  const key = await importP8(env.APNS_KEY_P8);
  const sig = await crypto.subtle.sign(
    { name: "ECDSA", hash: "SHA-256" },
    key,
    new TextEncoder().encode(`${header}.${claims}`),
  );
  return `${header}.${claims}.${b64url(sig)}`;
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);
    const token = url.searchParams.get("token");
    const topic = url.searchParams.get("topic");
    if (!token || !topic) {
      return Response.json({ usage: "?token=<device-push-token>&topic=<bundle-id>" }, { status: 400 });
    }

    let jwt: string;
    try {
      jwt = await apnsJWT(env);
    } catch (e) {
      return Response.json(
        { verdict: "JWT_SIGNING_FAILED_CHECK_SECRETS", error: String(e) },
        { status: 500 },
      );
    }
    const started = Date.now();
    try {
      const res = await fetch(`https://api.sandbox.push.apple.com/3/device/${token}`, {
        method: "POST",
        headers: {
          authorization: `bearer ${jwt}`,
          "apns-topic": topic,
          "apns-push-type": "alert",
          "content-type": "application/json",
        },
        body: JSON.stringify({ aps: { alert: "sitrep apns probe" } }),
      });
      return Response.json({
        verdict: res.ok ? "APNS_FROM_WORKERS_WORKS" : "APNS_REACHED_BUT_REJECTED",
        status: res.status,
        body: await res.text(),
        apns_id: res.headers.get("apns-id"),
        ms: Date.now() - started,
      });
    } catch (e) {
      return Response.json(
        {
          verdict: "FETCH_FAILED_LIKELY_H2_ISSUE",
          error: String(e),
          ms: Date.now() - started,
          fallback: "run APNs sending on a small relay (Fly.io/VPS); Workers keeps business logic",
        },
        { status: 502 },
      );
    }
  },
};
