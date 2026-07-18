// APNs Live Activity sender. Workers-only (WebCrypto); the Node adapter does
// not push in v0. JWTs are cached ~50 minutes (Apple requires 20min–60min).

import type { TaskState } from "./store.ts";

export interface ApnsConfig {
  keyP8: string; // .p8 body, header/footer stripped
  keyId: string;
  teamId: string;
  bundleId: string; // e.g. dev.sitrep.app
  host: string; // api.sandbox.push.apple.com | api.push.apple.com
}

let jwtCache: { token: string; iat: number; keyId: string } | null = null;

function b64url(data: ArrayBuffer | string): string {
  const bytes = typeof data === "string" ? new TextEncoder().encode(data) : new Uint8Array(data);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function jwt(cfg: ApnsConfig): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  if (jwtCache && jwtCache.keyId === cfg.keyId && now - jwtCache.iat < 3000) {
    return jwtCache.token;
  }
  const der = Uint8Array.from(atob(cfg.keyP8.replace(/\s/g, "")), (c) => c.charCodeAt(0));
  const key = await crypto.subtle.importKey(
    "pkcs8", der, { name: "ECDSA", namedCurve: "P-256" }, false, ["sign"],
  );
  const header = b64url(JSON.stringify({ alg: "ES256", kid: cfg.keyId }));
  const claims = b64url(JSON.stringify({ iss: cfg.teamId, iat: now }));
  const sig = await crypto.subtle.sign(
    { name: "ECDSA", hash: "SHA-256" }, key, new TextEncoder().encode(`${header}.${claims}`),
  );
  const token = `${header}.${claims}.${b64url(sig)}`;
  jwtCache = { token, iat: now, keyId: cfg.keyId };
  return token;
}

async function send(
  cfg: ApnsConfig,
  deviceToken: string,
  priority: 5 | 10,
  aps: Record<string, unknown>,
): Promise<number> {
  const res = await fetch(`https://${cfg.host}/3/device/${deviceToken}`, {
    method: "POST",
    headers: {
      authorization: `bearer ${await jwt(cfg)}`,
      "apns-topic": `${cfg.bundleId}.push-type.liveactivity`,
      "apns-push-type": "liveactivity",
      "apns-priority": String(priority),
      "content-type": "application/json",
    },
    body: JSON.stringify({ aps }),
  });
  const body = await res.text();
  if (!res.ok) console.log(`apns ${res.status}: ${body}`);
  return res.status;
}

/** Regular notification for messages. warn/error break through Focus
 * via time-sensitive interruption level (entitlement declared by the app). */
export async function sendAlert(
  cfg: ApnsConfig,
  alertToken: string,
  text: string,
  level: "info" | "warn" | "error",
): Promise<number> {
  const res = await fetch(`https://${cfg.host}/3/device/${alertToken}`, {
    method: "POST",
    headers: {
      authorization: `bearer ${await jwt(cfg)}`,
      "apns-topic": cfg.bundleId,
      "apns-push-type": "alert",
      "apns-priority": "10",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      aps: {
        alert: { title: level === "info" ? "Sitrep" : `Sitrep ${level.toUpperCase()}`, body: text },
        sound: "default",
        "interruption-level": level === "info" ? "active" : "time-sensitive",
      },
    }),
  });
  const body = await res.text();
  if (!res.ok) console.log(`apns alert ${res.status}: ${body}`);
  return res.status;
}

function contentState(task: TaskState) {
  return { percent: task.percent ?? null, step: task.step ?? null, status: task.status };
}

const nowS = () => Math.floor(Date.now() / 1000);

/** Remote-create a Live Activity on a device (push-to-start, iOS 17.2+). */
export function startActivity(cfg: ApnsConfig, pushToStartToken: string, task: TaskState) {
  return send(cfg, pushToStartToken, 10, {
    timestamp: nowS(),
    event: "start",
    "attributes-type": "TaskActivityAttributes",
    attributes: {
      sourceId: task.source_id,
      title: task.title,
      icon: task.icon ?? null,
      tint: task.tint ?? null,
      template: task.template ?? null,
      startedAtEpoch: task.started_at ? Math.floor(Date.parse(task.started_at) / 1000) : null,
    },
    "content-state": contentState(task),
    alert: {
      title: task.title,
      body: "Task started",
    },
  });
}

/** Update a running activity via its per-activity token. */
export function updateActivity(cfg: ApnsConfig, activityToken: string, task: TaskState) {
  return send(cfg, activityToken, 5, {
    timestamp: nowS(),
    event: "update",
    "content-state": contentState(task),
  });
}

/** End the activity; keeps the final state on the lock screen briefly. */
export function endActivity(cfg: ApnsConfig, activityToken: string, task: TaskState) {
  return send(cfg, activityToken, 10, {
    timestamp: nowS(),
    event: "end",
    "content-state": contentState(task),
    "dismissal-date": nowS() + 15 * 60,
  });
}
