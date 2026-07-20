// APNs delivery primitives for the v1 PushOutbox (docs/design/v1-apns-outbox.md).
//
// This module is deliberately *not* the thing that calls `fetch()` — it
// builds requests and classifies responses, both pure/testable without a
// Workers runtime. `SpaceHub`'s alarm handler owns the actual dispatch loop
// (bounded concurrency, row status transitions) and calls through its own
// injectable `apnsFetch` field so tests can stub the network boundary
// (see space-hub.ts). JWT signing (ES256, WebCrypto) is carried over
// unchanged from the pre-v1 implementation that validated this exact
// approach against a real APNs sandbox.

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

/** Apple requires a fresh-enough JWT every 20-60 minutes; cached ~50 min. */
export async function apnsJwt(cfg: ApnsConfig): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  if (jwtCache && jwtCache.keyId === cfg.keyId && now - jwtCache.iat < 3000) {
    return jwtCache.token;
  }
  const der = Uint8Array.from(atob(cfg.keyP8.replace(/\s/g, "")), (c) => c.charCodeAt(0));
  const key = await crypto.subtle.importKey("pkcs8", der, { name: "ECDSA", namedCurve: "P-256" }, false, ["sign"]);
  const header = b64url(JSON.stringify({ alg: "ES256", kid: cfg.keyId }));
  const claims = b64url(JSON.stringify({ iss: cfg.teamId, iat: now }));
  const sig = await crypto.subtle.sign({ name: "ECDSA", hash: "SHA-256" }, key, new TextEncoder().encode(`${header}.${claims}`));
  const token = `${header}.${claims}.${b64url(sig)}`;
  jwtCache = { token, iat: now, keyId: cfg.keyId };
  return token;
}

/** Test-only: clears the module-level JWT cache so a fresh cfg/keyId is
 * exercised (the cache is otherwise process-lifetime, which would leak
 * across independent test cases run in the same isolate). */
export function _resetApnsJwtCacheForTests(): void {
  jwtCache = null;
}

export type ApnsPushType = "liveactivity" | "alert";

/** Builds the APNs HTTP/2-over-fetch request for one push_outbox row's
 * dispatch attempt. Pure aside from the JWT signing await; never calls
 * `fetch` itself — see the module comment. */
export async function buildApnsRequest(
  cfg: ApnsConfig,
  deviceToken: string,
  opts: { pushType: ApnsPushType; priority: 5 | 10; aps: Record<string, unknown>; apnsId?: string },
): Promise<Request> {
  const topic = opts.pushType === "liveactivity" ? `${cfg.bundleId}.push-type.liveactivity` : cfg.bundleId;
  const headers: Record<string, string> = {
    authorization: `bearer ${await apnsJwt(cfg)}`,
    "apns-topic": topic,
    "apns-push-type": opts.pushType,
    "apns-priority": String(opts.priority),
    "content-type": "application/json",
  };
  if (opts.apnsId) headers["apns-id"] = opts.apnsId;
  return new Request(`https://${cfg.host}/3/device/${deviceToken}`, {
    method: "POST",
    headers,
    body: JSON.stringify({ aps: opts.aps }),
  });
}

/** APNs permanent-error reasons (docs/design/v1-apns-outbox.md §4.3): a
 * device token that will never succeed regardless of retry. Any of these
 * moves the row straight to permanent_failure and triggers token cleanup. */
const PERMANENT_TOKEN_REASONS = new Set(["BadDeviceToken", "Unregistered", "DeviceTokenNotForTopic"]);

export type ApnsOutcome =
  | { kind: "sent" }
  | { kind: "permanent"; reason: string; badToken: boolean }
  | { kind: "transient"; reason: string; retryAfterMs?: number };

/** Pure classification of one APNs HTTP response, so it's unit-testable
 * without constructing a real fetch Response. `status` is the HTTP status
 * code; `reason` is APNs's JSON body `{"reason": "..."}` field if present;
 * `retryAfterHeader` is the raw `Retry-After` header value if present
 * (seconds, per Apple's docs). */
export function classifyApnsResponse(status: number, reason: string | undefined, retryAfterHeader: string | null): ApnsOutcome {
  if (status >= 200 && status < 300) return { kind: "sent" };
  if (reason && PERMANENT_TOKEN_REASONS.has(reason)) {
    return { kind: "permanent", reason, badToken: true };
  }
  // HTTP 410 is APNs's dead-token status (`Unregistered`). Treat it as a
  // bad token even when the body is missing/unparseable so `reason` never
  // surfaced — otherwise a 410 with an empty body would fall through to the
  // generic-permanent branch below and leave the dead token in place
  // (v1-apns-outbox.md §4.3, fault-injection review).
  if (status === 410) {
    return { kind: "permanent", reason: reason ?? "Unregistered", badToken: true };
  }
  if (status === 429 || status >= 500) {
    const retryAfterSec = retryAfterHeader ? Number(retryAfterHeader) : undefined;
    return {
      kind: "transient",
      reason: reason ?? `http_${status}`,
      ...(Number.isFinite(retryAfterSec) && retryAfterSec !== undefined && retryAfterSec > 0
        ? { retryAfterMs: retryAfterSec * 1000 }
        : {}),
    };
  }
  // Any other non-2xx (e.g. BadTopic, BadPriority, PayloadTooLarge, an
  // unrecognized 4xx) is treated as permanent-but-not-a-bad-token: retrying
  // an identical malformed request forever is pointless (only 429/5xx/
  // network errors are transient per v1-apns-outbox.md §4.3), but the
  // token itself isn't known-bad, so no DeviceRegistry cleanup fires.
  return { kind: "permanent", reason: reason ?? `http_${status}`, badToken: false };
}

/** Exponential backoff with a cap and jitter (v1-apns-outbox.md §5):
 * min(2^attempts * 1000, 300_000) ms, +/-20% jitter to avoid synchronized
 * retry storms. `attempts` is the count BEFORE this retry (i.e. the value
 * about to be written as the new `attempts`). */
export function backoffMs(attempts: number, rand: () => number = Math.random): number {
  const base = Math.min(2 ** attempts * 1000, 300_000);
  const jitter = 1 + (rand() * 2 - 1) * 0.2; // uniform in [0.8, 1.2]
  return Math.round(base * jitter);
}

export const MAX_TRANSIENT_ATTEMPTS = 8;

// ---- per-kind aps payload builders ----
// `task` here is the folded row shape SpaceHub reads from its `tasks` table
// (task_id/device_id/title/state/percent/step/message/display-as-parsed-object).

export interface PushTaskView {
  task_id: string;
  title?: string;
  state: "running" | "done" | "failed";
  percent?: number;
  step?: string;
  message?: string;
  display?: { icon?: string; tint?: string; template?: string };
  started_at_epoch_s?: number | null;
}

function contentState(task: PushTaskView) {
  return { percent: task.percent ?? null, step: task.step ?? null, state: task.state };
}

const nowS = () => Math.floor(Date.now() / 1000);

/** Remote-create a Live Activity on a device (push-to-start, iOS 17.2+). */
export function pushToStartAps(task: PushTaskView): Record<string, unknown> {
  return {
    timestamp: nowS(),
    event: "start",
    "attributes-type": "TaskActivityAttributes",
    attributes: {
      taskId: task.task_id,
      title: task.title ?? task.task_id,
      icon: task.display?.icon ?? null,
      tint: task.display?.tint ?? null,
      template: task.display?.template ?? null,
      startedAtEpoch: task.started_at_epoch_s ?? null,
    },
    "content-state": contentState(task),
    alert: { title: task.title ?? task.task_id, body: "Task started" },
  };
}

/** Update a running activity via its per-activity token. `revision` is the
 * outbox row's monotonic revision, carried in content-state so a device can
 * defensively discard a stale update (v1-apns-outbox.md §4.2). */
export function activityUpdateAps(task: PushTaskView, revision: number): Record<string, unknown> {
  return { timestamp: nowS(), event: "update", "content-state": { ...contentState(task), revision } };
}

/** End the activity; keeps the final state on the lock screen briefly. */
export function activityEndAps(task: PushTaskView, revision: number): Record<string, unknown> {
  return {
    timestamp: nowS(),
    event: "end",
    "content-state": { ...contentState(task), revision },
    "dismissal-date": nowS() + 15 * 60,
  };
}

/** Regular notification for messages/task-completion/metric alerts.
 * warn/error break through Focus via time-sensitive interruption level
 * (entitlement declared by the app). */
export function alertAps(title: string, body: string, level: "info" | "warn" | "error"): Record<string, unknown> {
  return {
    alert: { title, body },
    sound: "default",
    "interruption-level": level === "info" ? "active" : "time-sensitive",
  };
}
