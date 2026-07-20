// Sitrep v1 HTTP API (docs/design/v1-architecture.md). Every route below
// resolves exactly one SpaceHub Durable Object per space and touches no
// other store — see the §12 consistency checklist. There is no `/v2`, no
// `/v3`, no `admin` role, no bare-secret `AUTH_TOKEN` path: auth resolves
// ONLY `sr1_` bearer tokens through SpaceHub's DeviceRegistry to a real
// device_id + role ∈ {owner, viewer, source} (§10.4).

import { Hono } from "hono";
import type { Context, Next } from "hono";
import { authorizeClientEnvelope, parseEnvelope } from "./realtime/guards.ts";
import { SUPPORTED_PROTOCOL_VERSIONS } from "./realtime/protocol.ts";
import type { SpaceHub } from "./realtime/space-hub.ts";
import type { AnyEnvelope, AutomationState, CommandBody, MessageEventBody, MetricFrameBody, TaskEventBody } from "./realtime/types.ts";
import {
  assertOwnerIsSuperset,
  COMMAND_TTL_MAX_MS,
  COMMAND_TTL_MIN_MS,
  DEFAULT_COMMAND_TTL_MS,
  EVENTS_BATCH_MAX,
  isRouteAllowed,
  newToken,
  SERIES_RANGES,
  TOKEN_RE,
  type AckedPair,
  type Capabilities,
  type DeviceRole,
  type EventEnvelopeType,
  type EventResult,
  type SeriesRange,
  type V1Route,
} from "./v1/contract/types.ts";

export interface AppOptions {
  spaceHub: (c: Context, spaceId: string) => DurableObjectStub<SpaceHub>;
  /** Space creation guard (official cloud may rate-limit); default allows. */
  allowSpaceCreation?: (c: Context) => boolean;
  /** Bounded abuse-prevention rate limit on top of allowSpaceCreation
   * (pre-launch fix): SPACE_CREATION_ENABLED alone is a deploy-level on/off
   * with no per-caller bound. Returns false when the caller is over the
   * bound (429). Default allows (no limiter wired) so tests/adapters that
   * don't care about this still work. */
  checkSpaceCreationRateLimit?: (c: Context) => Promise<boolean>;
  /** Kill switches (v1-architecture.md §8) — computed by the adapter from
   * its own env shape so this module stays decoupled from a concrete Env. */
  wsTransportEnabled: (c: Context) => boolean;
  apnsDeliveryEnabled: (c: Context) => boolean;
}

type Vars = { Variables: { role: DeviceRole; space: string; deviceId: string } };

function newSpaceId(): string {
  const alphabet = "abcdefghjkmnpqrstuvwxyz23456789";
  return [...crypto.getRandomValues(new Uint8Array(10))].map((b) => alphabet[b % alphabet.length]).join("");
}

/** SHA-256 of the Idempotency-Key, hex, truncated to 32 chars
 * (v1-architecture.md §4.2) — deterministic automation_id when a create
 * omits one, so an honest retry (same key, still no explicit id)
 * reconstructs the identical payload instead of minting a second
 * automation or spuriously 409ing against its own retry. */
async function deriveAutomationId(idempotencyKey: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(idempotencyKey));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("").slice(0, 32);
}

function parseAutomationCreate(body: unknown): { name: string; executor_kind: "script" | "agent" | "hybrid"; every_seconds: number } | null {
  const b = body as Record<string, unknown> | null;
  const kind = b?.executor_kind;
  const everySeconds = Number((b?.schedule as Record<string, unknown> | undefined)?.every_seconds);
  if (!b?.name || typeof b.name !== "string" || !["script", "agent", "hybrid"].includes(kind as string) || !Number.isFinite(everySeconds) || everySeconds < 1) {
    return null;
  }
  return { name: b.name.slice(0, 256), executor_kind: kind as "script" | "agent" | "hybrid", every_seconds: Math.max(1, Math.floor(everySeconds)) };
}

function mintResultResponse(c: Context, result: { revision: number; automation: AutomationState | null; conflict?: boolean }) {
  if (result.conflict) return c.json({ error: "idempotency key was already used for a different operation" }, 409);
  return c.json({ revision: result.revision, automation: result.automation });
}

function parseTtlMs(body: Record<string, unknown> | null): { ok: true; ttlMs: number } | { ok: false } {
  if (!body || body.ttl_ms === undefined) return { ok: true, ttlMs: DEFAULT_COMMAND_TTL_MS };
  const ttl = body.ttl_ms;
  if (!Number.isInteger(ttl) || (ttl as number) < COMMAND_TTL_MIN_MS || (ttl as number) > COMMAND_TTL_MAX_MS) return { ok: false };
  return { ok: true, ttlMs: ttl as number };
}

const EVENT_TYPES: readonly EventEnvelopeType[] = ["task.event", "message.event", "metric.frame"];

export function createApp(opts: AppOptions) {
  // Fail fast at boot if a ROUTE_ROLES edit ever narrowed owner below the
  // source/viewer union (P0-1): owner must be a strict capability superset.
  assertOwnerIsSuperset();

  const app = new Hono<Vars>();

  app.get("/healthz", (c) => c.json({ ok: true, service: "sitrep" }));

  // ---- unauthenticated control plane (v1-architecture.md §2.2) ----

  app.post("/v1/spaces", async (c) => {
    if (opts.allowSpaceCreation && !opts.allowSpaceCreation(c)) {
      return c.json({ error: "space creation disabled" }, 403);
    }
    // Pre-launch: a real bounded rate limit on top of the deploy-level
    // SPACE_CREATION_ENABLED flag, which by itself provides zero abuse
    // prevention once true (the shipped default). See adapters/workers.ts
    // for the mechanism (a tiny dedicated counting Durable Object) and its
    // bound.
    if (opts.checkSpaceCreationRateLimit && !(await opts.checkSpaceCreationRateLimit(c))) {
      return c.json({ error: "too many space creations, try again later" }, 429);
    }
    const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const platform = String(body?.platform || "macos").slice(0, 20);
    const name = String(body?.name || "此电脑").slice(0, 60);
    const spaceId = newSpaceId();
    const ownerToken = newToken(spaceId);
    const deviceId = await opts.spaceHub(c, spaceId).initSpace(spaceId, ownerToken, platform, name);
    if (!deviceId) return c.json({ error: "try again" }, 503);
    // Returns device_id (P0-1): device_seq is scoped to (device_id, space),
    // so the creating owner device must know its own id to uplink events.
    return c.json({ space_id: spaceId, device_id: deviceId, owner_token: ownerToken });
  });

  app.post("/v1/join", async (c) => {
    // P0-6 self-routing (v1-architecture.md §10.5): `space` is REQUIRED —
    // route directly to that space's SpaceHub with ZERO KV lookup. The
    // connect-code path derives it by decoding `code` locally; the
    // self-host deep-link path already carries it explicitly. Both send the
    // same {code, space} shape.
    const body = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
    const code = String(body?.code || "");
    const spaceId = String(body?.space || "");
    if (!spaceId) return c.json({ error: "space required" }, 400);
    const name = String(body?.name || "unnamed device").slice(0, 60);
    const platform = String(body?.platform || "unknown").slice(0, 20);
    const token = newToken(spaceId);
    const result = await opts.spaceHub(c, spaceId).join(code, name, platform, token);
    if (!result.ok) return c.json({ error: result.error }, result.status);
    return c.json({ token, device_id: result.deviceId, role: result.role, space_id: spaceId });
  });

  // ---- authenticate: resolve role/space/device_id from the sr1 bearer token ----
  // No admin role, no AUTH_TOKEN fallback, no open-dev bypass
  // (v1-architecture.md §3, §10.4) — a bare/malformed/unresolvable token is
  // always 401, full stop.

  const authenticate = async (c: Context<Vars>, next: Next) => {
    const got = c.req.header("authorization")?.replace(/^Bearer\s+/i, "");
    if (!got) return c.json({ error: "unauthorized" }, 401);
    const m = TOKEN_RE.exec(got);
    if (!m) return c.json({ error: "unauthorized" }, 401);
    const spaceId = m[1];
    const resolved = await opts.spaceHub(c, spaceId).resolveToken(got);
    if (!resolved) return c.json({ error: "unauthorized" }, 401);
    c.set("role", resolved.role);
    c.set("space", spaceId);
    c.set("deviceId", resolved.deviceId);
    await next();
  };
  app.use("/v1/*", async (c, next) => {
    if (c.req.path === "/v1/spaces" || c.req.path === "/v1/join") return next();
    return authenticate(c, next);
  });

  const stub = (c: Context<Vars>) => opts.spaceHub(c, c.get("space"));

  /** Role-matrix gate (v1-architecture.md §3), driven by the SAME
   * ROUTE_ROLES table `v1/contract/types.ts` freezes — a route's allowed
   * roles are never hand-duplicated here. */
  function requireRole(route: V1Route) {
    return async (c: Context<Vars>, next: Next) => {
      if (!isRouteAllowed(route, c.get("role"))) return c.json({ error: "forbidden" }, 403);
      await next();
    };
  }

  // ---- control plane (authenticated) ----

  app.post("/v1/invites", requireRole("POST /v1/invites"), async (c) => {
    const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const role = body?.role === "source" ? "source" : "viewer";
    // P0-6 (v1-architecture.md §10.5): the minted code is fully self-routing
    // (space_id embedded directly) — no KV publish step needed.
    const invite = await stub(c).createInvite(role);
    return c.json({ ...invite, space_id: c.get("space") });
  });

  app.get("/v1/devices", requireRole("GET /v1/devices"), async (c) => c.json(await stub(c).devices()));

  app.delete("/v1/devices/:id", requireRole("DELETE /v1/devices/:id"), async (c) => {
    await stub(c).revokeDevice(c.req.param("id")!);
    return c.json({ ok: true });
  });

  app.put("/v1/devices/self/push-tokens", requireRole("PUT /v1/devices/self/push-tokens"), async (c) => {
    const body = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
    const pushToStartToken = typeof body?.push_to_start_token === "string" ? body.push_to_start_token : undefined;
    const alertToken = typeof body?.alert_token === "string" ? body.alert_token : undefined;
    if (!pushToStartToken && !alertToken) return c.json({ error: "push_to_start_token or alert_token required" }, 400);
    await stub(c).registerPushTokens(c.get("deviceId"), { push_to_start_token: pushToStartToken, alert_token: alertToken });
    return c.json({ ok: true });
  });

  app.put("/v1/tasks/:id/live-activity-token", requireRole("PUT /v1/tasks/:id/live-activity-token"), async (c) => {
    const body = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
    if (typeof body?.token !== "string" || !body.token) return c.json({ error: "token required" }, 400);
    await stub(c).registerActivityToken(c.req.param("id")!, c.get("deviceId"), body.token);
    return c.json({ ok: true });
  });

  // ---- state plane: realtime WS upgrade ----

  app.get("/v1/realtime", async (c) => {
    const raw = c.req.raw as Request;
    if ((raw.headers.get("upgrade") ?? "").toLowerCase() !== "websocket") {
      return c.text("expected websocket upgrade", 426);
    }
    // Kill switch checked ONLY here (v1-architecture.md §8.1) — never on
    // any other /v1 route. 503, never 403: this is a transport/availability
    // fact, not an authorization failure.
    if (!opts.wsTransportEnabled(c)) {
      return c.json({ error: "transport_unavailable" }, 503);
    }
    const deviceId = c.get("deviceId");
    // Forward the token's HTTP role (owner/viewer/source), NOT a pre-mapped WS
    // role. The DO constrains the client-declared hello role against this
    // (P0-1): source→source, viewer→viewer, owner→either.
    const headers = new Headers(raw.headers);
    headers.set("x-sitrep-device-id", deviceId);
    headers.set("x-sitrep-role", c.get("role"));
    const forwardReq = new Request(raw.url, { method: raw.method, headers });
    return stub(c).fetch(forwardReq);
  });

  // ---- state plane: shared ingest (v1-architecture.md §4) ----

  app.post("/v1/events", requireRole("POST /v1/events"), async (c) => {
    const raw = await c.req.json().catch(() => null);
    if (!raw || typeof raw !== "object" || !Array.isArray((raw as Record<string, unknown>).events)) {
      return c.json({ error: "malformed" }, 400);
    }
    const events = (raw as { events: unknown[] }).events;
    if (events.length > EVENTS_BATCH_MAX) return c.json({ error: "malformed" }, 400);
    // Optional task partitioning for a multi-process source (v1-architecture.md
    // §4.1): scopes the response commands[] to this uplink's own task. Does
    // not affect event application.
    const rawForTaskId = (raw as Record<string, unknown>).for_task_id;
    const forTaskId = typeof rawForTaskId === "string" ? rawForTaskId : undefined;
    // P0-5 fetch-then-ack (v1-architecture.md §1.4, §4.1): command_ids this
    // device has durably handed off to its local process controller since it
    // last acked. Non-string entries are dropped rather than rejecting the
    // whole request — acking is best-effort and idempotent by design.
    const rawAckCommandIds = (raw as Record<string, unknown>).ack_command_ids;
    const ackCommandIds = Array.isArray(rawAckCommandIds) ? rawAckCommandIds.filter((id): id is string => typeof id === "string") : undefined;

    const authenticatedDeviceId = c.get("deviceId");
    const s = stub(c);
    const acked: AckedPair[] = [];
    const results: EventResult[] = [];
    let lastRevision: number | undefined;

    for (let index = 0; index < events.length; index++) {
      const item = events[index];
      const parsed = parseEnvelope(JSON.stringify(item));
      const rawType = typeof (item as Record<string, unknown> | null)?.type === "string" ? ((item as Record<string, unknown>).type as string) : "unknown";

      if (parsed.kind === "unknown_type" || parsed.kind === "error") {
        results.push({ index, type: rawType as EventEnvelopeType, status: "rejected", error: { code: parsed.kind === "error" ? parsed.code : "malformed", message: parsed.kind === "error" ? parsed.message : "unrecognized envelope type" } });
        continue;
      }
      const envelope: AnyEnvelope = parsed.envelope;

      // Mirrors dispatchMessage's own structure: a hello frame is a
      // handshake violation once a connection (here: a credentialed HTTP
      // caller) is already past authentication — checked before the
      // general role-based authorization matrix, exactly as the WS path
      // checks it before authorizeClientEnvelope.
      if (envelope.type === "hello") {
        results.push({ index, type: rawType as EventEnvelopeType, status: "rejected", error: { code: "hello_required", message: "hello is not valid on POST /v1/events" } });
        continue;
      }

      const authz = authorizeClientEnvelope("source", envelope);
      if (!authz.ok || !EVENT_TYPES.includes(envelope.type as EventEnvelopeType)) {
        results.push({
          index,
          type: rawType as EventEnvelopeType,
          status: "rejected",
          error: { code: authz.ok ? "malformed" : authz.code, message: authz.ok ? `type ${envelope.type} is not accepted on POST /v1/events` : `role source may not send ${envelope.type}` },
        });
        continue;
      }

      const bodyDeviceId = (envelope.body as { device_id?: string }).device_id;
      if (bodyDeviceId !== authenticatedDeviceId) {
        results.push({ index, type: envelope.type as EventEnvelopeType, status: "rejected", error: { code: "unauthorized", message: "device_id does not match the authenticated identity" } });
        continue;
      }

      if (envelope.type === "task.event") {
        const body = envelope.body as TaskEventBody;
        const { revision, duplicate } = await s.applyTaskEvent(body);
        lastRevision = revision;
        acked.push({ device_id: body.device_id, device_seq: body.device_seq });
        results.push({ index, type: "task.event", status: duplicate ? "duplicate" : "applied", device_seq: body.device_seq, revision });
      } else if (envelope.type === "message.event") {
        const body = envelope.body as MessageEventBody;
        const { revision, duplicate } = await s.applyMessageEvent(body);
        lastRevision = revision;
        acked.push({ device_id: body.device_id, device_seq: body.device_seq });
        results.push({ index, type: "message.event", status: duplicate ? "duplicate" : "applied", device_seq: body.device_seq, revision });
      } else {
        const body = envelope.body as MetricFrameBody;
        const outcome = await s.ingestMetricFrame(body);
        results.push({ index, type: "metric.frame", status: outcome.status, ...(outcome.error ? { error: outcome.error } : {}) });
      }
    }

    const spaceRevision = lastRevision ?? (await s.currentRevision());
    // Re-arm the outbox alarm with an awaited lifecycle (5c): any push_outbox
    // row the events just enqueued gets its alarm scheduled within this
    // request, so the last row is never stranded on abandoned async work.
    await s.ensureOutboxAlarm();
    // Reverse-control piggyback (v1-architecture.md §4.1, P0-5 fetch-then-
    // ack): applies ack_command_ids FIRST (the only thing that ever sets
    // delivered), then drains this HTTP source's pending commands on EVERY
    // uplink — including an empty events[] heartbeat — so a short-lived
    // `sitrep run` with no WS is still reachable by pause/resume/stop.
    // Included on EVERY poll regardless of prior inclusion, until acked or
    // expired — inclusion alone never marks a row delivered. Omitted when
    // none pending.
    const commands = await s.drainPendingCommandsForHttp(authenticatedDeviceId, forTaskId, ackCommandIds);
    return c.json({ space_revision: spaceRevision, acked, results, ...(commands.length ? { commands } : {}) });
  });

  // ---- state plane: reads ----

  app.get("/v1/snapshot", requireRole("GET /v1/snapshot"), async (c) => {
    const caps: Capabilities = {
      ws_transport_enabled: opts.wsTransportEnabled(c),
      apns_delivery_enabled: opts.apnsDeliveryEnabled(c),
      protocol_versions: [...SUPPORTED_PROTOCOL_VERSIONS],
    };
    return c.json(await stub(c).getSnapshot(caps));
  });

  app.get("/v1/metrics/:id", requireRole("GET /v1/metrics/:id"), async (c) => {
    const sample = await stub(c).getMetric(c.req.param("id")!);
    if (!sample) return c.json({ error: "not found" }, 404);
    return c.json(sample);
  });

  app.get("/v1/metrics/:id/series", requireRole("GET /v1/metrics/:id/series"), async (c) => {
    const range = c.req.query("range") ?? "1d";
    if (!(SERIES_RANGES as string[]).includes(range)) return c.json({ error: "invalid range" }, 400);
    const points = await stub(c).getMetricSeries(c.req.param("id")!, range as SeriesRange);
    return c.json(points);
  });

  app.get("/v1/tasks/:id/log", requireRole("GET /v1/tasks/:id/log"), async (c) => c.json(await stub(c).getTaskLog(c.req.param("id")!)));

  app.post("/v1/tasks/:id/log", requireRole("POST /v1/tasks/:id/log"), async (c) => {
    const body = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
    const lines = body?.lines;
    if (!Array.isArray(lines) || lines.length === 0 || !lines.every((l) => typeof l === "string")) {
      return c.json({ error: "lines[] required" }, 400);
    }
    await stub(c).appendTaskLogLines(c.req.param("id")!, lines as string[]);
    return c.json({ ok: true });
  });

  app.post("/v1/tasks/:id/commands", requireRole("POST /v1/tasks/:id/commands"), async (c) => {
    const body = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
    const action = body?.action;
    if (action !== "pause" && action !== "resume" && action !== "stop") return c.json({ error: "invalid action" }, 400);
    const ttl = parseTtlMs(body);
    if (!ttl.ok) return c.json({ error: "invalid ttl_ms" }, 400);
    const commandId = c.req.header("idempotency-key") ?? (typeof body?.command_id === "string" ? body.command_id : undefined) ?? crypto.randomUUID();
    const cmd: CommandBody = { command_id: commandId, origin: "viewer", issued_by_device_id: c.get("deviceId"), action, task_id: c.req.param("id")!, ttl_ms: ttl.ttlMs };
    // Directed to the task's owning device (P0-3). Reject at enqueue rather
    // than persist a row no device will ever drain: 404 when the task has no
    // owning device (no `started` seen), 409 when the owning device was
    // revoked (MINOR) — both give the viewer clear feedback.
    const result = await stub(c).relayOrQueueCommand(cmd);
    if (!result.ok) {
      const status = result.error === "owning device unavailable" ? 409 : 404;
      return c.json({ error: result.error }, status);
    }
    return c.json({ ok: true });
  });

  app.delete("/v1/messages", requireRole("DELETE /v1/messages"), async (c) => {
    await stub(c).deleteAllMessages();
    return c.json({ ok: true });
  });

  app.delete("/v1/messages/:id", requireRole("DELETE /v1/messages/:id"), async (c) => {
    await stub(c).deleteMessage(c.req.param("id")!);
    return c.json({ ok: true });
  });

  // ---- state plane: automations (v1-architecture.md §5) ----

  app.get("/v1/automations", requireRole("GET /v1/automations"), async (c) => {
    if (c.get("role") === "source") await stub(c).stampAgentSeen();
    return c.json(await stub(c).automationsSnapshot());
  });

  app.post("/v1/automations", requireRole("POST /v1/automations"), async (c) => {
    const body = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
    const parsed = parseAutomationCreate(body);
    if (!parsed) return c.json({ error: "name, executor_kind and schedule.every_seconds required" }, 400);
    const idempotencyKey = c.req.header("idempotency-key") ?? null;
    const explicitId = typeof body?.automation_id === "string" && body.automation_id ? body.automation_id : undefined;
    const automationId = explicitId ?? (idempotencyKey ? await deriveAutomationId(idempotencyKey) : crypto.randomUUID());
    const automation: AutomationState = {
      automation_id: automationId,
      name: parsed.name,
      executor_kind: parsed.executor_kind,
      schedule: { kind: "interval", every_seconds: parsed.every_seconds },
      state: body?.state === "paused" ? "paused" : "active",
      // Server-managed monotonic counter (P0-4): a new automation starts at 0.
      // foldConfigEvent never writes this column (DB DEFAULT 0), so the value
      // here is informational — the run route is the only mutator.
      run_request_id: 0,
    };
    const result = await stub(c).mintConfigEvent(idempotencyKey, { kind: "automation.upserted", automation_id: automationId, automation });
    return mintResultResponse(c, result);
  });

  app.patch("/v1/automations/:id", requireRole("PATCH /v1/automations/:id"), async (c) => {
    const id = c.req.param("id")!;
    const s = stub(c);
    const existing = (await s.automationsSnapshot()).find((a) => a.automation_id === id);
    if (!existing) return c.json({ error: "not found" }, 404);
    const body = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
    const schedule = body?.schedule as Record<string, unknown> | undefined;
    const next: AutomationState = {
      ...existing,
      ...(body?.state === "active" || body?.state === "paused" ? { state: body.state } : {}),
      ...(schedule?.every_seconds !== undefined ? { schedule: { kind: "interval" as const, every_seconds: Math.max(1, Number(schedule.every_seconds) | 0) } } : {}),
    };
    const idempotencyKey = c.req.header("idempotency-key") ?? null;
    const result = await s.mintConfigEvent(idempotencyKey, { kind: "automation.upserted", automation_id: id, automation: next });
    return mintResultResponse(c, result);
  });

  app.delete("/v1/automations/:id", requireRole("DELETE /v1/automations/:id"), async (c) => {
    const id = c.req.param("id")!;
    const s = stub(c);
    const existing = (await s.automationsSnapshot()).find((a) => a.automation_id === id);
    if (!existing) return c.json({ error: "not found" }, 404);
    const idempotencyKey = c.req.header("idempotency-key") ?? null;
    const result = await s.mintConfigEvent(idempotencyKey, { kind: "automation.removed", automation_id: id });
    return mintResultResponse(c, result);
  });

  app.post("/v1/automations/:id/run", requireRole("POST /v1/automations/:id/run"), async (c) => {
    // v1-architecture.md §5.1 (P0-4): a monotonic-id field poll, NOT a command.
    // Increments the automation's run_request_id; the resident agent runs it
    // once when it observes the id advance. Mints no config.event, advances no
    // revision, enqueues no command, takes no request body, returns 200 no
    // body. An optional Idempotency-Key dedups a network retry to one
    // increment. 404s if the id names no automation.
    const idempotencyKey = c.req.header("idempotency-key") ?? null;
    const runRequestId = await stub(c).setRunRequest(c.req.param("id")!, idempotencyKey);
    if (runRequestId === null) return c.json({ error: "not found" }, 404);
    return c.body(null, 200);
  });

  return app;
}
