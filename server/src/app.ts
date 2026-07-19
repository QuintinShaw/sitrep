import { Hono } from "hono";
import type { Context } from "hono";
import type { Next } from "hono";
import {
  mergeMetric,
  SERIES_RANGES,
  visibleTasks,
  type MetricPreference,
  type SeriesRange,
  type SitrepEvent,
  type Store,
  type AutomationDef,
} from "./store.ts";
import { makeSnapshot } from "./domain.ts";

// Multi-tenant spaces: every credential is bound to one space. Tokens are
// `st2_<space>_<secret>` so routing is stateless; the legacy AUTH_TOKEN env
// maps to the "default" space for pre-space installs.
export type Role = "admin" | "owner" | "viewer" | "source";
export type Command = "pause" | "resume" | "stop";

export const TOKEN_RE = /^st2_([a-z0-9]{1,16})_[a-f0-9]{48}$/;

export interface DeviceInfo {
  id: string;
  name: string;
  role: "owner" | "viewer" | "source";
  platform?: string; // macos | ios | android | linux | ...
  created_at: string;
  last_seen?: string;
}

export interface SpaceRegistry {
  /** Initialize a fresh space with its owner device. Returns false if the
   * space already exists (id collision or replay). */
  initSpace(spaceId: string, ownerToken: string, platform: string, name: string): Promise<boolean>;
  resolveToken(token: string): Promise<{ deviceId: string; role: Role } | null>;
  /** Invitation-based joining: possession of a live invite code IS the
   * approval (codes are minted by trusted devices and shown as QR/text). */
  createInvite(role: "viewer" | "source"): Promise<{ code: string; expires_in: number }>;
  join(
    spaceId: string,
    code: string,
    name: string,
    platform: string,
  ): Promise<{ token: string; device_id: string; role: string } | null>;
  devices(): Promise<DeviceInfo[]>;
  revokeDevice(id: string): Promise<void>;
  // push-token registration (viewer devices)
  registerDevice(deviceId: string, pushToStartToken: string): Promise<void>;
  registerAlertToken(deviceId: string, alertToken: string): Promise<void>;
  registerActivityToken(sourceId: string, token: string): Promise<void>;
  // reverse control
  enqueueCommand(sourceId: string, action: Command): Promise<void>;
  drainCommands(sourceIds: string[]): Promise<Record<string, Command[]>>;
}

export interface AppOptions {
  store: (c: Context, space: string) => Store;
  registry?: (c: Context, space: string) => SpaceRegistry;
  /** Schedule post-response work when the host provides an execution
   * context. Node has no Workers ExecutionContext, so it awaits instead. */
  defer?: (c: Context, work: Promise<unknown>) => void;
  /** Legacy single-tenant admin token (self-host); maps to space "default". */
  authToken?: (c: Context) => string | undefined;
  /** Space creation guard (official cloud may rate-limit); default allows. */
  allowSpaceCreation?: (c: Context) => boolean;
  /** Invite directory: connect codes are opaque (no space embedded), so
   * joining by code alone needs a code→space lookup with TTL. */
  publishInvite?: (c: Context, code: string, space: string) => Promise<void>;
  lookupInvite?: (c: Context, code: string) => Promise<string | null>;
  /** Realtime kill switch (`/v3/realtime`): a Wrangler `vars` entry
   * (`REALTIME_ENABLED`), no per-space granularity in v1. Absent on older
   * deployments that don't wire this option, which must mean disabled — so
   * the default below is `false`, never `true`. */
  realtimeEnabled?: (c: Context) => boolean;
}

type Vars = { Variables: { role: Role; space: string; deviceId?: string } };

const COMMANDS: Command[] = ["pause", "resume", "stop"];
const V2_EVENT_KINDS = [
  "task.start", "task.progress", "task.step", "task.done", "task.fail",
  "task.log", "metric.update", "message.send",
] as const;

function validateV2Event(event: Record<string, unknown>): string | null {
  if (!V2_EVENT_KINDS.includes(event.kind as (typeof V2_EVENT_KINDS)[number])) return "unknown kind";
  if (typeof event.source_id !== "string" || event.source_id.length < 1 || event.source_id.length > 256) return "invalid source_id";
  if (typeof event.ts !== "string" || !Number.isFinite(Date.parse(event.ts))) return "invalid timestamp";
  switch (event.kind) {
    case "task.progress":
      if (!Number.isInteger(event.percent) || (event.percent as number) < 0 || (event.percent as number) > 100) return "invalid percent";
      break;
    case "task.step":
      if (typeof event.step !== "string" || !event.step) return "step required";
      break;
    case "metric.update":
      if (typeof event.key !== "string" || !/^[a-z0-9_.-]{1,64}$/.test(event.key)) return "invalid metric id";
      if (typeof event.value !== "string" || event.value.length > 256) return "invalid metric value";
      break;
    case "message.send":
      if (typeof event.text !== "string" || !event.text || event.text.length > 1000) return "message text required";
      if (event.level !== undefined && !["info", "warn", "error"].includes(String(event.level))) return "invalid message level";
      break;
  }
  return null;
}

const rand = (bytes: number) =>
  [...crypto.getRandomValues(new Uint8Array(bytes))].map((b) => b.toString(16).padStart(2, "0")).join("");

export function newSpaceId(): string {
  // 10 chars from a lowercase alphanumeric alphabet.
  const alphabet = "abcdefghjkmnpqrstuvwxyz23456789";
  return [...crypto.getRandomValues(new Uint8Array(10))].map((b) => alphabet[b % alphabet.length]).join("");
}

export function newToken(spaceId: string): string {
  return `st2_${spaceId}_${rand(24)}`;
}

export function createApp(opts: AppOptions) {
  const app = new Hono<Vars>();

  app.get("/healthz", (c) => c.json({ ok: true, service: "sitrep" }));

  // ---- unauthenticated: space creation & invite joining ----

  app.post("/v2/spaces", async (c) => {
    if (!opts.registry) return c.json({ error: "spaces not supported by this deployment" }, 501);
    if (opts.allowSpaceCreation && !opts.allowSpaceCreation(c)) {
      return c.json({ error: "space creation disabled" }, 403);
    }
    const body = await c.req.json().catch(() => ({}));
    const platform = String(body?.platform || "macos").slice(0, 20);
    const name = String(body?.name || "此电脑").slice(0, 60);
    const spaceId = newSpaceId();
    const ownerToken = newToken(spaceId);
    const ok = await opts.registry(c, spaceId).initSpace(spaceId, ownerToken, platform, name);
    if (!ok) return c.json({ error: "try again" }, 503);
    return c.json({ space_id: spaceId, owner_token: ownerToken });
  });

  app.post("/v2/join", async (c) => {
    if (!opts.registry) return c.json({ error: "joining not supported by this deployment" }, 501);
    const body = await c.req.json().catch(() => null);
    const code = String(body?.code || "").toUpperCase();
    if (!code) return c.json({ error: "code required" }, 400);
    // Space comes from the sitrep:// link (self-host) or the invite
    // directory (connect codes are opaque).
    let space = String(body?.space || "");
    if (!space && opts.lookupInvite) {
      space = (await opts.lookupInvite(c, code)) ?? "";
    }
    if (!/^[a-z0-9]{1,16}$/.test(space)) {
      return c.json({ error: "invite invalid or expired" }, 404);
    }
    const name = String(body?.name || "unnamed device").slice(0, 60);
    const platform = String(body?.platform || "unknown").slice(0, 20);
    const joined = await opts.registry(c, space).join(space, code, name, platform);
    if (!joined) return c.json({ error: "invite invalid or expired" }, 404);
    return c.json({ ...joined, space_id: space });
  });

  // ---- authenticated: resolve space + role from the token ----

  // Shared by /v2/* (existing product API) and /v3/* (realtime + its HTTP
  // control-plane companion): identical st2 token parsing and role/space/
  // deviceId resolution, so /v3 gets exactly the same "invalid token never
  // reaches a handler" guarantee /v2 already has, with no duplicated logic.
  const authenticate = async (c: Context<Vars>, next: Next) => {
    if (c.req.path === "/v2/spaces" || c.req.path === "/v2/join") return next();
    const admin = opts.authToken?.(c);
    if (!admin && !opts.registry) {
      c.set("role", "admin");
      c.set("space", "default");
      return next(); // bare local dev: open
    }
    const got = c.req.header("authorization")?.replace(/^Bearer\s+/i, "");
    if (!got) return c.json({ error: "unauthorized" }, 401);
    if (admin && got === admin) {
      c.set("role", "admin");
      c.set("space", "default");
      return next();
    }
    const m = TOKEN_RE.exec(got);
    if (!m || !opts.registry) return c.json({ error: "unauthorized" }, 401);
    const space = m[1];
    const resolved = await opts.registry(c, space).resolveToken(got);
    if (!resolved) return c.json({ error: "unauthorized" }, 401);
    c.set("role", resolved.role);
    c.set("space", space);
    c.set("deviceId", resolved.deviceId);
    await next();
  };
  app.use("/v2/*", authenticate);
  app.use("/v3/*", authenticate);

  const store = (c: Context<Vars>) => opts.store(c, c.get("space"));
  const registry = (c: Context<Vars>) => opts.registry?.(c, c.get("space"));
  const canView = (c: Context<Vars>) => ["admin", "owner", "viewer"].includes(c.get("role"));
  const canEmit = (c: Context<Vars>) => ["admin", "owner", "source"].includes(c.get("role"));

  app.post("/v2/ingest", async (c) => {
    if (!canEmit(c)) return c.json({ error: "forbidden" }, 403);
    const body = await c.req.json().catch(() => null);
    const raw = Array.isArray(body) ? body : [body];
    if (raw.length > 1000) return c.json({ error: "batch too large" }, 413);
    for (const event of raw) {
      if (!event || typeof event !== "object") return c.json({ error: "invalid v2 event" }, 400);
      const error = validateV2Event(event);
      if (error) return c.json({ error }, 400);
    }
    const events = raw as SitrepEvent[];
    await store(c).apply(events);
    const presenceWrite = store(c).stampPresence("ingest", new Date().toISOString());
    if (opts.defer) opts.defer(c, presenceWrite);
    else await presenceWrite;
    const ids = new Set(events.map((event) => event.source_id));
    for (const source of (c.req.query("sources") || "").split(",")) if (source) ids.add(source);
    const r = registry(c);
    return c.json({ accepted: events.length, commands: r && ids.size ? await r.drainCommands([...ids]) : {} });
  });

  app.post("/v2/invites", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    const r = registry(c);
    if (!r) return c.json({ error: "not supported" }, 501);
    const body = await c.req.json().catch(() => ({}));
    const role = body?.role === "source" ? "source" : "viewer";
    const invite = await r.createInvite(role);
    await opts.publishInvite?.(c, invite.code, c.get("space"));
    return c.json({ ...invite, space_id: c.get("space") });
  });

  // ---- product API ----

  app.get("/v2/snapshot", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    const s = store(c);
    const [prefs, tasks, metrics, events, automations, presence] = await Promise.all([
      s.metricPrefs(),
      s.tasks(),
      s.metrics(),
      s.eventLog(),
      s.automations(),
      s.presence(),
    ]);
    return c.json({
      ...makeSnapshot({
        now: new Date().toISOString(),
        presence,
        tasks: visibleTasks(tasks, false),
        metrics: metrics.map((metric) => mergeMetric(metric, prefs)),
        events,
        automations,
      }),
      // Apple client's server-side kill switch for /v3/realtime auto-connect
      // (see AppOptions.realtimeEnabled): missing or unset must read as
      // disabled, so this defaults to false rather than being omitted.
      realtime_enabled: opts.realtimeEnabled ? opts.realtimeEnabled(c) : false,
    });
  });

  app.get("/v2/automations", async (c) => {
    if (!canEmit(c)) return c.json({ error: "forbidden" }, 403);
    if (c.req.query("agent") === "1") {
      await store(c).stampPresence("agent", new Date().toISOString());
    }
    return c.json(await store(c).automations());
  });

  app.post("/v2/automations", async (c) => {
    if (!canEmit(c)) return c.json({ error: "forbidden" }, 403);
    const body = await c.req.json().catch(() => null);
    const kind = body?.executor?.kind;
    const command = body?.executor?.command;
    const everySeconds = Number(body?.schedule?.every_seconds);
    if (!body?.name || !["script", "agent", "hybrid"].includes(kind) || !Array.isArray(command) || command.length === 0 || !Number.isFinite(everySeconds)) {
      return c.json({ error: "name, executor.kind, executor.command and schedule.every_seconds required" }, 400);
    }
    const now = new Date().toISOString();
    const automation: AutomationDef = {
      id: rand(6),
      name: String(body.name).slice(0, 60),
      command: command.map(String),
      executor_kind: kind,
      every_s: Math.max(5, everySeconds | 0),
      enabled: body.state !== "paused",
      created_at: now,
      updated_at: now,
    };
    await store(c).putAutomation(automation);
    return c.json(automation);
  });

  app.patch("/v2/automations/:id", async (c) => {
    if (!canView(c) && !canEmit(c)) return c.json({ error: "forbidden" }, 403);
    const body = await c.req.json().catch(() => null);
    if (!body) return c.json({ error: "invalid JSON" }, 400);
    const patch: Record<string, unknown> = {};
    if (body.schedule?.every_seconds !== undefined) {
      patch.every_s = Math.max(5, Number(body.schedule.every_seconds) | 0);
    }
    if (body.state !== undefined) {
      if (body.state !== "active" && body.state !== "paused") {
        return c.json({ error: "state must be active or paused" }, 400);
      }
      patch.enabled = body.state === "active";
    }
    if (body.run_now === true) patch.run_requested_at = new Date().toISOString();
    if (body.last_run_at !== undefined && canEmit(c)) patch.last_run = String(body.last_run_at);
    const automation = await store(c).patchAutomation(c.req.param("id"), patch);
    return automation ? c.json({ ok: true }) : c.json({ error: "not found" }, 404);
  });

  app.delete("/v2/automations/:id", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    await store(c).deleteAutomation(c.req.param("id"));
    return c.json({ ok: true });
  });

  app.patch("/v2/metrics/:id", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    const body = (await c.req.json().catch(() => null)) as MetricPreference | null;
    if (!body) return c.json({ error: "invalid JSON" }, 400);
    await store(c).setMetricPref(c.req.param("id"), body);
    return c.json({ ok: true });
  });

  app.get("/v2/tasks/:id/log", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    return c.json(await store(c).taskLog(c.req.param("id")));
  });

  app.get("/v2/metrics/:id/series", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    const range = (c.req.query("range") ?? "1d") as SeriesRange;
    if (!SERIES_RANGES.includes(range)) return c.json({ error: "invalid range" }, 400);
    return c.json(await store(c).metricSeries(c.req.param("id"), range));
  });

  app.post("/v2/messages/delete", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    const body = await c.req.json().catch(() => null);
    if (body?.all === true) await store(c).deleteEvents("all");
    else {
      const ids = Array.isArray(body?.ids) ? body.ids.map(String) : [];
      if (!ids.length) return c.json({ error: "ids[] or all:true required" }, 400);
      await store(c).deleteEvents(ids);
    }
    return c.json({ ok: true });
  });

  app.post("/v2/tasks/:id/commands", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    const r = registry(c);
    if (!r) return c.json({ error: "commands not supported" }, 501);
    const body = await c.req.json().catch(() => null);
    const action = body?.action as Command;
    if (!COMMANDS.includes(action)) return c.json({ error: "invalid action" }, 400);
    await r.enqueueCommand(c.req.param("id"), action);
    return c.json({ ok: true });
  });

  // ---- device management & push registration ----

  app.get("/v2/devices", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    const r = registry(c);
    return c.json(r ? await r.devices() : []);
  });

  app.delete("/v2/devices/:id", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    const r = registry(c);
    if (!r) return c.json({ error: "not supported" }, 501);
    await r.revokeDevice(c.req.param("id"));
    return c.json({ ok: true });
  });

  app.post("/v2/devices", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    const r = registry(c);
    if (!r) return c.json({ error: "push not supported by this deployment" }, 501);
    const body = await c.req.json().catch(() => null);
    if (!body?.device_id || (!body?.push_to_start_token && !body?.alert_token)) {
      return c.json({ error: "device_id plus push_to_start_token or alert_token required" }, 400);
    }
    if (body.push_to_start_token) await r.registerDevice(body.device_id, body.push_to_start_token);
    if (body.alert_token) await r.registerAlertToken(body.device_id, body.alert_token);
    return c.json({ ok: true });
  });

  app.post("/v2/activities", async (c) => {
    if (!canView(c)) return c.json({ error: "forbidden" }, 403);
    const r = registry(c);
    if (!r) return c.json({ error: "push not supported by this deployment" }, 501);
    const body = await c.req.json().catch(() => null);
    if (!body?.source_id || !body?.token) return c.json({ error: "source_id and token required" }, 400);
    await r.registerActivityToken(body.source_id, body.token);
    return c.json({ ok: true });
  });

  return app;
}
