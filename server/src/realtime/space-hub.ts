// SpaceHub: one Durable Object per Sitrep space, implementing the realtime
// synchronization protocol (proto/realtime/SPEC.md, frozen v1). SQLite
// backed, Hibernatable-WebSockets based — the DO holds no connection state
// in memory that isn't recoverable from storage + each connection's
// serialized attachment (see ./attachment.ts).
//
// Concurrency note read before editing: every handler below that must
// behave as "one durable transaction" (SPEC.md section 5.2 dedup+apply,
// section 5.5 config.event mint+revision) is written as a sequence of
// synchronous `this.ctx.storage.sql.exec` calls with NO `await` between
// them. Durable Object storage coalesces consecutive synchronous writes —
// and the JS isolate never interleaves another callback into a running
// synchronous function — so this is sufficient for atomicity without a
// separate transaction API. Do not introduce an `await` in the middle of
// one of these methods without re-establishing atomicity another way.
//
// The same single-threaded-synchronous-function argument is what satisfies
// SPEC.md section 6.2's "no other outbound envelope may interleave a
// chunked snapshot": sendSnapshotChunks/sendChainedDeltas build every frame
// up front and `ws.send()` them in a plain synchronous loop, so nothing
// else in this DO can run until the whole reply has gone out.

import { DurableObject } from "cloudflare:workers";
import { type ConnAttachment, isConnAttachment } from "./attachment.ts";
import { chunkDeltaEvents, chunkSnapshot } from "./chunking.ts";
import { authorizeClientEnvelope, parseEnvelope } from "./guards.ts";
import {
  ERROR_SEMANTICS,
  HEARTBEAT_INTERVAL_MS,
  LEASE_DEFAULT_MS,
  METRIC_CACHE_MAX_METRICS,
  METRIC_FRAME_RATE_PER_SEC,
  EVENT_LOG_RETENTION_REVISIONS,
  MESSAGE_WINDOW,
  PROTOCOL_VERSION,
  SUPPORTED_PROTOCOL_VERSIONS,
  makeEnvelope,
  makeStandardError,
  newEnvelopeId,
} from "./protocol.ts";
import {
  rowToAutomationState,
  rowToDeltaEventItem,
  rowToMessageRecord,
  rowToTaskState,
  type AutomationRow,
  type EventLogRow,
  type MessageRow,
  type TaskRow,
} from "./rows.ts";
import type {
  AnyEnvelope,
  AutomationState,
  CommandBody,
  ConfigEventBody,
  ConfigEventKind,
  DeltaBody,
  ErrorCode,
  HelloOfferBody,
  InterestRenewBody,
  MessageEventBody,
  MessageType,
  MetricFrameBody,
  MetricSample,
  ResumeBody,
  SubscribeBody,
  TaskEventBody,
} from "./types.ts";

interface RateLimiterState {
  frameTimestamps: number[];
}

/** Deterministic JSON with recursively sorted object keys, so two
 * structurally-equal values always serialize identically regardless of
 * property construction order. Used for idempotency fingerprints. */
function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (typeof value === "object" && value !== null) {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`);
    return `{${entries.join(",")}}`;
  }
  return JSON.stringify(value);
}

export class SpaceHub extends DurableObject<Env> {
  /** Current schema version this build expects. Bump when `migrate()`'s
   * DDL block changes, alongside adding whatever new `CREATE TABLE/INDEX
   * IF NOT EXISTS` statements the change needs. */
  private static readonly SCHEMA_VERSION = 1;

  /** Best-effort metric cache (SPEC.md section 6.2: snapshot.metrics is a
   * convenience cache, allowed to be empty/stale, outside space_revision
   * accounting) — deliberately in-memory only, never written to SQLite.
   * Capped at METRIC_CACHE_MAX_METRICS distinct metric_ids with
   * least-recently-updated eviction (see touchMetricCache) so a source
   * rotating metric_ids without bound can't grow this without bound. Map
   * iteration order is insertion order and JS re-inserts a key at the end
   * on delete+set, which is exactly what touchMetricCache relies on to
   * track recency. */
  private metricsCache = new Map<string, MetricSample>();
  private rateLimiters = new WeakMap<WebSocket, RateLimiterState>();
  private hotPathCounter = 0;

  /** Set true only when migrate() actually executed the DDL block during
   * this construct, as opposed to finding the store already at
   * SCHEMA_VERSION and returning without touching SQLite. Exists purely
   * so tests can assert the 10+ `CREATE TABLE/INDEX IF NOT EXISTS`
   * statements do NOT re-run on every hibernation wake (DO constructors
   * re-run on wake; see the constructor's migrate() call and the
   * space-hub.ts cost notes) — never read by production logic. */
  private ranMigrationDdl = false;

  /** Injectable for tests: decides whether the Nth hot-path event gets
   * logged. Default samples at <=1%. Errors always log regardless of this
   * (see logAlways) — only routine per-frame hot-path logging is sampled. */
  logSampler: (n: number) => boolean = (n) => n % 100 === 0;

  /** Every logAlways() (100%-sampled, security/error tier) event recorded
   * by this DO instance, newest last. In-memory only, capped, and never
   * persisted or read by production code — it exists purely so
   * vitest-pool-workers tests can assert a log fired (e.g. on
   * supersession) via `runInDurableObject(stub, (i) => (i as any).
   * securityEventLog)`, since console output can't be captured across the
   * workerd/vitest process boundary. */
  private securityEventLog: Array<{ event: string; data: Record<string, unknown> }> = [];

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // Answers a bare "ping" text frame with "pong" entirely inside the
    // runtime, without waking a hibernated DO or invoking webSocketMessage
    // (SPEC.md section 9.3) — the whole point of the heartbeat being plain
    // text rather than a JSON envelope.
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    this.migrate();
  }

  /** Runs the schema DDL exactly once per store lifetime, not once per DO
   * construct. DO constructors re-run on every hibernation wake (that's
   * the whole point of hibernation — the JS instance is cheap to recreate,
   * the SQLite store is what's durable), so unconditionally re-issuing
   * 10+ `CREATE TABLE/INDEX IF NOT EXISTS` statements here would mean
   * every wake pays for a full schema scan, defeating the sparse-
   * connection cost goals documented in realtime-server.md. Instead: one
   * lightweight version read, and the DDL block (plus the version write)
   * only runs when the store is behind — including the fresh-DO case,
   * where the version table itself doesn't exist yet. */
  private migrate(): void {
    if (this.schemaVersion() >= SpaceHub.SCHEMA_VERSION) return;

    this.ranMigrationDdl = true;
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS _schema_migrations (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        version INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS space_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS event_log (
        revision INTEGER PRIMARY KEY,
        event_type TEXT NOT NULL,
        device_id TEXT,
        device_seq INTEGER,
        occurred_at INTEGER NOT NULL,
        payload TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_event_log_device ON event_log(device_id, device_seq);
      CREATE TABLE IF NOT EXISTS dedup (
        device_id TEXT NOT NULL,
        device_seq INTEGER NOT NULL,
        revision INTEGER NOT NULL,
        PRIMARY KEY (device_id, device_seq)
      );
      CREATE TABLE IF NOT EXISTS tasks (
        task_id TEXT PRIMARY KEY,
        device_id TEXT NOT NULL,
        title TEXT,
        state TEXT NOT NULL,
        percent INTEGER,
        step TEXT,
        message TEXT,
        updated_at INTEGER NOT NULL,
        display TEXT
      );
      CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY,
        device_id TEXT NOT NULL,
        level TEXT NOT NULL,
        text TEXT NOT NULL,
        occurred_at INTEGER NOT NULL,
        revision INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_messages_revision ON messages(revision);
      CREATE TABLE IF NOT EXISTS automations (
        automation_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        executor_kind TEXT NOT NULL,
        every_seconds INTEGER NOT NULL,
        state TEXT NOT NULL,
        last_run_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS leases (
        device_id TEXT PRIMARY KEY,
        expires_at INTEGER NOT NULL,
        topics TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS push_outbox (
        event_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS http_idempotency (
        idempotency_key TEXT PRIMARY KEY,
        fingerprint TEXT NOT NULL,
        revision INTEGER NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS pending_commands (
        command_id TEXT PRIMARY KEY,
        target_device_id TEXT,
        origin_ts INTEGER NOT NULL,
        ttl_ms INTEGER NOT NULL,
        payload TEXT NOT NULL,
        delivered INTEGER NOT NULL DEFAULT 0
      );
    `);
    // Same synchronous, no-`await`-in-between transaction as the DDL above
    // (see the class-level concurrency note) — the version write is
    // atomic with the schema it describes.
    this.ctx.storage.sql.exec(
      `INSERT INTO _schema_migrations (id, version) VALUES (1, ?)
       ON CONFLICT(id) DO UPDATE SET version = excluded.version`,
      SpaceHub.SCHEMA_VERSION,
    );
  }

  /** One lightweight SELECT: current schema version, or 0 for a fresh DO
   * (the `_schema_migrations` table itself doesn't exist yet, which throws
   * rather than returning zero rows — SQLite has no "does this table
   * exist" query result short of inspecting sqlite_master, and a bare
   * SELECT-that-may-throw is cheaper and simpler than checking
   * sqlite_master first on every call). */
  private schemaVersion(): number {
    try {
      const row = this.ctx.storage.sql.exec<{ version: number }>("SELECT version FROM _schema_migrations WHERE id = 1").toArray()[0];
      return row?.version ?? 0;
    } catch {
      return 0;
    }
  }

  // ---- WebSocket entry point ----

  async fetch(request: Request): Promise<Response> {
    if ((request.headers.get("upgrade") ?? "").toLowerCase() !== "websocket") {
      return new Response("expected websocket upgrade", { status: 426 });
    }
    // Trust boundary: these headers are set by the Worker AFTER it already
    // validated the st2 token (adapters/workers.ts) — never derived from
    // anything the client sent inside the WebSocket connection itself.
    const deviceId = request.headers.get("x-sitrep-device-id");
    const role = request.headers.get("x-sitrep-role");
    if (!deviceId || (role !== "source" && role !== "viewer")) {
      return new Response("missing trusted identity", { status: 400 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server, [deviceId]);
    const attachment: ConnAttachment = {
      deviceId,
      role,
      connectedAt: Date.now(),
      helloDone: false,
      subscribedThisConn: false,
      deltaEligible: false,
    };
    server.serializeAttachment(attachment);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    // Top-level exception guard: an unexpected throw anywhere in a message
    // handler must degrade to error{internal_error} (SPEC.md section 13:
    // retryable, non-fatal, message free of implementation details), never
    // kill the DO instance or silently swallow the frame.
    try {
      this.dispatchMessage(ws, message);
    } catch (e) {
      this.logAlways("unhandled_error", { message: String(e), stack: (e as Error)?.stack });
      try {
        this.reply(ws, "internal_error", "unexpected server error", undefined);
      } catch {
        // The socket itself may be broken; nothing more to do.
      }
    }
  }

  private dispatchMessage(ws: WebSocket, message: string | ArrayBuffer): void {
    if (typeof message !== "string") return; // no binary frames in this protocol
    if (message === "ping" || message === "pong") return; // handled by auto-response; defensive no-op

    const raw = ws.deserializeAttachment();
    if (!isConnAttachment(raw)) {
      ws.close(1011, "missing connection identity");
      return;
    }
    const att = raw;

    if (!att.helloDone) {
      this.handlePreHello(ws, att, message);
      return;
    }

    const parsed = parseEnvelope(message);
    if (parsed.kind === "unknown_type") return; // SPEC.md section 15: ignore, never malformed
    if (parsed.kind === "error") {
      this.reply(ws, parsed.code, parsed.message, undefined);
      return;
    }
    const envelope = parsed.envelope;

    // A client-sent hello{stage:"accept"} is a handshake violation at ANY
    // point in the connection's life (SPEC.md section 9.1), not just before
    // the first accept.
    if (envelope.type === "hello") {
      this.reply(ws, "hello_required", "stage:accept is server-only", envelope.id);
      ws.close(1008, "hello_required");
      return;
    }

    const authz = authorizeClientEnvelope(att.role, envelope);
    if (!authz.ok) {
      this.reply(ws, authz.code, `role ${att.role} may not send ${envelope.type}`, envelope.id);
      if (ERROR_SEMANTICS[authz.code].fatal) ws.close(1008, authz.code);
      return;
    }

    this.logHotPath("frame", { type: envelope.type, device_id: att.deviceId });

    switch (envelope.type) {
      case "resume":
        this.handleResume(ws, att, envelope.body as ResumeBody, envelope.id);
        break;
      case "subscribe":
        this.handleSubscribe(ws, att, envelope.body as SubscribeBody, envelope.id);
        break;
      case "unsubscribe":
        this.handleUnsubscribe(ws, att, envelope.id);
        break;
      case "interest.renew":
        this.handleInterestRenew(ws, att, envelope.body as InterestRenewBody, envelope.id);
        break;
      case "task.event":
        this.handleTaskEvent(ws, att, envelope.body as TaskEventBody, envelope.id);
        break;
      case "message.event":
        this.handleMessageEvent(ws, att, envelope.body as MessageEventBody, envelope.id);
        break;
      case "metric.frame":
        this.handleMetricFrame(ws, att, envelope.body as MetricFrameBody, envelope.id);
        break;
      case "command":
        this.handleCommand(ws, att, envelope.body as CommandBody, envelope.ts, envelope.id);
        break;
      case "ack":
      case "error":
        // Optional/diagnostic client-sent frames: nothing to durably do.
        break;
      default:
        this.reply(ws, "malformed", `unexpected type ${envelope.type}`, envelope.id);
    }
  }

  async webSocketClose(): Promise<void> {
    try {
      // Interest leases and the event log are keyed by device/space, not by
      // connection (SPEC.md section 7) — nothing to clean up on close beyond
      // letting the socket go. The guard exists so any future cleanup added
      // here degrades to a log line instead of an uncaught DO exception.
    } catch (e) {
      this.logAlways("unhandled_error", { message: String(e), phase: "webSocketClose" });
    }
  }

  async webSocketError(_ws: WebSocket, error: unknown): Promise<void> {
    this.logAlways("ws_error", { message: String(error) });
  }

  // ---- HTTP control-plane entry point (config.event minting) ----

  /** Mints (or replays, if idempotencyKey was already seen) a config.event
   * for an automation upsert/removal. Single synchronous transaction, same
   * discipline as applyReliableEvent — see the class-level comment.
   *
   * The idempotency key is bound to the request's CONTENT, not just its
   * key string: the stored fingerprint is truly canonical JSON —
   * recursively key-sorted (see canonicalJson), so it cannot drift if a
   * caller's object-construction order ever changes — of
   * (kind, automation_id, automation), compared verbatim on a replay.
   * (Exact string comparison is strictly stronger than a hash and stays
   * synchronous — crypto.subtle.digest is async and would break the
   * single-transaction discipline.) A key reused for a DIFFERENT
   * operation returns { conflict: true } and mints nothing, instead of
   * silently replaying the first operation's result.
   *
   * Contract note: the fingerprint must cover only client-provided
   * content. When the client omits automation_id on a create, the HTTP
   * layer derives it deterministically from the Idempotency-Key BEFORE
   * calling this method (see adapters/workers.ts deriveAutomationId), so
   * an honest retry reconstructs the identical payload and replays with
   * the originally-created automation_id rather than tripping a 409. */
  mintConfigEvent(
    idempotencyKey: string | null,
    payload: { kind: ConfigEventKind; automation_id: string; automation?: AutomationState },
  ): { revision: number; automation: AutomationState | null; conflict?: boolean } {
    const fingerprint = canonicalJson({
      kind: payload.kind,
      automation_id: payload.automation_id,
      automation: payload.automation ?? null,
    });
    if (idempotencyKey) {
      const existing = this.ctx.storage.sql
        .exec<{ revision: number; fingerprint: string }>(
          "SELECT revision, fingerprint FROM http_idempotency WHERE idempotency_key = ?",
          idempotencyKey,
        )
        .toArray()[0];
      if (existing) {
        if (existing.fingerprint !== fingerprint) {
          return { revision: existing.revision, automation: null, conflict: true };
        }
        return { revision: existing.revision, automation: this.readAutomation(payload.automation_id) };
      }
    }

    const occurredAt = Date.now();
    const revision = this.getRevision() + 1;
    const body: ConfigEventBody = {
      kind: payload.kind,
      automation_id: payload.automation_id,
      automation: payload.automation,
      occurred_at: occurredAt,
    };
    const eventPayload = JSON.stringify(body);

    this.ctx.storage.sql.exec(
      `INSERT INTO event_log (revision, event_type, device_id, device_seq, occurred_at, payload) VALUES (?, 'config.event', NULL, NULL, ?, ?)`,
      revision,
      occurredAt,
      eventPayload,
    );
    this.foldConfigEvent(body);
    this.setRevision(revision);
    if (idempotencyKey) {
      this.ctx.storage.sql.exec(
        `INSERT INTO http_idempotency (idempotency_key, fingerprint, revision, created_at) VALUES (?, ?, ?, ?)`,
        idempotencyKey,
        fingerprint,
        revision,
        occurredAt,
      );
      // Bounded growth, cleaned lazily on the write path (no alarm): drop
      // entries older than 24h, and cap the table at the most recent 500
      // rows. A retry arriving after both windows re-executes as a fresh
      // request — acceptable, since config.event minting is idempotent at
      // the state level (upsert/delete) even without the key.
      this.ctx.storage.sql.exec(`DELETE FROM http_idempotency WHERE created_at < ?`, occurredAt - 24 * 3600 * 1000);
      this.ctx.storage.sql.exec(
        `DELETE FROM http_idempotency WHERE idempotency_key NOT IN (
           SELECT idempotency_key FROM http_idempotency ORDER BY created_at DESC LIMIT 500
         )`,
      );
    }
    this.pruneEventLog(revision);

    this.broadcastDelta({
      from_revision: revision - 1,
      to_revision: revision,
      events: [{ event_type: "config.event", event: body }],
    });

    return { revision, automation: this.readAutomation(payload.automation_id) };
  }

  automationsSnapshot(): AutomationState[] {
    return this.ctx.storage.sql.exec<AutomationRow>("SELECT * FROM automations ORDER BY automation_id").toArray().map(rowToAutomationState);
  }

  // ---- hello / handshake ----

  private handlePreHello(ws: WebSocket, att: ConnAttachment, message: string): void {
    const parsed = parseEnvelope(message);
    const offerBody =
      parsed.kind === "ok" && parsed.envelope.type === "hello" && (parsed.envelope.body as HelloOfferBody).stage === "offer"
        ? (parsed.envelope.body as HelloOfferBody)
        : null;
    if (!offerBody) {
      this.reply(ws, "hello_required", "hello{stage:offer} must be the first frame", undefined);
      ws.close(1008, "hello_required");
      return;
    }

    const intersection = offerBody.protocol_versions.filter((v) => SUPPORTED_PROTOCOL_VERSIONS.includes(v));
    if (intersection.length === 0) {
      this.reply(ws, "version_unsupported", "no shared protocol version", parsed.kind === "ok" ? parsed.envelope.id : undefined);
      ws.close(1008, "version_unsupported");
      return;
    }
    const negotiated = Math.max(...intersection);

    // SPEC.md section 9.4: the new connection completing hello supersedes
    // any other open connection already authenticated for this device_id.
    const newSessionId = crypto.randomUUID();
    for (const other of this.ctx.getWebSockets(att.deviceId)) {
      if (other === ws) continue;
      const otherAtt = other.deserializeAttachment();
      if (isConnAttachment(otherAtt) && otherAtt.helloDone) {
        this.send(other, "error", {
          code: "superseded",
          message: "device completed hello on a newer connection",
          ...ERROR_SEMANTICS.superseded,
        });
        // Product invariant 5: superseded must never be silent — it is a
        // security-relevant event (possible credential/session reuse), so it
        // always logs at 100% via logAlways, independent of the wire reply
        // above (which must stay byte-identical for the superseded peer).
        this.logAlways("superseded", {
          device_id: att.deviceId,
          role: att.role,
          superseded_session_id: otherAtt.sessionId,
          superseding_session_id: newSessionId,
        });
        other.close(1008, "superseded");
      }
    }

    att.helloDone = true;
    att.sessionId = newSessionId;
    ws.serializeAttachment(att);

    this.send(ws, "hello", {
      stage: "accept",
      protocol_version: negotiated,
      session_id: att.sessionId,
      heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
    });

    if (att.role === "source") {
      // Interest-state sync on (re)connect. SPEC.md section 7's throttle/
      // resume_rate are pure edge triggers, so a source that was offline
      // when the space's lease count crossed an edge would otherwise stay
      // stuck at its last-known rate forever (e.g. throttled while viewers
      // are actively watching). Immediately after the accept, tell this
      // connection the CURRENT interest state. This adds no new message
      // type or schema change — it reuses command{origin:server} with a
      // fresh command_id — and is a v1.1 protocol clarification candidate
      // (ruled by the protocol owner; see the handoff notes).
      this.sendCurrentRateState(ws);
      this.drainPendingCommands(ws, att.deviceId);
    }
    this.reconcileLeaseEdge();
  }

  // ---- resume / snapshot / delta ----

  private handleResume(ws: WebSocket, att: ConnAttachment, body: ResumeBody, envelopeId: string): void {
    if (!att.subscribedThisConn) {
      this.reply(ws, "malformed", "resume must follow subscribe on the same connection", envelopeId);
      return;
    }
    const N = body.last_revision;
    const R = this.getRevision();

    if (N > R) {
      this.reply(ws, "revision_unavailable", `last_revision ${N} exceeds current revision ${R}`, envelopeId);
      return; // deltaEligible stays false — no reply here confers eligibility
    }

    if (N === 0) {
      this.sendSnapshotChunks(ws, R);
    } else if (N === R) {
      this.send(ws, "delta", { from_revision: N, to_revision: N, events: [] });
    } else {
      const minRetained = this.minRetainedRevision();
      if (minRetained !== null && minRetained <= N + 1) {
        this.sendChainedDeltas(ws, N, this.eventLogRange(N, R));
      } else {
        this.sendSnapshotChunks(ws, R);
      }
    }

    att.deltaEligible = true;
    ws.serializeAttachment(att);
  }

  private sendSnapshotChunks(ws: WebSocket, revision: number): void {
    const arrays = this.buildSnapshotArrays();
    const chunks = chunkSnapshot(revision, arrays);
    // No `await` in this loop: see the class-level concurrency note. This
    // is what guarantees nothing else can interleave an envelope (SPEC.md
    // section 6.2) between chunks, including on this same connection.
    for (const chunk of chunks) this.send(ws, "snapshot", chunk);
  }

  private sendChainedDeltas(ws: WebSocket, fromRevision: number, rows: EventLogRow[]): void {
    const events = rows.map(rowToDeltaEventItem);
    const deltas = chunkDeltaEvents(fromRevision, events);
    for (const delta of deltas) this.send(ws, "delta", delta);
  }

  private buildSnapshotArrays() {
    const tasks = this.ctx.storage.sql.exec<TaskRow>("SELECT * FROM tasks ORDER BY task_id").toArray().map(rowToTaskState);
    const automations = this.ctx.storage.sql
      .exec<AutomationRow>("SELECT * FROM automations ORDER BY automation_id")
      .toArray()
      .map(rowToAutomationState);
    const messageRows = this.ctx.storage.sql
      .exec<MessageRow>("SELECT * FROM messages ORDER BY revision DESC LIMIT ?", MESSAGE_WINDOW)
      .toArray();
    messageRows.reverse();
    const messages = messageRows.map(rowToMessageRecord);
    const metrics = [...this.metricsCache.values()];
    return { tasks, metrics, messages, automations };
  }

  private minRetainedRevision(): number | null {
    const row = this.ctx.storage.sql.exec<{ m: number | null }>("SELECT MIN(revision) as m FROM event_log").toArray()[0];
    return row?.m ?? null;
  }

  private eventLogRange(fromExclusive: number, toInclusive: number): EventLogRow[] {
    return this.ctx.storage.sql
      .exec<EventLogRow>("SELECT * FROM event_log WHERE revision > ? AND revision <= ? ORDER BY revision", fromExclusive, toInclusive)
      .toArray();
  }

  // ---- reliable event application (task.event / message.event) ----

  private handleTaskEvent(ws: WebSocket, att: ConnAttachment, body: TaskEventBody, envelopeId: string): void {
    if (body.device_id !== att.deviceId) {
      this.reply(ws, "unauthorized", "device_id does not match the authenticated identity", envelopeId);
      return;
    }
    const { revision, duplicate } = this.applyReliableEvent("task.event", body.device_id, body.device_seq, body, body.occurred_at);
    this.send(ws, "ack", { acked: [{ device_id: body.device_id, device_seq: body.device_seq }] });
    if (!duplicate) {
      this.broadcastDelta({ from_revision: revision - 1, to_revision: revision, events: [{ event_type: "task.event", event: body }] });
      this.reconcileLeaseEdge();
    }
  }

  private handleMessageEvent(ws: WebSocket, att: ConnAttachment, body: MessageEventBody, envelopeId: string): void {
    if (body.device_id !== att.deviceId) {
      this.reply(ws, "unauthorized", "device_id does not match the authenticated identity", envelopeId);
      return;
    }
    const { revision, duplicate } = this.applyReliableEvent("message.event", body.device_id, body.device_seq, body, body.occurred_at);
    this.send(ws, "ack", { acked: [{ device_id: body.device_id, device_seq: body.device_seq }] });
    if (!duplicate) {
      this.broadcastDelta({ from_revision: revision - 1, to_revision: revision, events: [{ event_type: "message.event", event: body }] });
      this.reconcileLeaseEdge();
    }
  }

  private applyReliableEvent(
    eventType: "task.event" | "message.event",
    deviceId: string,
    deviceSeq: number,
    eventBody: TaskEventBody | MessageEventBody,
    occurredAt: number,
  ): { revision: number; duplicate: boolean } {
    const existing = this.ctx.storage.sql
      .exec<{ revision: number }>("SELECT revision FROM dedup WHERE device_id = ? AND device_seq = ?", deviceId, deviceSeq)
      .toArray()[0];
    if (existing) {
      // SPEC.md section 5.2: MUST NOT re-apply, MUST NOT bump revision,
      // MUST still ack — the caller sends the ack regardless of `duplicate`.
      return { revision: existing.revision, duplicate: true };
    }

    const revision = this.getRevision() + 1;
    const payload = JSON.stringify(eventBody);

    this.ctx.storage.sql.exec(
      `INSERT INTO event_log (revision, event_type, device_id, device_seq, occurred_at, payload) VALUES (?, ?, ?, ?, ?, ?)`,
      revision,
      eventType,
      deviceId,
      deviceSeq,
      occurredAt,
      payload,
    );
    this.ctx.storage.sql.exec(`INSERT INTO dedup (device_id, device_seq, revision) VALUES (?, ?, ?)`, deviceId, deviceSeq, revision);
    if (eventType === "task.event") this.foldTaskEvent(eventBody as TaskEventBody);
    else this.foldMessageEvent(eventBody as MessageEventBody, revision);
    this.setRevision(revision);
    // event_id is its own idempotency key: this INSERT only ever runs on
    // the non-duplicate branch, so a retried device_seq can never mint a
    // second outbox row for the same logical event.
    this.ctx.storage.sql.exec(
      `INSERT INTO push_outbox (event_id, kind, payload, status, created_at) VALUES (?, ?, ?, 'pending', ?)`,
      crypto.randomUUID(),
      eventType,
      payload,
      Date.now(),
    );
    this.pruneEventLog(revision);

    return { revision, duplicate: false };
  }

  private foldTaskEvent(body: TaskEventBody): void {
    const prev = this.ctx.storage.sql.exec<TaskRow>("SELECT * FROM tasks WHERE task_id = ?", body.task_id).toArray()[0];

    const state = body.kind === "done" ? "done" : body.kind === "failed" ? "failed" : "running";

    const title = body.title && body.title.length > 0 ? body.title : (prev?.title ?? null);

    let percent = prev?.percent ?? null;
    if (state === "running" && body.percent !== undefined) percent = body.percent;
    else if (body.kind === "done") percent = 100;
    // failed: keep last value (no assignment)

    const step = body.kind === "done" || body.kind === "failed" ? null : (body.step ?? prev?.step ?? null);
    const message = body.kind === "done" || body.kind === "failed" ? (body.message ?? null) : null;
    const display = body.display ? JSON.stringify(body.display) : (prev?.display ?? null);

    this.ctx.storage.sql.exec(
      `INSERT INTO tasks (task_id, device_id, title, state, percent, step, message, updated_at, display)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(task_id) DO UPDATE SET
         device_id = excluded.device_id, title = excluded.title, state = excluded.state,
         percent = excluded.percent, step = excluded.step, message = excluded.message,
         updated_at = excluded.updated_at, display = excluded.display`,
      body.task_id,
      body.device_id,
      title,
      state,
      percent,
      step,
      message,
      body.occurred_at,
      display,
    );
  }

  private foldMessageEvent(body: MessageEventBody, revision: number): void {
    this.ctx.storage.sql.exec(
      `INSERT INTO messages (message_id, device_id, level, text, occurred_at, revision) VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(message_id) DO NOTHING`,
      body.message_id,
      body.device_id,
      body.level,
      body.text,
      body.occurred_at,
      revision,
    );
    // Bounded window (SPEC.md section 6.4, N=MESSAGE_WINDOW): keep only the
    // most recent N rows. No-op while fewer than N rows exist (subquery
    // returns NULL, so the DELETE matches nothing).
    this.ctx.storage.sql.exec(
      `DELETE FROM messages WHERE revision < (
         SELECT revision FROM messages ORDER BY revision DESC LIMIT 1 OFFSET ?
       )`,
      MESSAGE_WINDOW - 1,
    );
  }

  private foldConfigEvent(body: ConfigEventBody): void {
    if (body.kind === "automation.upserted" && body.automation) {
      const a = body.automation;
      this.ctx.storage.sql.exec(
        `INSERT INTO automations (automation_id, name, executor_kind, every_seconds, state, last_run_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(automation_id) DO UPDATE SET
           name = excluded.name, executor_kind = excluded.executor_kind, every_seconds = excluded.every_seconds,
           state = excluded.state, last_run_at = excluded.last_run_at`,
        a.automation_id,
        a.name,
        a.executor_kind,
        a.schedule.every_seconds,
        a.state,
        a.last_run_at ?? null,
      );
    } else if (body.kind === "automation.removed") {
      this.ctx.storage.sql.exec(`DELETE FROM automations WHERE automation_id = ?`, body.automation_id);
    }
  }

  private readAutomation(id: string): AutomationState | null {
    const row = this.ctx.storage.sql.exec<AutomationRow>("SELECT * FROM automations WHERE automation_id = ?", id).toArray()[0];
    return row ? rowToAutomationState(row) : null;
  }

  private getRevision(): number {
    const row = this.ctx.storage.sql.exec<{ value: string }>("SELECT value FROM space_meta WHERE key = 'revision'").toArray()[0];
    return row ? Number(row.value) : 0;
  }

  private setRevision(n: number): void {
    this.ctx.storage.sql.exec(
      `INSERT INTO space_meta (key, value) VALUES ('revision', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      String(n),
    );
  }

  private pruneEventLog(currentRevision: number): void {
    const floor = currentRevision - EVENT_LOG_RETENTION_REVISIONS;
    if (floor > 0) this.ctx.storage.sql.exec(`DELETE FROM event_log WHERE revision <= ?`, floor);
  }

  private broadcastDelta(delta: DeltaBody): void {
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment();
      if (!isConnAttachment(att) || att.role !== "viewer" || !att.deltaEligible) continue;
      if (!this.deviceHasActiveLease(att.deviceId)) continue;
      this.send(ws, "delta", delta);
    }
  }

  // ---- metric.frame (best-effort, never persisted, never revisioned) ----

  private handleMetricFrame(ws: WebSocket, att: ConnAttachment, body: MetricFrameBody, envelopeId: string): void {
    if (body.device_id !== att.deviceId) {
      this.reply(ws, "unauthorized", "device_id does not match the authenticated identity", envelopeId);
      return;
    }
    const limiter = this.rateLimiterFor(ws);
    const now = Date.now();
    limiter.frameTimestamps = limiter.frameTimestamps.filter((t) => now - t < 1000);
    if (limiter.frameTimestamps.length >= METRIC_FRAME_RATE_PER_SEC) {
      this.reply(ws, "rate_limited", "metric.frame exceeded 10/s on this connection", envelopeId);
      return;
    }
    limiter.frameTimestamps.push(now);

    const accepted: MetricSample[] = [];
    for (const sample of body.metrics) {
      // SPEC.md section 12: discard any sample whose ts <= the last-applied
      // ts for that metric_id. The baseline is SPACE-level (metricsCache,
      // keyed by metric_id alone), NOT per connection: staleness must be
      // judged across connections and across devices, or a reconnecting or
      // second source could replay an older sample past a fresher one.
      // Same lifetime as the rest of metricsCache — reset after eviction
      // is acceptable (the whole cache is best-effort, section 6.2).
      const cached = this.metricsCache.get(sample.metric_id);
      if (cached && sample.ts <= cached.ts) continue;
      this.touchMetricCache(sample.metric_id, sample);
      accepted.push(sample);
    }
    if (accepted.length > 0) this.broadcastMetricFrame({ device_id: body.device_id, metrics: accepted });
    // Deliberately no this.ctx.storage.sql.exec(...) anywhere in this
    // method and no revision change — see the class-level cost notes and
    // docs/design/realtime-server.md.
  }

  /** Upserts one metric_id into metricsCache, enforcing
   * METRIC_CACHE_MAX_METRICS with least-recently-updated eviction. Always
   * deletes before re-setting (even for an existing key) so the entry
   * moves to the end of Map iteration order — that's what makes "the
   * first key in iteration order" equivalent to "the least recently
   * updated key" below. */
  private touchMetricCache(metricId: string, sample: MetricSample): void {
    this.metricsCache.delete(metricId);
    if (this.metricsCache.size >= METRIC_CACHE_MAX_METRICS) {
      const oldest = this.metricsCache.keys().next().value;
      if (oldest !== undefined) this.metricsCache.delete(oldest);
    }
    this.metricsCache.set(metricId, sample);
  }

  private broadcastMetricFrame(body: MetricFrameBody): void {
    const now = Date.now();
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment();
      if (!isConnAttachment(att) || att.role !== "viewer" || !att.helloDone) continue;
      if (!this.deviceHasMetricInterest(att.deviceId, now)) continue;
      this.send(ws, "metric.frame", body);
    }
  }

  private rateLimiterFor(ws: WebSocket): RateLimiterState {
    let state = this.rateLimiters.get(ws);
    if (!state) {
      state = { frameTimestamps: [] };
      this.rateLimiters.set(ws, state);
    }
    return state;
  }

  // ---- interest lease ----

  private handleSubscribe(ws: WebSocket, att: ConnAttachment, body: SubscribeBody, envelopeId: string): void {
    const expiresAt = this.upsertLease(att.deviceId, body.topics ?? []);
    att.subscribedThisConn = true;
    ws.serializeAttachment(att);
    this.send(ws, "ack", { in_reply_to: envelopeId, lease: { expires_at: expiresAt } });
    this.reconcileLeaseEdge();
  }

  private handleUnsubscribe(ws: WebSocket, att: ConnAttachment, envelopeId: string): void {
    this.ctx.storage.sql.exec(`DELETE FROM leases WHERE device_id = ?`, att.deviceId);
    this.send(ws, "ack", { in_reply_to: envelopeId });
    this.reconcileLeaseEdge();
  }

  private handleInterestRenew(ws: WebSocket, att: ConnAttachment, body: InterestRenewBody, envelopeId: string): void {
    // SPEC.md section 7: interest.renew always wholesale-replaces topics,
    // and if no active lease exists it behaves exactly like a fresh
    // subscribe — the upsert below does both without special-casing.
    const expiresAt = this.upsertLease(att.deviceId, body.topics ?? []);
    this.send(ws, "ack", { in_reply_to: envelopeId, lease: { expires_at: expiresAt } });
    this.reconcileLeaseEdge();
  }

  private upsertLease(deviceId: string, topics: string[]): number {
    const expiresAt = Date.now() + LEASE_DEFAULT_MS;
    this.ctx.storage.sql.exec(
      `INSERT INTO leases (device_id, expires_at, topics) VALUES (?, ?, ?)
       ON CONFLICT(device_id) DO UPDATE SET expires_at = excluded.expires_at, topics = excluded.topics`,
      deviceId,
      expiresAt,
      JSON.stringify(topics),
    );
    return expiresAt;
  }

  private deviceHasActiveLease(deviceId: string): boolean {
    const row = this.ctx.storage.sql.exec<{ expires_at: number }>("SELECT expires_at FROM leases WHERE device_id = ?", deviceId).toArray()[0];
    return !!row && row.expires_at > Date.now();
  }

  private deviceHasMetricInterest(deviceId: string, now: number): boolean {
    const row = this.ctx.storage.sql
      .exec<{ expires_at: number; topics: string }>("SELECT expires_at, topics FROM leases WHERE device_id = ?", deviceId)
      .toArray()[0];
    if (!row || row.expires_at <= now) return false;
    const topics: string[] = JSON.parse(row.topics);
    return topics.length === 0 || topics.includes("metric");
  }

  /** Lazy edge-detection (SPEC.md section 7 permits lazy lease expiry): run
   * after every lease mutation and every reliable-event application so a
   * lapsed lease is never counted active, without any timer/alarm. */
  private reconcileLeaseEdge(): void {
    const now = Date.now();
    const activeNow = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) as n FROM leases WHERE expires_at > ?", now).toArray()[0].n;
    const prevRow = this.ctx.storage.sql.exec<{ value: string }>("SELECT value FROM space_meta WHERE key = 'lease_active_count'").toArray()[0];
    const prev = prevRow ? Number(prevRow.value) : 0;
    if (activeNow === prev) return;
    this.ctx.storage.sql.exec(
      `INSERT INTO space_meta (key, value) VALUES ('lease_active_count', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      String(activeNow),
    );
    if (prev > 0 && activeNow === 0) this.broadcastServerCommand("throttle");
    else if (prev === 0 && activeNow > 0) this.broadcastServerCommand("resume_rate");
  }

  private broadcastServerCommand(action: "throttle" | "resume_rate"): void {
    const body: CommandBody = { command_id: crypto.randomUUID(), origin: "server", action, ttl_ms: 60_000 };
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment();
      if (!isConnAttachment(att) || att.role !== "source" || !att.helloDone) continue;
      this.send(ws, "command", body);
    }
  }

  /** Unicast the space's CURRENT interest state to one freshly-connected
   * source (see the call site in handlePreHello for why edges alone are
   * not enough). Counts only unexpired leases, same rule as
   * reconcileLeaseEdge. */
  private sendCurrentRateState(ws: WebSocket): void {
    const active = this.ctx.storage.sql
      .exec<{ n: number }>("SELECT COUNT(*) as n FROM leases WHERE expires_at > ?", Date.now())
      .toArray()[0].n;
    const body: CommandBody = {
      command_id: crypto.randomUUID(),
      origin: "server",
      action: active > 0 ? "resume_rate" : "throttle",
      ttl_ms: 60_000,
    };
    this.send(ws, "command", body);
  }

  // ---- command relay (viewer-issued pause/resume/stop/run_now) ----

  private handleCommand(ws: WebSocket, att: ConnAttachment, body: CommandBody, envelopeTs: number, envelopeId: string): void {
    if (body.issued_by_device_id !== att.deviceId) {
      this.reply(ws, "unauthorized", "issued_by_device_id must match the authenticated device", envelopeId);
      return;
    }
    if (Date.now() > envelopeTs + body.ttl_ms) {
      this.reply(ws, "command_expired", "command TTL elapsed before relay", envelopeId);
      return;
    }

    const targets = body.target_device_id ? this.ctx.getWebSockets(body.target_device_id) : this.ctx.getWebSockets();
    let delivered = false;
    for (const target of targets) {
      const targetAtt = target.deserializeAttachment();
      if (!isConnAttachment(targetAtt) || targetAtt.role !== "source" || !targetAtt.helloDone) continue;
      if (body.target_device_id && targetAtt.deviceId !== body.target_device_id) continue;
      // Relay rules (SPEC.md section 8): preserve the original ts and
      // command_id, only the envelope id is fresh.
      this.sendRaw(target, { type: "command", id: newEnvelopeId(), ts: envelopeTs, body });
      delivered = true;
    }
    if (!delivered) {
      this.ctx.storage.sql.exec(
        `INSERT INTO pending_commands (command_id, target_device_id, origin_ts, ttl_ms, payload, delivered)
         VALUES (?, ?, ?, ?, ?, 0) ON CONFLICT(command_id) DO NOTHING`,
        body.command_id,
        body.target_device_id ?? null,
        envelopeTs,
        body.ttl_ms,
        JSON.stringify(body),
      );
    }
  }

  /** Best-effort redelivery of commands that missed a disconnected source
   * (SPEC.md section 14: command is "persisted... until delivered or
   * expired"). Runs once a source completes hello on a fresh connection. */
  private drainPendingCommands(ws: WebSocket, deviceId: string): void {
    const now = Date.now();
    const rows = this.ctx.storage.sql
      .exec<{ command_id: string; origin_ts: number; ttl_ms: number; payload: string }>(
        `SELECT command_id, origin_ts, ttl_ms, payload FROM pending_commands WHERE delivered = 0 AND (target_device_id = ? OR target_device_id IS NULL)`,
        deviceId,
      )
      .toArray();
    for (const row of rows) {
      if (now > row.origin_ts + row.ttl_ms) {
        this.ctx.storage.sql.exec(`DELETE FROM pending_commands WHERE command_id = ?`, row.command_id);
        continue;
      }
      const body = JSON.parse(row.payload) as CommandBody;
      this.sendRaw(ws, { type: "command", id: newEnvelopeId(), ts: row.origin_ts, body });
      this.ctx.storage.sql.exec(`UPDATE pending_commands SET delivered = 1 WHERE command_id = ?`, row.command_id);
    }
  }

  // ---- send helpers ----

  private send<B>(ws: WebSocket, type: MessageType, body: B): void {
    ws.send(JSON.stringify(makeEnvelope(type, body)));
  }

  private sendRaw(ws: WebSocket, envelope: AnyEnvelope): void {
    ws.send(JSON.stringify(envelope));
  }

  private reply(ws: WebSocket, code: ErrorCode, message: string, inReplyTo: string | undefined): void {
    this.sendRaw(ws, makeStandardError(code, message, inReplyTo));
    this.logAlways("protocol_error", { code, message });
  }

  private logHotPath(event: string, data: Record<string, unknown>): void {
    this.hotPathCounter++;
    if (this.logSampler(this.hotPathCounter)) {
      console.log(JSON.stringify({ level: "info", event, ...data, ts: Date.now() }));
    }
  }

  private logAlways(event: string, data: Record<string, unknown>): void {
    console.log(JSON.stringify({ level: "error", event, ...data, ts: Date.now() }));
    this.securityEventLog.push({ event, data });
    // Bounded so a pathological run can't grow this without limit; recent
    // history is all tests need, and this is never persisted or read by
    // production logic.
    if (this.securityEventLog.length > 200) this.securityEventLog.shift();
  }
}

// Referenced only for the protocol_version constant it advertises via
// hello{stage:accept}; kept here so the export is exercised by typecheck.
export const SPACE_HUB_PROTOCOL_VERSION = PROTOCOL_VERSION;
