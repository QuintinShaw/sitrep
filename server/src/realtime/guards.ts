// Lightweight, hand-written runtime validation for the realtime protocol.
// Deliberately NOT ajv: proto/realtime/tools/validate.js is the fixture
// conformance tool (its own package, pinned ajv) and is the authority for
// "does this fixture match the schema". This module is the production-path
// guard the server actually runs on every inbound frame — it mirrors the
// same rules (proto/realtime/*.schema.json) by hand, deliberately narrow in
// scope (no generic JSON Schema engine as a production dependency).
//
// Per SPEC.md section 15, the envelope TOP LEVEL is permanently strict
// (unknown top-level field is always malformed) while `body` tolerates
// unknown fields in future minor versions — these guards enforce the exact
// v1.0.0 body shapes (matching the frozen schemas byte for byte, same as
// the fixtures expect), which is stricter than the runtime-tolerance clause
// permits for a hypothetical v1.1 peer. That's an accepted, documented
// trade: see docs/design/realtime-server.md.

import {
  MESSAGE_TYPES,
  type AckBody,
  type AnyEnvelope,
  type AutomationState,
  type CommandBody,
  type ConfigEventBody,
  type DeviceRole,
  type ErrorBody,
  type ErrorCode,
  type HelloBody,
  type InterestRenewBody,
  type MessageEventBody,
  type MessageType,
  type MetricFrameBody,
  type MetricSample,
  type ResumeBody,
  type SubscribeBody,
  type TaskEventBody,
  type UnsubscribeBody,
} from "./types.ts";

export const FRAME_MAX_BYTES = 64 * 1024;

const ENVELOPE_ID_RE = /^[A-Za-z0-9_-]{1,64}$/;
const DEVICE_ID_RE = /^[A-Za-z0-9_-]{1,128}$/;
const METRIC_ID_RE = /^[a-z0-9_.-]{1,64}$/;

export type ValidationResult<T> = { ok: true; value: T } | { ok: false; code: ErrorCode; message: string };

const ok = <T>(value: T): ValidationResult<T> => ({ ok: true, value });
const fail = <T>(code: ErrorCode, message: string): ValidationResult<T> => ({ ok: false, code, message });

export type ParseResult =
  | { kind: "ok"; envelope: AnyEnvelope }
  | { kind: "unknown_type"; type: string; id?: string }
  | { kind: "error"; code: ErrorCode; message: string; inReplyTo?: string };

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

export function isUnixMsTimestamp(v: unknown): v is number {
  return typeof v === "number" && Number.isInteger(v) && v >= 1_000_000_000_000 && v <= 9_999_999_999_999;
}

function isFreeText(v: unknown): v is string {
  return typeof v === "string" && v.length <= 2048;
}

function isLabelText(v: unknown): v is string {
  return typeof v === "string" && v.length <= 256;
}

function isDeviceId(v: unknown): v is string {
  return typeof v === "string" && DEVICE_ID_RE.test(v);
}

function isEnvelopeId(v: unknown): v is string {
  return typeof v === "string" && ENVELOPE_ID_RE.test(v);
}

function hasOnlyKeys(obj: Record<string, unknown>, allowed: readonly string[]): boolean {
  return Object.keys(obj).every((k) => allowed.includes(k));
}

function validateDisplayHints(v: unknown): boolean {
  if (v === undefined) return true;
  if (!isPlainObject(v)) return false;
  if (!hasOnlyKeys(v, ["icon", "tint", "template"])) return false;
  if (v.icon !== undefined && !(typeof v.icon === "string" && v.icon.length <= 64)) return false;
  if (v.tint !== undefined && !(typeof v.tint === "string" && v.tint.length <= 32)) return false;
  if (v.template !== undefined && !(typeof v.template === "string" && v.template.length <= 32)) return false;
  return true;
}

function validateMetricSample(v: unknown): v is MetricSample {
  if (!isPlainObject(v)) return false;
  if (!hasOnlyKeys(v, ["metric_id", "value", "label", "ts", "display", "target", "min", "max", "alert_above", "alert_below"])) return false;
  if (typeof v.metric_id !== "string" || !METRIC_ID_RE.test(v.metric_id)) return false;
  if (typeof v.value !== "string" || v.value.length > 256) return false;
  if (!isUnixMsTimestamp(v.ts)) return false;
  if (v.label !== undefined && !isLabelText(v.label)) return false;
  if (!validateDisplayHints(v.display)) return false;
  for (const key of ["target", "min", "max", "alert_above", "alert_below"] as const) {
    if (v[key] !== undefined && !(typeof v[key] === "string" && (v[key] as string).length <= 64)) return false;
  }
  return true;
}

export function validateAutomationState(v: unknown): v is AutomationState {
  if (!isPlainObject(v)) return false;
  if (!hasOnlyKeys(v, ["automation_id", "name", "executor_kind", "schedule", "state", "last_run_at"])) return false;
  if (typeof v.automation_id !== "string" || v.automation_id.length < 1 || v.automation_id.length > 128) return false;
  if (typeof v.name !== "string" || v.name.length < 1 || v.name.length > 256) return false;
  if (!["script", "agent", "hybrid"].includes(v.executor_kind as string)) return false;
  const schedule = v.schedule;
  if (!isPlainObject(schedule) || schedule.kind !== "interval" || typeof schedule.every_seconds !== "number" || schedule.every_seconds < 1) {
    return false;
  }
  if (!["active", "paused"].includes(v.state as string)) return false;
  if (v.last_run_at !== undefined && !isUnixMsTimestamp(v.last_run_at)) return false;
  return true;
}

// ---- per-type body validators ----

function validateHelloBody(body: Record<string, unknown>): ValidationResult<HelloBody> {
  if (body.stage === "offer") {
    if (!hasOnlyKeys(body, ["stage", "device_id", "role", "protocol_versions", "capabilities"])) {
      return fail("malformed", "hello offer: unexpected field");
    }
    if (!isDeviceId(body.device_id)) return fail("malformed", "hello offer: invalid device_id");
    if (body.role !== "source" && body.role !== "viewer") return fail("malformed", "hello offer: invalid role");
    if (!Array.isArray(body.protocol_versions) || body.protocol_versions.length < 1) {
      return fail("malformed", "hello offer: protocol_versions must be non-empty");
    }
    if (!body.protocol_versions.every((n) => Number.isInteger(n) && (n as number) >= 1)) {
      return fail("malformed", "hello offer: invalid protocol_versions entry");
    }
    if (body.capabilities !== undefined) {
      if (!Array.isArray(body.capabilities) || !body.capabilities.every((c) => typeof c === "string" && c.length <= 64)) {
        return fail("malformed", "hello offer: invalid capabilities");
      }
    }
    return ok({
      stage: "offer",
      device_id: body.device_id,
      role: body.role,
      protocol_versions: body.protocol_versions as number[],
      capabilities: body.capabilities as string[] | undefined,
    });
  }
  if (body.stage === "accept") {
    if (!hasOnlyKeys(body, ["stage", "protocol_version", "session_id", "heartbeat_interval_ms", "capabilities"])) {
      return fail("malformed", "hello accept: unexpected field");
    }
    if (!Number.isInteger(body.protocol_version) || (body.protocol_version as number) < 1) {
      return fail("malformed", "hello accept: invalid protocol_version");
    }
    if (typeof body.session_id !== "string" || body.session_id.length < 1 || body.session_id.length > 64) {
      return fail("malformed", "hello accept: invalid session_id");
    }
    if (!Number.isInteger(body.heartbeat_interval_ms) || (body.heartbeat_interval_ms as number) < 1000 || (body.heartbeat_interval_ms as number) > 300000) {
      return fail("malformed", "hello accept: invalid heartbeat_interval_ms");
    }
    return ok({
      stage: "accept",
      protocol_version: body.protocol_version as number,
      session_id: body.session_id,
      heartbeat_interval_ms: body.heartbeat_interval_ms as number,
      capabilities: body.capabilities as string[] | undefined,
    });
  }
  return fail("malformed", "hello: invalid stage");
}

function validateResumeBody(body: Record<string, unknown>): ValidationResult<ResumeBody> {
  if (!hasOnlyKeys(body, ["last_revision"])) return fail("malformed", "resume: unexpected field");
  if (!Number.isInteger(body.last_revision) || (body.last_revision as number) < 0) {
    return fail("malformed", "resume: last_revision must be a non-negative integer");
  }
  return ok({ last_revision: body.last_revision as number });
}

function validateAckBody(body: Record<string, unknown>): ValidationResult<AckBody> {
  if (!hasOnlyKeys(body, ["acked", "in_reply_to", "lease"])) return fail("malformed", "ack: unexpected field");
  if (body.acked === undefined && body.in_reply_to === undefined) {
    return fail("malformed", "ack: at least one of acked or in_reply_to required");
  }
  if (body.acked !== undefined) {
    if (!Array.isArray(body.acked) || body.acked.length < 1) return fail("malformed", "ack: acked must be non-empty array");
    for (const pair of body.acked) {
      if (!isPlainObject(pair) || !hasOnlyKeys(pair, ["device_id", "device_seq"])) return fail("malformed", "ack: invalid acked entry");
      if (!isDeviceId(pair.device_id)) return fail("malformed", "ack: invalid device_id in acked");
      if (!Number.isInteger(pair.device_seq) || (pair.device_seq as number) < 1) return fail("malformed", "ack: invalid device_seq in acked");
    }
  }
  if (body.in_reply_to !== undefined && !isEnvelopeId(body.in_reply_to)) return fail("malformed", "ack: invalid in_reply_to");
  if (body.lease !== undefined) {
    if (!isPlainObject(body.lease) || !hasOnlyKeys(body.lease, ["expires_at"]) || !isUnixMsTimestamp(body.lease.expires_at)) {
      return fail("malformed", "ack: invalid lease");
    }
  }
  return ok(body as unknown as AckBody);
}

function validateTaskEventBody(body: Record<string, unknown>): ValidationResult<TaskEventBody> {
  const allowed = ["device_id", "device_seq", "task_id", "kind", "occurred_at", "title", "percent", "step", "message", "display"];
  if (!hasOnlyKeys(body, allowed)) return fail("malformed", "task.event: unexpected field");
  if (!isDeviceId(body.device_id)) return fail("malformed", "task.event: invalid device_id");
  if (!Number.isInteger(body.device_seq) || (body.device_seq as number) < 1) return fail("malformed", "task.event: invalid device_seq");
  if (typeof body.task_id !== "string" || body.task_id.length < 1 || body.task_id.length > 256) return fail("malformed", "task.event: invalid task_id");
  if (!["started", "progress", "step", "done", "failed"].includes(body.kind as string)) return fail("malformed", "task.event: invalid kind");
  if (!isUnixMsTimestamp(body.occurred_at)) return fail("malformed", "task.event: invalid occurred_at");
  if (body.title !== undefined && !isFreeText(body.title)) return fail("malformed", "task.event: invalid title");
  if (body.step !== undefined && !isFreeText(body.step)) return fail("malformed", "task.event: invalid step");
  if (body.message !== undefined && !isFreeText(body.message)) return fail("malformed", "task.event: invalid message");
  if (body.percent !== undefined && !(Number.isInteger(body.percent) && (body.percent as number) >= 0 && (body.percent as number) <= 100)) {
    return fail("malformed", "task.event: invalid percent");
  }
  if (!validateDisplayHints(body.display)) return fail("malformed", "task.event: invalid display");
  if (body.kind === "progress" && body.percent === undefined) return fail("malformed", "task.event: progress requires percent");
  return ok(body as unknown as TaskEventBody);
}

function validateMessageEventBody(body: Record<string, unknown>): ValidationResult<MessageEventBody> {
  const allowed = ["device_id", "device_seq", "message_id", "level", "text", "occurred_at", "automation_id"];
  if (!hasOnlyKeys(body, allowed)) return fail("malformed", "message.event: unexpected field");
  if (!isDeviceId(body.device_id)) return fail("malformed", "message.event: invalid device_id");
  if (!Number.isInteger(body.device_seq) || (body.device_seq as number) < 1) return fail("malformed", "message.event: invalid device_seq");
  if (typeof body.message_id !== "string" || body.message_id.length < 1 || body.message_id.length > 128) {
    return fail("malformed", "message.event: invalid message_id");
  }
  if (!["info", "warn", "error"].includes(body.level as string)) return fail("malformed", "message.event: invalid level");
  if (!isFreeText(body.text)) return fail("malformed", "message.event: invalid text");
  if (!isUnixMsTimestamp(body.occurred_at)) return fail("malformed", "message.event: invalid occurred_at");
  if (body.automation_id !== undefined && !(typeof body.automation_id === "string" && body.automation_id.length <= 128)) {
    return fail("malformed", "message.event: invalid automation_id");
  }
  return ok(body as unknown as MessageEventBody);
}

function validateMetricFrameBody(body: Record<string, unknown>): ValidationResult<MetricFrameBody> {
  if (!hasOnlyKeys(body, ["device_id", "metrics"])) return fail("malformed", "metric.frame: unexpected field");
  if (!isDeviceId(body.device_id)) return fail("malformed", "metric.frame: invalid device_id");
  if (!Array.isArray(body.metrics) || body.metrics.length < 1) return fail("malformed", "metric.frame: metrics must be non-empty array");
  if (body.metrics.length > 64) return fail("batch_too_large", "metric.frame: metrics exceeds 64 items");
  for (const m of body.metrics) {
    if (!validateMetricSample(m)) return fail("malformed", "metric.frame: invalid metric sample");
  }
  return ok(body as unknown as MetricFrameBody);
}

function validateConfigEventBody(body: Record<string, unknown>): ValidationResult<ConfigEventBody> {
  if (!hasOnlyKeys(body, ["kind", "automation_id", "automation", "occurred_at"])) return fail("malformed", "config.event: unexpected field");
  if (!["automation.upserted", "automation.removed"].includes(body.kind as string)) return fail("malformed", "config.event: invalid kind");
  if (typeof body.automation_id !== "string" || body.automation_id.length < 1 || body.automation_id.length > 128) {
    return fail("malformed", "config.event: invalid automation_id");
  }
  if (!isUnixMsTimestamp(body.occurred_at)) return fail("malformed", "config.event: invalid occurred_at");
  if (body.kind === "automation.upserted" && !validateAutomationState(body.automation)) {
    return fail("malformed", "config.event: automation.upserted requires a valid automation");
  }
  return ok(body as unknown as ConfigEventBody);
}

const SUBSCRIBE_TOPICS = ["task", "metric", "message"];

function validateSubscribeLikeBody(body: Record<string, unknown>, label: string): ValidationResult<SubscribeBody> {
  if (!hasOnlyKeys(body, ["topics"])) return fail("malformed", `${label}: unexpected field`);
  if (body.topics !== undefined) {
    if (!Array.isArray(body.topics) || !body.topics.every((t) => SUBSCRIBE_TOPICS.includes(t as string))) {
      return fail("malformed", `${label}: invalid topics`);
    }
  }
  return ok(body as unknown as SubscribeBody);
}

function validateUnsubscribeBody(body: Record<string, unknown>): ValidationResult<UnsubscribeBody> {
  if (Object.keys(body).length > 0) return fail("malformed", "unsubscribe: body must be empty");
  return ok({} as UnsubscribeBody);
}

function validateCommandBody(body: Record<string, unknown>): ValidationResult<CommandBody> {
  const allowed = ["command_id", "origin", "issued_by_device_id", "action", "task_id", "automation_id", "target_device_id", "ttl_ms", "params"];
  if (!hasOnlyKeys(body, allowed)) return fail("malformed", "command: unexpected field");
  if (typeof body.command_id !== "string" || body.command_id.length < 1 || body.command_id.length > 64) {
    return fail("malformed", "command: invalid command_id");
  }
  if (body.origin !== "viewer" && body.origin !== "server") return fail("malformed", "command: invalid origin");
  if (!["pause", "resume", "stop", "run_now", "throttle", "resume_rate"].includes(body.action as string)) {
    return fail("malformed", "command: invalid action");
  }
  if (!Number.isInteger(body.ttl_ms) || (body.ttl_ms as number) < 1 || (body.ttl_ms as number) > 86_400_000) {
    return fail("malformed", "command: invalid ttl_ms");
  }
  if (body.issued_by_device_id !== undefined && !isDeviceId(body.issued_by_device_id)) {
    return fail("malformed", "command: invalid issued_by_device_id");
  }
  if (body.target_device_id !== undefined && !isDeviceId(body.target_device_id)) {
    return fail("malformed", "command: invalid target_device_id");
  }
  if (body.task_id !== undefined && !(typeof body.task_id === "string" && body.task_id.length >= 1 && body.task_id.length <= 256)) {
    return fail("malformed", "command: invalid task_id");
  }
  if (body.automation_id !== undefined && !(typeof body.automation_id === "string" && body.automation_id.length >= 1 && body.automation_id.length <= 128)) {
    return fail("malformed", "command: invalid automation_id");
  }
  if (body.params !== undefined && !isPlainObject(body.params)) return fail("malformed", "command: invalid params");

  const action = body.action as string;
  if (action === "pause" || action === "resume" || action === "stop") {
    if (body.origin !== "viewer") return fail("malformed", "command: pause/resume/stop require origin viewer");
    if (body.issued_by_device_id === undefined) return fail("malformed", "command: pause/resume/stop require issued_by_device_id");
    if (body.task_id === undefined) return fail("malformed", "command: pause/resume/stop require task_id");
    if (body.automation_id !== undefined) return fail("malformed", "command: pause/resume/stop must not carry automation_id");
  } else if (action === "run_now") {
    if (body.origin !== "viewer") return fail("malformed", "command: run_now requires origin viewer");
    if (body.issued_by_device_id === undefined) return fail("malformed", "command: run_now requires issued_by_device_id");
    if (body.automation_id === undefined) return fail("malformed", "command: run_now requires automation_id");
    if (body.task_id !== undefined) return fail("malformed", "command: run_now must not carry task_id");
  } else {
    // throttle | resume_rate
    if (body.origin !== "server") return fail("malformed", "command: throttle/resume_rate require origin server");
    if (body.task_id !== undefined || body.automation_id !== undefined) {
      return fail("malformed", "command: throttle/resume_rate must not carry task_id or automation_id");
    }
  }
  return ok(body as unknown as CommandBody);
}

const ERROR_CODES: readonly ErrorCode[] = [
  "version_unsupported",
  "hello_required",
  "unauthenticated",
  "unauthorized",
  "malformed",
  "rate_limited",
  "frame_too_large",
  "batch_too_large",
  "revision_unavailable",
  "command_expired",
  "sequence_gap",
  "superseded",
  "internal_error",
];

function validateErrorBody(body: Record<string, unknown>): ValidationResult<ErrorBody> {
  if (!hasOnlyKeys(body, ["code", "message", "in_reply_to", "retryable", "fatal"])) return fail("malformed", "error: unexpected field");
  if (!ERROR_CODES.includes(body.code as ErrorCode)) return fail("malformed", "error: invalid code");
  if (typeof body.message !== "string" || body.message.length > 500) return fail("malformed", "error: invalid message");
  if (typeof body.retryable !== "boolean") return fail("malformed", "error: retryable required");
  if (typeof body.fatal !== "boolean") return fail("malformed", "error: fatal required");
  if (body.in_reply_to !== undefined && !isEnvelopeId(body.in_reply_to)) return fail("malformed", "error: invalid in_reply_to");
  return ok(body as unknown as ErrorBody);
}

function validateBody(type: MessageType, body: Record<string, unknown>): ValidationResult<unknown> {
  switch (type) {
    case "hello":
      return validateHelloBody(body);
    case "resume":
      return validateResumeBody(body);
    case "ack":
      return validateAckBody(body);
    case "task.event":
      return validateTaskEventBody(body);
    case "message.event":
      return validateMessageEventBody(body);
    case "metric.frame":
      return validateMetricFrameBody(body);
    case "config.event":
      return validateConfigEventBody(body);
    case "subscribe":
      return validateSubscribeLikeBody(body, "subscribe");
    case "unsubscribe":
      return validateUnsubscribeBody(body);
    case "interest.renew":
      return validateSubscribeLikeBody(body, "interest.renew") as ValidationResult<InterestRenewBody>;
    case "command":
      return validateCommandBody(body);
    case "error":
      return validateErrorBody(body);
    case "snapshot":
    case "delta":
      // Server-only message types: any client-sent instance is rejected by
      // the authorization layer (authorizeClientEnvelope), not here. We
      // still validate shape for the server's own outbound round-trip test.
      return ok(body);
    default:
      return fail("malformed", `unhandled type ${type as string}`);
  }
}

/** Parses one raw text frame. Returns "unknown_type" (never "malformed")
 * for a `type` this version doesn't recognize (SPEC.md section 15) — the
 * caller MUST ignore such frames rather than raising an error. */
export function parseEnvelope(raw: string): ParseResult {
  const byteLength = new TextEncoder().encode(raw).length;
  if (byteLength > FRAME_MAX_BYTES) {
    return { kind: "error", code: "frame_too_large", message: "frame exceeds 64 KiB" };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { kind: "error", code: "malformed", message: "invalid JSON" };
  }
  if (!isPlainObject(parsed)) return { kind: "error", code: "malformed", message: "envelope must be an object" };
  if (!hasOnlyKeys(parsed, ["type", "id", "ts", "body"])) {
    return { kind: "error", code: "malformed", message: "envelope carries an unexpected top-level field" };
  }
  const { type, id, ts, body } = parsed;
  if (typeof type !== "string") return { kind: "error", code: "malformed", message: "type must be a string" };
  if (!isEnvelopeId(id)) return { kind: "error", code: "malformed", message: "invalid envelope id" };
  if (!isUnixMsTimestamp(ts)) return { kind: "error", code: "malformed", message: "invalid envelope ts" };
  if (!isPlainObject(body)) return { kind: "error", code: "malformed", message: "body must be an object" };
  if (!(MESSAGE_TYPES as readonly string[]).includes(type)) {
    return { kind: "unknown_type", type, id };
  }
  const result = validateBody(type as MessageType, body);
  if (!result.ok) return { kind: "error", code: result.code, message: result.message, inReplyTo: id };
  return { kind: "ok", envelope: { type, id, ts, body: result.value } as AnyEnvelope };
}

/** Encodes an envelope back to its wire JSON string. Used by the round-trip
 * fixture test (decode a valid fixture, encode it, decode again, compare). */
export function encodeEnvelope(envelope: AnyEnvelope): string {
  return JSON.stringify(envelope);
}

// ---- SPEC.md section 10.1 authorization matrix ----

export type AuthorizeResult = { ok: true } | { ok: false; code: "unauthorized" | "hello_required" };

const authOk: AuthorizeResult = { ok: true };
const authUnauthorized: AuthorizeResult = { ok: false, code: "unauthorized" };
const authHelloRequired: AuthorizeResult = { ok: false, code: "hello_required" };

/** Validates a client-originated envelope's `type` against the sending
 * connection's role, per SPEC.md section 10.1. Governs client-originated
 * envelopes ONLY — server-originated `command` (throttle/resume_rate) is
 * not subject to this matrix (SPEC.md section 8). */
export function authorizeClientEnvelope(role: DeviceRole, envelope: AnyEnvelope): AuthorizeResult {
  switch (envelope.type) {
    case "hello": {
      const body = envelope.body as HelloBody;
      if (body.stage === "accept") return authHelloRequired;
      return authOk;
    }
    case "resume":
    case "subscribe":
    case "unsubscribe":
    case "interest.renew":
      return role === "viewer" ? authOk : authUnauthorized;
    case "task.event":
    case "message.event":
    case "metric.frame":
    case "ack":
      return role === "source" ? authOk : authUnauthorized;
    case "command": {
      const body = envelope.body as CommandBody;
      if (role !== "viewer") return authUnauthorized;
      if (body.origin !== "viewer") return authUnauthorized;
      return authOk;
    }
    case "error":
      return authOk;
    case "snapshot":
    case "delta":
    case "config.event":
      return authUnauthorized;
    default:
      return authUnauthorized;
  }
}
