// Shared constants and envelope-construction helpers for the SpaceHub DO.
// Pure/no I/O so they are cheap to unit test directly.

import type {
  AnyEnvelope,
  Envelope,
  ErrorBody,
  ErrorCode,
  MessageType,
} from "./types.ts";

export const PROTOCOL_VERSION = 1;
export const SUPPORTED_PROTOCOL_VERSIONS = [1];

/** SPEC.md section 7: server MUST choose a lease duration between 30000 ms
 * and 60000 ms inclusive; 45000 ms is RECOMMENDED as a default. */
export const LEASE_MIN_MS = 30_000;
export const LEASE_MAX_MS = 60_000;
export const LEASE_DEFAULT_MS = 45_000;

/** Heartbeat cadence advertised in hello{stage:accept}. Informative only
 * (SPEC.md section 9.3); the server never depends on it for correctness. */
export const HEARTBEAT_INTERVAL_MS = 30_000;

/** Reliable-event retention window: how many trailing revisions the event
 * log keeps for incremental `resume` catch-up before a viewer is forced
 * onto a fresh `snapshot`. Documented normative choice (SPEC.md section
 * 6.2 permits any retention policy as long as gaps fall back to snapshot). */
export const EVENT_LOG_RETENTION_REVISIONS = 1000;

/** Bounded message history window carried in `snapshot.body.messages`
 * (SPEC.md section 6.4 recommends 200). */
export const MESSAGE_WINDOW = 200;

/** Soft ceiling used when packing snapshot/delta chunks, comfortably under
 * the 64 KiB hard frame limit (SPEC.md section 11) to leave room for
 * envelope/JSON overhead. */
export const CHUNK_SOFT_MAX_BYTES = 56 * 1024;

/** SPEC.md section 11: a connection MUST NOT emit more than 10
 * metric.frame envelopes per second. */
export const METRIC_FRAME_RATE_PER_SEC = 10;

/** SPEC.md section 12: 2 Hz ceiling per metric_id. */
export const METRIC_SAMPLE_MIN_INTERVAL_MS = 500;

/** Cardinality cap on `SpaceHub#metricsCache` (distinct `metric_id`s held
 * at once, per space). Without this, a compromised or misbehaving source
 * that rotates metric_ids without bound would grow DO memory unboundedly
 * until the instance crashes — the existing per-frame size/rate limits
 * (METRIC_FRAME_RATE_PER_SEC, the 64-samples-per-frame guard in
 * guards.ts) bound how fast the cache can grow but not how big it can
 * get. Beyond this cap, the least-recently-updated metric_id is evicted
 * to make room (see SpaceHub#touchMetricCache); eviction is safe per
 * SPEC.md section 6.2 (snapshot.metrics is a best-effort cache, evicted
 * metrics are simply absent from the next snapshot). */
export const METRIC_CACHE_MAX_METRICS = 256;

export function newEnvelopeId(): string {
  return crypto.randomUUID();
}

export function makeEnvelope<T extends MessageType, B>(type: T, body: B, ts = Date.now()): Envelope<T, B> {
  return { type, id: newEnvelopeId(), ts, body };
}

export function makeError(
  code: ErrorCode,
  message: string,
  opts: { retryable: boolean; fatal: boolean; inReplyTo?: string },
): AnyEnvelope {
  const body: ErrorBody = {
    code,
    message,
    retryable: opts.retryable,
    fatal: opts.fatal,
    ...(opts.inReplyTo ? { in_reply_to: opts.inReplyTo } : {}),
  };
  return makeEnvelope("error", body) as AnyEnvelope;
}

/** Fixed retryable/fatal semantics per SPEC.md section 13's error code
 * table, so call sites never have to repeat these pairings by hand. */
export const ERROR_SEMANTICS: Record<ErrorCode, { retryable: boolean; fatal: boolean }> = {
  version_unsupported: { retryable: false, fatal: true },
  hello_required: { retryable: true, fatal: true },
  unauthenticated: { retryable: false, fatal: true },
  unauthorized: { retryable: false, fatal: false },
  malformed: { retryable: true, fatal: false },
  rate_limited: { retryable: true, fatal: false },
  frame_too_large: { retryable: true, fatal: false },
  batch_too_large: { retryable: true, fatal: false },
  revision_unavailable: { retryable: true, fatal: false },
  command_expired: { retryable: false, fatal: false },
  sequence_gap: { retryable: false, fatal: false },
  superseded: { retryable: false, fatal: true },
  internal_error: { retryable: true, fatal: false },
};

export function makeStandardError(code: ErrorCode, message: string, inReplyTo?: string): AnyEnvelope {
  return makeError(code, message, { ...ERROR_SEMANTICS[code], inReplyTo });
}
