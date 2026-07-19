// Per-connection identity + handshake state, persisted via
// WebSocket#serializeAttachment so it survives Durable Object hibernation
// and eviction/reconstruction. Cloudflare caps a serialized attachment at
// 2 KiB — this shape stays a handful of short scalar fields specifically to
// stay far under that limit.
//
// The trust boundary: `deviceId`/`role` are set exactly once, in
// SpaceHub#fetch, from headers the Worker attaches AFTER it has already
// authenticated the connection's st2 token (see adapters/workers.ts). The
// DO never re-derives identity from a client-sent `hello` body (SPEC.md
// section 10) — it only ever reads this attachment.

import type { DeviceRole } from "./types.ts";

export interface ConnAttachment {
  deviceId: string;
  role: DeviceRole;
  connectedAt: number;
  /** True once this connection has completed the hello offer/accept
   * handshake (SPEC.md section 9.1). Every frame before that must be
   * answered with hello_required. */
  helloDone: boolean;
  /** True once this connection has sent `subscribe` (viewer only) — gates
   * the "resume must follow subscribe on the same connection" rule
   * (SPEC.md section 6.3). Not set by interest.renew. */
  subscribedThisConn: boolean;
  /** True once this connection has received a snapshot-or-delta reply to
   * `resume` (SPEC.md section 6.2/6.3) — the sole gate for receiving live
   * deltas. An error reply (e.g. revision_unavailable) never sets this. */
  deltaEligible: boolean;
  sessionId?: string;
}

export function isConnAttachment(v: unknown): v is ConnAttachment {
  if (typeof v !== "object" || v === null) return false;
  const a = v as Record<string, unknown>;
  return (
    typeof a.deviceId === "string" &&
    (a.role === "source" || a.role === "viewer") &&
    typeof a.connectedAt === "number" &&
    typeof a.helloDone === "boolean" &&
    typeof a.subscribedThisConn === "boolean" &&
    typeof a.deltaEligible === "boolean"
  );
}
