// Per-connection identity + handshake state, persisted via
// WebSocket#serializeAttachment so it survives Durable Object hibernation
// and eviction/reconstruction. Cloudflare caps a serialized attachment at
// 2 KiB — this shape stays a handful of short scalar fields specifically to
// stay far under that limit.
//
// The trust boundary: `deviceId`/`role` are set exactly once, in
// SpaceHub#fetch, from headers the Worker attaches AFTER it has already
// authenticated the connection's sr1 token (see adapters/workers.ts). The
// DO never re-derives identity from a client-sent `hello` body (SPEC.md
// section 10) — it only ever reads this attachment.

import type { DeviceRole } from "./types.ts";

export interface ConnAttachment {
  deviceId: string;
  /** The realtime-protocol role for THIS connection (SPEC.md §9.2): finalized
   * at `hello{stage:offer}` from the client's declared role, constrained by
   * `tokenRole` (P0-1). Before hello it holds a provisional value and is not
   * consulted. */
  role: DeviceRole;
  /** The token's HTTP role (owner/viewer/source), forwarded by the Worker as
   * `x-sitrep-role`. Constrains which WS `role` the client may declare: a
   * source-token → only `source`, a viewer-token → only `viewer`, an
   * owner-token → EITHER. WS role is no longer a fixed HTTP→WS mapping (P0-1). */
  tokenRole: "owner" | "viewer" | "source";
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
    (a.tokenRole === "owner" || a.tokenRole === "viewer" || a.tokenRole === "source") &&
    typeof a.connectedAt === "number" &&
    typeof a.helloDone === "boolean" &&
    typeof a.subscribedThisConn === "boolean" &&
    typeof a.deltaEligible === "boolean"
  );
}
