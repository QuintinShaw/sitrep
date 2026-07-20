import Foundation

/// Pure decision logic for the realtime TRANSPORT gate (v1-architecture.md
/// §8): the app must only ever open a `RealtimeClient` connection when the
/// most recent successful snapshot refresh (`GET /v1/snapshot`) reported
/// `capabilities.ws_transport_enabled == true`. Field absent/false (an old
/// server, a misbehaving one, or `WS_TRANSPORT_ENABLED=false` on this
/// deployment) means "do not connect" — never inferred from URL shape or a
/// `/v1/realtime` probe.
///
/// This is a TRANSPORT decision only, never a store decision (§0, §8.1): v1
/// has exactly one authoritative store (SpaceHub) reached identically by WS
/// and HTTP, so "capability off" means "poll `GET /v1/snapshot` against the
/// same space instead of opening a socket," not "read from a different
/// source." A `GET /v1/realtime` upgrade attempt that gets rejected with
/// `503 {"error": "transport_unavailable"}` is the same signal arriving
/// reactively instead of via the next snapshot poll — `RealtimeClient` maps
/// that into `.transportUnavailable`, and the caller (AppModel) reacts by
/// calling `reset()` here exactly as it would for a refresh reporting
/// `false`, so a later snapshot reporting `true` again re-probes cleanly.
///
/// Deliberately free of any RealtimeClient/URLSession/actor dependency so it
/// can be unit-tested without a live socket. `RealtimeClient` itself stays
/// transport-agnostic; the caller (AppModel) owns one of these and asks it
/// what to do each time a snapshot refresh completes.
public struct RealtimeCapabilityGate: Sendable, Equatable {
    /// What the caller should do to the RealtimeClient in response to a
    /// refreshed capability value.
    public enum Action: Sendable, Equatable {
        /// No change — either the value didn't change, or a repeat of an
        /// already-applied value.
        case none
        /// Capability just turned on (including the very first time a
        /// refresh reports `true`) — safe to (re)start the connection.
        case connect
        /// Capability just turned off (server rolled the flag back, or a
        /// legacy/absent response) while previously on — tear down any live
        /// connection and fall back to plain HTTP; no reconnect attempts
        /// until a later refresh reports `true` again.
        case disconnect
    }

    /// Cold start (before any snapshot refresh has ever completed) must
    /// behave as "not capable" — this default is what makes that safe
    /// without any extra bookkeeping at call sites.
    public private(set) var isCapable = false

    public init() {}

    /// Feed the `capabilities.ws_transport_enabled` value from a
    /// just-completed snapshot refresh. Returns the action the caller should
    /// take on its RealtimeClient. Idempotent: repeating the same value is
    /// `.none`.
    @discardableResult
    public mutating func apply(refreshedCapability enabled: Bool) -> Action {
        guard enabled != isCapable else { return .none }
        isCapable = enabled
        return enabled ? .connect : .disconnect
    }

    /// Force back to the cold-start state — used when the identity this
    /// gate was tracking no longer applies (re-pairing to a different
    /// server/space, disconnecting entirely, or a reactive
    /// `.transportUnavailable` notice from a 503'd WS upgrade). The next
    /// refresh against the new identity must prove capability again before
    /// connecting.
    public mutating func reset() {
        isCapable = false
    }
}
