import Foundation

/// Pure decision logic for the realtime capability P0 gate: the app must
/// only ever open a `RealtimeClient` connection when the most recent
/// successful REST refresh reported `realtime_enabled == true` on
/// `SpaceSnapshot`. Field absent/false (old or not-yet-rolled-out servers)
/// means "do not connect" — never inferred from URL shape or a `/v3` probe.
///
/// Deliberately free of any RealtimeClient/URLSession/actor dependency so it
/// can be unit-tested without a live socket. `RealtimeClient` itself stays
/// capability-agnostic; the caller (AppModel) owns one of these and asks it
/// what to do each time a REST refresh completes.
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

    /// Cold start (before any REST refresh has ever completed) must behave
    /// as "not capable" — this default is what makes that safe without any
    /// extra bookkeeping at call sites.
    public private(set) var isCapable = false

    public init() {}

    /// Feed the `realtime_enabled` value from a just-completed REST
    /// refresh. Returns the action the caller should take on its
    /// RealtimeClient. Idempotent: repeating the same value is `.none`.
    @discardableResult
    public mutating func apply(refreshedCapability enabled: Bool) -> Action {
        guard enabled != isCapable else { return .none }
        isCapable = enabled
        return enabled ? .connect : .disconnect
    }

    /// Force back to the cold-start state — used when the identity this
    /// gate was tracking no longer applies (re-pairing to a different
    /// server/space, or disconnecting entirely). The next refresh against
    /// the new identity must prove capability again before connecting.
    public mutating func reset() {
        isCapable = false
    }
}
