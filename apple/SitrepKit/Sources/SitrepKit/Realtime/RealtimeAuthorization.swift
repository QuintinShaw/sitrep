import Foundation

/// A narrow mirror of SPEC.md §10.1's authorization matrix, scoped to what a
/// *client* (source or viewer) may legally send — the rules a conformant
/// server enforces against inbound client traffic. SitrepApp only ever acts
/// as a viewer, so `RealtimeClient` only ever needs the viewer column, but
/// this stays general so the two role-tagged invalid fixtures
/// (`fixtures/invalid/role-client-hello-accept.json`,
/// `fixtures/invalid/role-viewer-command-origin-server.json`) can be checked
/// exactly as `proto/realtime/tools/validate.js` checks them, and so a
/// future source-role implementation (e.g. the menu bar app, stage 5) can
/// reuse it instead of re-deriving the matrix.
///
/// This is a client-side sanity net, not a substitute for the server's own
/// enforcement: the server is the authority per §10.1; this only prevents
/// *this* implementation from ever constructing or accepting a frame the
/// matrix forbids.
public enum RealtimeAuthorization {
    /// Whether a client authenticated with `role` may send a frame of this
    /// shape. Only encodes the rules exercised by the two role-tagged
    /// invalid fixtures (hello stage/role, command origin) — see SPEC.md
    /// §10.1 for the full matrix, most of which server-side implementations
    /// need and a viewer-only client does not.
    public static func clientMaySend(_ frame: RealtimeFrame, as role: RTDeviceRole) -> Bool {
        switch frame {
        case .hello(let e):
            // §9.1: a client may only ever send stage "offer"; "accept"
            // belongs exclusively to the server.
            if case .accept = e.body { return false }
            return true
        case .command(let e):
            // §8: a client-sent command with origin "server" is always
            // unauthorized, regardless of role or action.
            return e.body.origin != .server
        case .resume, .subscribe, .unsubscribe, .interestRenew:
            return role == .viewer
        case .taskEvent, .messageEvent, .metricFrame:
            return role == .source
        case .snapshot, .delta, .configEvent:
            // Server-only in both directions; no client may send these.
            return false
        case .ack, .error:
            return true
        }
    }
}
