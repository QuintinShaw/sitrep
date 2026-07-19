import Foundation

public extension APIClient {
    /// The realtime WebSocket endpoint (`proto/realtime/`), derived from
    /// this client's REST `baseURL` by swapping the scheme and appending the
    /// realtime path.
    ///
    /// NOTE for the protocol/server owner: `proto/realtime/SPEC.md` is
    /// deliberately transport-independent and does not pin a concrete HTTP
    /// path — this repository currently has no server-side realtime route
    /// to confirm against (checked: no `server/src` file references
    /// WebSockets or a realtime endpoint as of this change). `/v2/realtime`
    /// is this client's assumption, chosen to match the existing `/v2/...`
    /// REST surface; it is not yet a cross-implementation contract. Update
    /// this in lockstep once the server lands its route.
    var realtimeURL: URL? {
        guard var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false) else { return nil }
        switch components.scheme {
        case "https": components.scheme = "wss"
        case "http": components.scheme = "ws"
        default: break
        }
        let base = components.path.hasSuffix("/") ? String(components.path.dropLast()) : components.path
        components.path = base + "/v2/realtime"
        return components.url
    }
}
