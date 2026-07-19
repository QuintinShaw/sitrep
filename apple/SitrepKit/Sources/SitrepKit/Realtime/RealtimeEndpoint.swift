import Foundation

public extension APIClient {
    /// The realtime WebSocket endpoint (`proto/realtime/`), derived from
    /// this client's REST `baseURL` by swapping the scheme and appending the
    /// realtime path.
    ///
    /// `proto/realtime/SPEC.md` is deliberately transport-independent and
    /// does not pin a concrete HTTP path; the cross-implementation route
    /// convention is `/v3/realtime`, per protocol-owner ruling (the server
    /// implementation targets the same path). Note this is intentionally
    /// NOT `/v2/...`: the v2 prefix is the REST snapshot surface, while the
    /// realtime channel lives under its own v3 route.
    var realtimeURL: URL? {
        guard var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false) else { return nil }
        switch components.scheme {
        case "https": components.scheme = "wss"
        case "http": components.scheme = "ws"
        default: break
        }
        let base = components.path.hasSuffix("/") ? String(components.path.dropLast()) : components.path
        components.path = base + "/v3/realtime"
        return components.url
    }
}
