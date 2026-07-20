import Foundation

public extension APIClient {
    /// The realtime WebSocket endpoint (`proto/realtime/`), derived from
    /// this client's REST `baseURL` by swapping the scheme and appending the
    /// realtime path.
    ///
    /// `proto/realtime/SPEC.md` is deliberately transport-independent and
    /// does not pin a concrete HTTP path; v1 mounts it at `GET /v1/realtime`
    /// (docs/api/v1/openapi.yaml, v1-architecture.md §2.1) — the single
    /// state-plane route surface shared with every other `/v1/*` route, not
    /// a separate version prefix the way the pre-v1 client's `/v3/realtime`
    /// was.
    var realtimeURL: URL? {
        guard var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false) else { return nil }
        switch components.scheme {
        case "https": components.scheme = "wss"
        case "http": components.scheme = "ws"
        default: break
        }
        let base = components.path.hasSuffix("/") ? String(components.path.dropLast()) : components.path
        components.path = base + "/v1/realtime"
        return components.url
    }
}
