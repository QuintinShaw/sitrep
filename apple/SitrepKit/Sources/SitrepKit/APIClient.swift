import Foundation

/// Client for the Sitrep v1 HTTP API (docs/api/v1/openapi.yaml,
/// docs/design/v1-architecture.md). Every route below is exactly the `/v1`
/// path the frozen contract specifies — this replaced the old `/v2`+`/v3`
/// surface wholesale, not just a prefix rename (v1-architecture.md §2.3).
public struct APIClient: Sendable {
    public var baseURL: URL
    public var token: String?

    public init(baseURL: URL, token: String?) {
        self.baseURL = baseURL
        self.token = token
    }

    public func devices() async throws -> [DeviceInfo] {
        try await get("/v1/devices")
    }

    public func taskLog(id: String) async throws -> [String] {
        try await get("/v1/tasks/\(id)/log")
    }

    public func series(key: String, range: SeriesRange) async throws -> [SeriesPoint] {
        try await get("/v1/metrics/\(key)/series?range=\(range.rawValue)")
    }

    public func snapshot() async throws -> SpaceSnapshot {
        try await get("/v1/snapshot")
    }

    // ---- messages (v1-architecture.md §2.3: split from POST /v2/messages/delete) ----

    /// Deletes each id with its own `DELETE /v1/messages/:id` call — v1 has
    /// no batch-by-body-array form; a client deleting N specific messages
    /// makes N calls (v1-architecture.md §2.3).
    public func deleteEvents(ids: [String]) async throws {
        for id in ids {
            try await delete("/v1/messages/\(id)")
        }
    }

    /// `DELETE /v1/messages` (no path id) — the successor to the old
    /// `{all: true}` body form.
    public func clearEvents() async throws {
        try await delete("/v1/messages")
    }

    // ---- spaces & invitations (multi-tenant model, unauthenticated control-plane routes) ----

    public struct SpaceCredentials: Codable, Sendable {
        public var spaceID: String
        /// The owner device's id, minted + persisted server-side (P0-1).
        /// `device_seq` is scoped to `(device_id, space)`, so the creating
        /// Mac MUST keep this — it is the identity the owner device's
        /// `POST /v1/events` uplink is scoped to (v1-architecture.md §2.2).
        /// Optional on decode only so an older server that omits it doesn't
        /// hard-break onboarding; the current contract always sends it.
        public var deviceID: String?
        public var ownerToken: String
        enum CodingKeys: String, CodingKey {
            case spaceID = "space_id"
            case deviceID = "device_id"
            case ownerToken = "owner_token"
        }
    }

    /// Mint an anonymous space (zero signup) — first-launch path.
    public static func createSpace(server: URL, platform: String, name: String) async throws -> SpaceCredentials {
        var req = URLRequest(url: server.appending(path: "/v1/spaces"))
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.httpBody = try JSONEncoder().encode(["platform": platform, "name": name])
        let (data, response) = try await URLSession.shared.data(for: req)
        guard (response as? HTTPURLResponse)?.statusCode == 200 else {
            throw APIError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
        return try JSONDecoder().decode(SpaceCredentials.self, from: data)
    }

    public struct Invite: Codable, Sendable {
        public var code: String
        public var spaceID: String
        public var expiresIn: Int
        enum CodingKeys: String, CodingKey {
            case code
            case spaceID = "space_id"
            case expiresIn = "expires_in"
        }
    }

    public func createInvite(role: String = "viewer") async throws -> Invite {
        var req = URLRequest(url: baseURL.appending(path: "/v1/invites"))
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let token { req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization") }
        req.httpBody = try JSONEncoder().encode(["role": role])
        let (data, response) = try await URLSession.shared.data(for: req)
        guard (response as? HTTPURLResponse)?.statusCode == 200 else {
            throw APIError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
        return try JSONDecoder().decode(Invite.self, from: data)
    }

    public struct Joined: Codable, Sendable {
        public var token: String
        public var deviceID: String
        public var spaceID: String
        enum CodingKeys: String, CodingKey {
            case token
            case deviceID = "device_id"
            case spaceID = "space_id"
        }
    }

    /// Join with a connect code. `space` is REQUIRED (v1-architecture.md
    /// §10.5, P0-6): the Worker routes directly to the target SpaceHub via
    /// `env.SPACE_HUB.getByName(space)` with zero KV lookup, so every call
    /// site — connect-code scan/paste (space decoded client-side from the
    /// code, see `ConnectCode.decode`) and the self-host deep-link path
    /// alike — must supply it. Making this non-optional (rather than
    /// defaulting to `nil`) is deliberate: it forces the compiler to catch
    /// a call site that forgets to decode/pass `space`, which is exactly
    /// the bug a prior round shipped (a hardcoded `space: nil` on the
    /// official-scan path).
    public static func join(server: URL, space: String, code: String,
                            name: String, platform: String) async throws -> Joined {
        var req = URLRequest(url: server.appending(path: "/v1/join"))
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body = ["code": code, "space": space, "name": name, "platform": platform]
        req.httpBody = try JSONEncoder().encode(body)
        let (data, response) = try await URLSession.shared.data(for: req)
        guard (response as? HTTPURLResponse)?.statusCode == 200 else {
            throw APIError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
        return try JSONDecoder().decode(Joined.self, from: data)
    }

    // ---- devices ----

    public func revokeDevice(id: String) async throws {
        try await delete("/v1/devices/\(id)")
    }

    // ---- push tokens (v1-architecture.md §2.3: POST /v2/devices + POST
    // /v2/activities split into two routes/tables) ----

    /// `PUT /v1/devices/self/push-tokens` — this device's own push-to-start
    /// and/or alert token. No `device_id` field: the target is always the
    /// authenticated caller (v1-architecture.md §2.3).
    public func registerPushTokens(pushToStartToken: String? = nil, alertToken: String? = nil) async throws {
        var body: [String: String] = [:]
        body["push_to_start_token"] = pushToStartToken
        body["alert_token"] = alertToken
        try await put("/v1/devices/self/push-tokens", body: body)
    }

    /// `PUT /v1/tasks/:id/live-activity-token` — the per-activity Live
    /// Activity update token for one task, keyed by path id (replaces the
    /// old `POST /v2/activities {source_id, token}` body form).
    public func registerLiveActivityToken(taskID: String, token activityToken: String) async throws {
        try await put("/v1/tasks/\(taskID)/live-activity-token", body: ["token": activityToken])
    }

    // ---- automations ----

    /// `PATCH /v1/automations/:id` — schedule/state only; v1 has no
    /// `run_now` field on this route (v1-architecture.md §5.1). Use
    /// `runAutomation(id:)` to trigger a run.
    public func patchAutomation(id: String, everyS: Int? = nil, enabled: Bool? = nil) async throws {
        var body: [String: AnyEncodableValue] = [:]
        if let everyS { body["schedule"] = .dictionary(["every_seconds": .int(everyS)]) }
        if let enabled { body["state"] = .string(enabled ? "active" : "paused") }
        var req = URLRequest(url: baseURL.appending(path: "/v1/automations/\(id)"))
        req.httpMethod = "PATCH"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let token { req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization") }
        req.httpBody = try JSONEncoder().encode(body)
        let (_, response) = try await URLSession.shared.data(for: req)
        guard (response as? HTTPURLResponse)?.statusCode == 200 else {
            throw APIError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
    }

    public func deleteAutomation(id: String) async throws {
        try await delete("/v1/automations/\(id)")
    }

    /// `POST /v1/automations/:id/run` — "run now" is a reverse-control
    /// command, not a state write: it mints no `config.event` and does not
    /// advance `space_revision` (v1-architecture.md §5.1). This REPLACES the
    /// old `PATCH /v2/automations/:id {run_now: true}` shape.
    public func runAutomation(id: String) async throws {
        var req = URLRequest(url: baseURL.appending(path: "/v1/automations/\(id)/run"))
        req.httpMethod = "POST"
        if let token { req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization") }
        let (_, response) = try await URLSession.shared.data(for: req)
        guard (response as? HTTPURLResponse)?.statusCode == 200 else {
            throw APIError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
    }

    // ---- task commands ----

    /// Reverse control (docs/design/pairing-and-control.md).
    public func sendCommand(_ action: TaskCommand, to taskID: String) async throws {
        try await post("/v1/tasks/\(taskID)/commands", body: ["action": action.rawValue])
    }

    // ---- generic HTTP helpers ----

    /// POST a JSON body.
    public func post(_ path: String, body: [String: String]) async throws {
        try await send("POST", path, body: body)
    }

    /// PUT a JSON body (idempotent-by-construction routes: push-token and
    /// live-activity-token registration).
    public func put(_ path: String, body: [String: String]) async throws {
        try await send("PUT", path, body: body)
    }

    /// DELETE with no body (message deletion, device revocation).
    private func delete(_ path: String) async throws {
        var req = URLRequest(url: baseURL.appending(path: path))
        req.httpMethod = "DELETE"
        if let token { req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization") }
        let (_, response) = try await URLSession.shared.data(for: req)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            throw APIError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
    }

    private func send(_ method: String, _ path: String, body: [String: String]) async throws {
        var req = URLRequest(url: baseURL.appending(path: path))
        req.httpMethod = method
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let token {
            req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        req.httpBody = try JSONEncoder().encode(body)
        let (_, response) = try await URLSession.shared.data(for: req)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw APIError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
    }

    private func get<T: Decodable>(_ path: String) async throws -> T {
        // URL(string:relativeTo:) keeps query strings intact (appending(path:)
        // would percent-encode the "?").
        guard let url = URL(string: path, relativeTo: baseURL) else {
            throw APIError.badStatus(-1)
        }
        var req = URLRequest(url: url)
        if let token {
            req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        let (data, response) = try await URLSession.shared.data(for: req)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            throw APIError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .custom { d in
            // Only `generated_at` (a Snapshot's top-level field) still
            // decodes as an ISO string in v1; every other timestamp is a
            // Unix-ms Int handled by each type's own manual init(from:).
            let s = try d.singleValueContainer().decode(String.self)
            if let date = Self.isoFractional.date(from: s) ?? Self.iso.date(from: s) {
                return date
            }
            throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "bad date \(s)"))
        }
        return try decoder.decode(T.self, from: data)
    }

    // ISO8601DateFormatter is documented thread-safe; it just predates Sendable.
    nonisolated(unsafe) private static let isoFractional: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }()
    nonisolated(unsafe) private static let iso = ISO8601DateFormatter()
}

public enum APIError: Error, LocalizedError {
    case badStatus(Int)

    public var errorDescription: String? {
        switch self {
        case .badStatus(let code):
            code == 401 ? "unauthorized — check your token" : "server returned \(code)"
        }
    }
}
