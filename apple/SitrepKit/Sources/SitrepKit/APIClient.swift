import Foundation

/// Read-only client for the Sitrep server API (server/src/app.ts).
public struct APIClient: Sendable {
    public var baseURL: URL
    public var token: String?

    public init(baseURL: URL, token: String?) {
        self.baseURL = baseURL
        self.token = token
    }

    public func devices() async throws -> [DeviceInfo] {
        try await get("/v2/devices")
    }

    public func taskLog(id: String) async throws -> [String] {
        try await get("/v2/tasks/\(id)/log")
    }

    public func series(key: String, range: SeriesRange) async throws -> [SeriesPoint] {
        try await get("/v2/metrics/\(key)/series?range=\(range.rawValue)")
    }

    public func snapshot() async throws -> SpaceSnapshot {
        try await get("/v2/snapshot")
    }

    public func deleteEvents(ids: [String]) async throws {
        try await post("/v2/messages/delete", jsonBody: ["ids": ids])
    }

    public func clearEvents() async throws {
        try await post("/v2/messages/delete", jsonBody: ["all": true])
    }

    // ---- spaces & invitations (multi-tenant model) ----

    public struct SpaceCredentials: Codable, Sendable {
        public var spaceID: String
        public var ownerToken: String
        enum CodingKeys: String, CodingKey {
            case spaceID = "space_id"
            case ownerToken = "owner_token"
        }
    }

    /// Mint an anonymous space (zero signup) — first-launch path.
    public static func createSpace(server: URL, platform: String, name: String) async throws -> SpaceCredentials {
        var req = URLRequest(url: server.appending(path: "/v2/spaces"))
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
        var req = URLRequest(url: baseURL.appending(path: "/v2/invites"))
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

    /// Join with a connect code; `space` only needed on the self-host link
    /// path (official codes resolve server-side via the invite directory).
    public static func join(server: URL, space: String? = nil, code: String,
                            name: String, platform: String) async throws -> Joined {
        var req = URLRequest(url: server.appending(path: "/v2/join"))
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        var body = ["code": code, "name": name, "platform": platform]
        if let space { body["space"] = space }
        req.httpBody = try JSONEncoder().encode(body)
        let (data, response) = try await URLSession.shared.data(for: req)
        guard (response as? HTTPURLResponse)?.statusCode == 200 else {
            throw APIError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
        return try JSONDecoder().decode(Joined.self, from: data)
    }

    // ---- automations ----

    public func patchAutomation(id: String, everyS: Int? = nil, enabled: Bool? = nil, runNow: Bool = false) async throws {
        var body: [String: AnyEncodableValue] = [:]
        if let everyS { body["schedule"] = .dictionary(["every_seconds": .int(everyS)]) }
        if let enabled { body["state"] = .string(enabled ? "active" : "paused") }
        if runNow { body["run_now"] = .bool(true) }
        var req = URLRequest(url: baseURL.appending(path: "/v2/automations/\(id)"))
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
        var req = URLRequest(url: baseURL.appending(path: "/v2/automations/\(id)"))
        req.httpMethod = "DELETE"
        if let token { req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization") }
        let (_, response) = try await URLSession.shared.data(for: req)
        guard (response as? HTTPURLResponse)?.statusCode == 200 else {
            throw APIError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
    }

    public func updateMetric(id: String, icon: String? = nil, tint: String? = nil,
                             template: String? = nil, level: String? = nil,
                             alertAbove: String? = nil, alertBelow: String? = nil) async throws {
        var body: [String: String] = [:]
        body["icon"] = icon
        body["tint"] = tint
        body["template"] = template
        body["level"] = level
        body["alert_above"] = alertAbove
        body["alert_below"] = alertBelow
        try await sendJSON("PATCH", "/v2/metrics/\(id)", body: body)
    }

    private func sendJSON(_ method: String, _ path: String, body: [String: String]) async throws {
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

    /// Reverse control (docs/design/pairing-and-control.md).
    public func sendCommand(_ action: TaskCommand, to sourceID: String) async throws {
        try await post("/v2/tasks/\(sourceID)/commands", body: ["action": action.rawValue])
    }

    public func revokeDevice(id: String) async throws {
        var req = URLRequest(url: baseURL.appending(path: "/v2/devices/\(id)"))
        req.httpMethod = "DELETE"
        if let token {
            req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        let (_, response) = try await URLSession.shared.data(for: req)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            throw APIError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
    }

    /// POST a JSON body; used for device/activity token registration.
    public func post(_ path: String, body: [String: String]) async throws {
        var req = URLRequest(url: baseURL.appending(path: path))
        req.httpMethod = "POST"
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

    /// POST with an arbitrary JSON object body (arrays, bools, …).
    public func post(_ path: String, jsonBody: [String: Any]) async throws {
        var req = URLRequest(url: baseURL.appending(path: path))
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let token {
            req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        req.httpBody = try JSONSerialization.data(withJSONObject: jsonBody)
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
            // Server timestamps mix second and millisecond precision.
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
