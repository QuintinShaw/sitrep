import Foundation

/// Client configuration, resolved from (in order):
/// 1. SITREP_SERVER / SITREP_TOKEN environment variables
/// 2. ~/.config/sitrep/config.json  {"server": "...", "token": "..."}
public struct ClientConfig: Codable, Sendable {
    public var server: String
    public var token: String?
    public var space: String?
    /// Read-side token for this machine's viewers (menu bar); falls back to
    /// `token` when absent (bootstrap/admin setups).
    public var viewerToken: String?

    enum CodingKeys: String, CodingKey {
        case server, token, space
        case viewerToken = "viewer_token"
    }

    public init(server: String, token: String?, space: String? = nil, viewerToken: String? = nil) {
        self.server = server
        self.token = token
        self.space = space
        self.viewerToken = viewerToken
    }

    #if os(macOS)
    /// Persist to the shared config file (0600) used by the CLI/agent too.
    public func save() throws {
        let url = FileManager.default.homeDirectoryForCurrentUser
            .appending(path: ".config/sitrep/config.json")
        try FileManager.default.createDirectory(
            at: url.deletingLastPathComponent(), withIntermediateDirectories: true,
            attributes: [.posixPermissions: 0o700]
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        try encoder.encode(self).write(to: url, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
    }

    public static func loadFromFile() -> ClientConfig? {
        let url = FileManager.default.homeDirectoryForCurrentUser
            .appending(path: ".config/sitrep/config.json")
        guard let data = try? Data(contentsOf: url) else { return nil }
        return try? JSONDecoder().decode(ClientConfig.self, from: data)
    }
    #endif

    public static func load() -> ClientConfig? {
        let env = ProcessInfo.processInfo.environment
        if let server = env["SITREP_SERVER"], !server.isEmpty {
            return ClientConfig(server: server, token: env["SITREP_TOKEN"])
        }
        #if os(macOS)
        let fileURL = FileManager.default.homeDirectoryForCurrentUser
            .appending(path: ".config/sitrep/config.json")
        guard let data = try? Data(contentsOf: fileURL) else { return nil }
        return try? JSONDecoder().decode(ClientConfig.self, from: data)
        #else
        return nil // iOS configures in-app, not via config file
        #endif
    }

    public func makeClient() -> APIClient? {
        guard let url = URL(string: server) else { return nil }
        return APIClient(baseURL: url, token: viewerToken ?? token)
    }
}
