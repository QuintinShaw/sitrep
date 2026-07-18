import Foundation

/// Server credentials shared between the app and the widget extension via
/// the App Group container.
enum SharedConfig {
    static let suiteName = "group.dev.sitrep.app"

    static func save(server: String, token: String) {
        let d = UserDefaults(suiteName: suiteName)
        d?.set(server, forKey: "serverURL")
        d?.set(token, forKey: "token")
    }

    static func load() -> (server: URL, token: String?)? {
        guard let d = UserDefaults(suiteName: suiteName),
              let server = d.string(forKey: "serverURL"),
              let url = URL(string: server), !server.isEmpty else { return nil }
        let token = d.string(forKey: "token")
        return (url, (token?.isEmpty ?? true) ? nil : token)
    }
}
