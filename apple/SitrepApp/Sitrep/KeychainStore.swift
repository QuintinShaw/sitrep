import Foundation
import Security

/// Minimal keychain string storage. Unlike UserDefaults / the App Group
/// container, keychain items survive app reinstalls — dev builds and app
/// updates keep their server credentials. The stored `token` is a v1
/// `sr1_<space_id>_<secret>` bearer credential (v1-architecture.md §10);
/// this layer treats it as an opaque string end to end — no client-side
/// format parsing or `st2_`-era regex lives anywhere in this app.
enum KeychainStore {
    private static let service = "dev.sitrep.app.credentials"

    static func set(_ value: String, for key: String) {
        let base: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: key,
        ]
        SecItemDelete(base as CFDictionary)
        guard !value.isEmpty else { return }
        var add = base
        add[kSecValueData as String] = Data(value.utf8)
        add[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlock
        SecItemAdd(add as CFDictionary, nil)
    }

    static func get(_ key: String) -> String? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: key,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var out: AnyObject?
        guard SecItemCopyMatching(query as CFDictionary, &out) == errSecSuccess,
              let data = out as? Data else { return nil }
        return String(data: data, encoding: .utf8)
    }
}
