import Foundation

/// Loads fixtures directly from `proto/realtime/fixtures/` (the frozen
/// protocol repository, never copied or duplicated into `apple/`) using
/// this test file's own `#filePath` to locate the repo root. This keeps the
/// Swift test suite testing the SAME fixtures the spec's own
/// `tools/validate.js` checks, with zero drift risk from a stale copy.
enum FixtureLoader {
    /// `.../apple/SitrepKit/Tests/SitrepKitTests/Realtime/FixtureLoader.swift`
    /// → walk up to the repo root.
    static var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // FixtureLoader.swift -> Realtime/
            .deletingLastPathComponent() // Realtime/ -> SitrepKitTests/
            .deletingLastPathComponent() // SitrepKitTests/ -> Tests/
            .deletingLastPathComponent() // Tests/ -> SitrepKit/
            .deletingLastPathComponent() // SitrepKit/ -> apple/
            .deletingLastPathComponent() // apple/ -> repo root
    }

    static var fixturesRoot: URL {
        repoRoot.appendingPathComponent("proto/realtime/fixtures")
    }

    static func data(_ relativePath: String) throws -> Data {
        try Data(contentsOf: fixturesRoot.appendingPathComponent(relativePath))
    }

    static func names(in subdirectory: String) throws -> [String] {
        let dir = fixturesRoot.appendingPathComponent(subdirectory)
        return try FileManager.default.contentsOfDirectory(atPath: dir.path)
            .filter { $0.hasSuffix(".json") }
            .sorted()
    }

    /// Ordered `.json` files in a scenario directory (excludes `README.md`),
    /// sorted by filename — the scenarios are numbered `01-...json`,
    /// `02-...json`, so lexicographic order is message order.
    static func scenarioFiles(_ scenario: String) throws -> [(name: String, data: Data)] {
        let names = try names(in: "scenarios/\(scenario)")
        return try names.map { ($0, try data("scenarios/\(scenario)/\($0)")) }
    }
}
