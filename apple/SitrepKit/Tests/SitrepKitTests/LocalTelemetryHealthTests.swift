import XCTest
@testable import SitrepKit

/// N1 + v1-architecture.md §14: the daemon writes one
/// `~/.config/sitrep/health.d/<component>.json` = `{ok, reason}` file per
/// component, and the menu bar aggregates non-stale `ok: false` files into
/// a warning. These cover the pure parse + anomaly-derivation contract (no
/// filesystem needed) plus the directory-aggregation contract via a temp
/// directory.
final class LocalTelemetryHealthTests: XCTestCase {
    func testHealthyReportsNoAnomaly() throws {
        let health = try LocalTelemetryHealth.parse(Data(#"{"ok": true}"#.utf8))
        XCTAssertTrue(health.ok)
        XCTAssertNil(health.anomalyReason, "ok:true is not an anomaly")
    }

    func testUnhealthySurfacesReason() throws {
        let health = try LocalTelemetryHealth.parse(Data(#"{"ok": false, "reason": "outbox db unwritable"}"#.utf8))
        XCTAssertFalse(health.ok)
        XCTAssertEqual(health.anomalyReason, "outbox db unwritable")
    }

    func testUnhealthyWithoutReasonFallsBackToGenericMessage() throws {
        let health = try LocalTelemetryHealth.parse(Data(#"{"ok": false}"#.utf8))
        XCTAssertNotNil(health.anomalyReason, "an ok:false with no reason still raises the warning")
    }

    func testUnhealthyWithBlankReasonFallsBackToGenericMessage() throws {
        let health = try LocalTelemetryHealth.parse(Data(#"{"ok": false, "reason": "   "}"#.utf8))
        XCTAssertNotNil(health.anomalyReason)
        XCTAssertFalse(try XCTUnwrap(health.anomalyReason).isEmpty)
    }

    func testMalformedPayloadThrows() {
        XCTAssertThrowsError(try LocalTelemetryHealth.parse(Data("not json".utf8)))
    }
}

/// `LocalTelemetryHealthDirectory.aggregate`/`failingComponents` against a
/// real temp directory, since the interesting behavior (enumeration,
/// staleness via mtime, multi-component combination) needs actual files.
final class LocalTelemetryHealthDirectoryTests: XCTestCase {
    private var dir: URL!

    override func setUpWithError() throws {
        dir = FileManager.default.temporaryDirectory
            .appending(path: "sitrep-health-d-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    }

    override func tearDownWithError() throws {
        try? FileManager.default.removeItem(at: dir)
    }

    private func write(_ name: String, ok: Bool, reason: String? = nil, mtime: Date = Date()) throws {
        let url = dir.appending(path: "\(name).json")
        var json = #"{"ok": \#(ok)"#
        if let reason { json += #", "reason": "\#(reason)""# }
        json += "}"
        try Data(json.utf8).write(to: url)
        try FileManager.default.setAttributes([.modificationDate: mtime], ofItemAtPath: url.path)
    }

    // MARK: - Absence == healthy

    func testMissingDirectoryIsHealthy() {
        let missing = FileManager.default.temporaryDirectory
            .appending(path: "sitrep-health-d-does-not-exist-\(UUID().uuidString)")
        XCTAssertNil(LocalTelemetryHealthDirectory.aggregate(directory: missing))
        XCTAssertEqual(LocalTelemetryHealthDirectory.failingComponents(in: missing), [])
    }

    func testEmptyDirectoryIsHealthy() {
        XCTAssertNil(LocalTelemetryHealthDirectory.aggregate(directory: dir))
    }

    func testDirectoryWithOnlyHealthyFilesIsHealthy() throws {
        try write("outbox", ok: true)
        try write("device_seq", ok: true)
        XCTAssertNil(LocalTelemetryHealthDirectory.aggregate(directory: dir))
    }

    // MARK: - Staleness resolves a component without a recovery write

    func testStaleUnhealthyAndFreshUnhealthyOnlyFreshContributes() throws {
        let now = Date()
        let staleTime = now.addingTimeInterval(-(LocalTelemetryHealthDirectory.staleAfter + 60))
        try write("outbox_open", ok: false, reason: "stale failure, should be ignored", mtime: staleTime)
        try write("device_seq", ok: false, reason: "fresh failure", mtime: now)

        let failing = LocalTelemetryHealthDirectory.failingComponents(in: dir, now: now)
        XCTAssertEqual(failing.count, 1)
        XCTAssertEqual(failing.first?.component, "device_seq")
        XCTAssertEqual(failing.first?.reason, "fresh failure")

        let warning = try XCTUnwrap(LocalTelemetryHealthDirectory.aggregate(directory: dir, now: now))
        XCTAssertTrue(warning.contains("fresh failure"))
        XCTAssertFalse(warning.contains("stale failure"))
    }

    func testFileExactlyAtStaleBoundaryIsStillFresh() throws {
        let now = Date()
        let boundary = now.addingTimeInterval(-LocalTelemetryHealthDirectory.staleAfter)
        try write("outbox", ok: false, reason: "right at the edge", mtime: boundary)
        let failing = LocalTelemetryHealthDirectory.failingComponents(in: dir, now: now)
        XCTAssertEqual(failing.count, 1, "== staleAfter should not yet be discarded")
    }

    // MARK: - Multi-component aggregation names each failing component

    func testMultipleFreshUnhealthyComponentsAreAllNamedInWarning() throws {
        let now = Date()
        try write("outbox", ok: false, reason: "outbox full", mtime: now)
        try write("device_seq", ok: false, reason: "seq db locked", mtime: now)

        let warning = try XCTUnwrap(LocalTelemetryHealthDirectory.aggregate(directory: dir, now: now))
        XCTAssertTrue(warning.contains("outbox"))
        XCTAssertTrue(warning.contains("outbox full"))
        XCTAssertTrue(warning.contains("device_seq"))
        XCTAssertTrue(warning.contains("seq db locked"))
    }

    func testSingleUnhealthyComponentOmitsComponentPrefix() throws {
        try write("outbox", ok: false, reason: "disk full")
        let warning = try XCTUnwrap(LocalTelemetryHealthDirectory.aggregate(directory: dir))
        // Matches the prior single-file design's bare-reason message.
        XCTAssertEqual(warning, "disk full")
    }

    // MARK: - Malformed / non-json entries are skipped, not surfaced as errors

    func testMalformedComponentFileIsTreatedAsHealthy() throws {
        try Data("not json".utf8).write(to: dir.appending(path: "outbox.json"))
        XCTAssertNil(LocalTelemetryHealthDirectory.aggregate(directory: dir))
    }

    func testNonJSONFilesInDirectoryAreIgnored() throws {
        try Data("hello".utf8).write(to: dir.appending(path: "notes.txt"))
        try write("outbox", ok: true)
        XCTAssertNil(LocalTelemetryHealthDirectory.aggregate(directory: dir))
    }

    #if os(macOS)
    func testCanonicalDirectoryURLIsUnderConfigDir() {
        XCTAssertEqual(LocalTelemetryHealthDirectory.directoryURL.lastPathComponent, "health.d")
        XCTAssertTrue(LocalTelemetryHealthDirectory.directoryURL.path.contains(".config/sitrep"))
    }
    #endif
}
