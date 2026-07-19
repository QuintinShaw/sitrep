import XCTest
@testable import SitrepKit

final class ModelsTests: XCTestCase {
    func testTaskStateDecodesServerJSON() throws {
        let json = """
        {"source_id":"t1","title":"nightly build","status":"running","percent":45,"step":"downloading","updated_at":"2026-07-17T12:00:00Z"}
        """
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let task = try decoder.decode(TaskState.self, from: Data(json.utf8))
        XCTAssertEqual(task.sourceID, "t1")
        XCTAssertEqual(task.status, .running)
        XCTAssertEqual(task.percent, 45)
    }

    // MARK: - realtime_enabled (P0 capability gate input)

    private func decodeSnapshot(extraTopLevelField: String) throws -> SpaceSnapshot {
        let json = """
        {
          "version": 2,
          "generated_at": "2026-07-17T12:00:00Z",
          "presence": {},
          "tasks": [],
          "metrics": [],
          "messages": [],
          "automations": []\(extraTopLevelField)
        }
        """
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(SpaceSnapshot.self, from: Data(json.utf8))
    }

    func testSnapshotDecodesRealtimeEnabledTrue() throws {
        let snapshot = try decodeSnapshot(extraTopLevelField: #", "realtime_enabled": true"#)
        XCTAssertTrue(snapshot.realtimeEnabled)
    }

    func testSnapshotDecodesRealtimeEnabledFalse() throws {
        let snapshot = try decodeSnapshot(extraTopLevelField: #", "realtime_enabled": false"#)
        XCTAssertFalse(snapshot.realtimeEnabled)
    }

    /// Old servers never send this field — absence must decode to `false`,
    /// never inferred from anything else (P0 fix).
    func testSnapshotDecodesRealtimeEnabledAbsentAsFalse() throws {
        let snapshot = try decodeSnapshot(extraTopLevelField: "")
        XCTAssertFalse(snapshot.realtimeEnabled)
    }
}
