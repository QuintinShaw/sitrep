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
}
