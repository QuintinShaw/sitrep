import XCTest
@testable import SitrepKit

/// Fixture-driven coverage of the Swift protocol layer against
/// `proto/realtime/fixtures/`, mirroring what `tools/validate.js` checks for
/// the TS/Go implementations: every valid fixture decodes and round-trips;
/// every invalid fixture is rejected, either by decoding (schema-equivalent
/// validation) or by the client authorization matrix for the two
/// role-tagged fixtures.
final class RealtimeMessageFixtureTests: XCTestCase {
    // MARK: - valid/

    func testAllValidFixturesDecodeAndRoundTrip() throws {
        let names = try FixtureLoader.names(in: "valid")
        XCTAssertFalse(names.isEmpty, "expected at least one valid fixture")
        for name in names {
            try assertDecodesAndRoundTrips(fixture: "valid/\(name)")
        }
    }

    // MARK: - scenarios/**/*.json (every message in every scenario is individually valid)

    func testAllScenarioMessagesDecodeAndRoundTrip() throws {
        let scenarioDirs = try scenarioDirectoryNames()
        XCTAssertFalse(scenarioDirs.isEmpty)
        for dir in scenarioDirs {
            for file in try FixtureLoader.scenarioFiles(dir) {
                try assertDecodesAndRoundTrips(fixture: "scenarios/\(dir)/\(file.name)", data: file.data)
            }
        }
    }

    private func assertDecodesAndRoundTrips(fixture: String, data: Data? = nil) throws {
        let bytes = try data ?? FixtureLoader.data(fixture)
        let frame: RealtimeFrame
        do {
            frame = try RealtimeFrame.decode(bytes)
        } catch {
            XCTFail("\(fixture): expected successful decode, got \(error)")
            return
        }
        let reencoded = try frame.encoded()
        let redecoded = try RealtimeFrame.decode(reencoded)
        XCTAssertEqual(frame, redecoded, "\(fixture): round-trip changed semantics")
    }

    // MARK: - invalid/

    /// Fixtures whose invalidity is a role/authorization violation rather
    /// than a schema violation: the frame body decodes fine on its own (it's
    /// well-formed JSON matching its schema), but the sender's role forbids
    /// sending it (SPEC.md §10.1). These use the
    /// `{"sender_role": ..., "frame": {...}}` wrapper described in §16.
    private static let roleTaggedInvalidFixtures: Set<String> = [
        "role-client-hello-accept.json",
        "role-viewer-command-origin-server.json",
    ]

    func testInvalidFixturesFailDecodeOrAreUnauthorized() throws {
        let names = try FixtureLoader.names(in: "invalid")
        XCTAssertFalse(names.isEmpty)
        for name in names {
            let bytes = try FixtureLoader.data("invalid/\(name)")
            if Self.roleTaggedInvalidFixtures.contains(name) {
                try assertRejectedByAuthorization(fixture: name, wrapperData: bytes)
            } else {
                assertFailsToDecode(fixture: name, data: bytes)
            }
        }
    }

    private func assertFailsToDecode(fixture: String, data: Data) {
        XCTAssertThrowsError(try RealtimeFrame.decode(data), "invalid/\(fixture) should fail to decode") { error in
            guard error is RealtimeDecodingError || error is DecodingError else {
                XCTFail("invalid/\(fixture): unexpected error type \(error)")
                return
            }
        }
    }

    private func assertRejectedByAuthorization(fixture: String, wrapperData: Data) throws {
        guard let object = try JSONSerialization.jsonObject(with: wrapperData) as? [String: Any],
              let senderRoleRaw = object["sender_role"] as? String,
              let senderRole = RTDeviceRole(rawValue: senderRoleRaw),
              let frameObject = object["frame"]
        else {
            XCTFail("invalid/\(fixture): expected {sender_role, frame} wrapper")
            return
        }
        let frameData = try JSONSerialization.data(withJSONObject: frameObject)
        // The wrapped frame is schema-valid on its own — decoding it must
        // succeed; the rejection is purely a role/authorization matter.
        let frame = try RealtimeFrame.decode(frameData)
        XCTAssertFalse(
            RealtimeAuthorization.clientMaySend(frame, as: senderRole),
            "invalid/\(fixture): expected role '\(senderRole)' to be disallowed from sending this frame")
    }

    // MARK: - helpers

    private func scenarioDirectoryNames() throws -> [String] {
        let dir = FixtureLoader.fixturesRoot.appendingPathComponent("scenarios")
        return try FileManager.default.contentsOfDirectory(atPath: dir.path)
            .filter { name in
                var isDir: ObjCBool = false
                FileManager.default.fileExists(atPath: dir.appendingPathComponent(name).path, isDirectory: &isDir)
                return isDir.boolValue
            }
            .sorted()
    }
}
