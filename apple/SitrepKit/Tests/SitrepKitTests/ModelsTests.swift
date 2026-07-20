import XCTest
@testable import SitrepKit

final class ModelsTests: XCTestCase {
    func testTaskStateDecodesServerJSON() throws {
        let json = """
        {"task_id":"t1","title":"nightly build","state":"running","percent":45,"step":"downloading","updated_at":1784476800100}
        """
        let task = try JSONDecoder().decode(TaskState.self, from: Data(json.utf8))
        XCTAssertEqual(task.sourceID, "t1")
        XCTAssertEqual(task.status, .running)
        XCTAssertEqual(task.percent, 45)
        XCTAssertEqual(task.updatedAt.timeIntervalSince1970, 1_784_476_800.1, accuracy: 0.001)
        // v1's TaskState has no started_at field at all.
        XCTAssertNil(task.startedAt)
    }

    func testTaskStateDecodesNestedDisplayHints() throws {
        let json = """
        {"task_id":"t1","state":"running","updated_at":1784476800100,
         "display":{"icon":"bolt","tint":"orange","template":"timer"}}
        """
        let task = try JSONDecoder().decode(TaskState.self, from: Data(json.utf8))
        XCTAssertEqual(task.icon, "bolt")
        XCTAssertEqual(task.tint, "orange")
        XCTAssertEqual(task.template, "timer")
    }

    // MARK: - capabilities.ws_transport_enabled (transport-gate input)

    private func decodeSnapshot(extraCapabilitiesField: String) throws -> SpaceSnapshot {
        let json = """
        {
          "space_revision": 128,
          "generated_at": "2026-07-19T10:00:00Z",
          "capabilities": {\(extraCapabilitiesField)},
          "presence": {"sources_online": 1},
          "tasks": [],
          "metrics": [],
          "messages": [],
          "automations": []
        }
        """
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(SpaceSnapshot.self, from: Data(json.utf8))
    }

    func testSnapshotDecodesWsTransportEnabledTrue() throws {
        let snapshot = try decodeSnapshot(extraCapabilitiesField: #""ws_transport_enabled": true"#)
        XCTAssertTrue(snapshot.capabilities.wsTransportEnabled)
    }

    func testSnapshotDecodesWsTransportEnabledFalse() throws {
        let snapshot = try decodeSnapshot(extraCapabilitiesField: #""ws_transport_enabled": false"#)
        XCTAssertFalse(snapshot.capabilities.wsTransportEnabled)
    }

    /// Old/misbehaving servers that omit the field entirely — absence must
    /// decode to `false`, never inferred from anything else.
    func testSnapshotDecodesWsTransportEnabledAbsentAsFalse() throws {
        let snapshot = try decodeSnapshot(extraCapabilitiesField: "")
        XCTAssertFalse(snapshot.capabilities.wsTransportEnabled)
    }

    /// A response with no `capabilities` object at all (not just a missing
    /// field inside it) must also decode as "nothing enabled."
    func testSnapshotDecodesMissingCapabilitiesObjectAsAllFalse() throws {
        let json = """
        {
          "space_revision": 1,
          "generated_at": "2026-07-19T10:00:00Z",
          "presence": {"sources_online": 0},
          "tasks": [], "metrics": [], "messages": [], "automations": []
        }
        """
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let snapshot = try decoder.decode(SpaceSnapshot.self, from: Data(json.utf8))
        XCTAssertFalse(snapshot.capabilities.wsTransportEnabled)
        XCTAssertFalse(snapshot.capabilities.apnsDeliveryEnabled)
    }

    func testSnapshotDecodesPresenceUnixMsTimestampsAndSourcesOnline() throws {
        let json = """
        {
          "space_revision": 129,
          "generated_at": "2026-07-19T10:00:00Z",
          "capabilities": {"ws_transport_enabled": true, "apns_delivery_enabled": true, "protocol_versions": [1]},
          "presence": {"ingest_last_seen": 1784476800100, "agent_last_seen": 1784476790000, "sources_online": 1},
          "tasks": [], "metrics": [], "messages": [], "automations": []
        }
        """
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let snapshot = try decoder.decode(SpaceSnapshot.self, from: Data(json.utf8))
        XCTAssertEqual(snapshot.presence.sourcesOnline, 1)
        XCTAssertEqual(try XCTUnwrap(snapshot.presence.ingestLastSeen).timeIntervalSince1970, 1_784_476_800.1, accuracy: 0.001)
        XCTAssertEqual(try XCTUnwrap(snapshot.presence.agentLastSeen).timeIntervalSince1970, 1_784_476_790.0, accuracy: 0.001)
    }

    func testSpaceRevisionField() throws {
        let snapshot = try decodeSnapshot(extraCapabilitiesField: "")
        XCTAssertEqual(snapshot.spaceRevision, 128)
    }

    // MARK: - MetricSample: flat target/min/max/alert_* (v1) vs. nested (old /v2)

    func testMetricSampleDecodesFlatFieldsAndNestedDisplay() throws {
        let json = """
        {"metric_id":"cpu.load1","value":"2.34","ts":1784476800100,
         "display":{"icon":"cpu","tint":"blue"},
         "target":"4","min":"0","max":"8","alert_above":"6","alert_below":"0.5"}
        """
        let metric = try JSONDecoder().decode(SnapshotMetric.self, from: Data(json.utf8))
        XCTAssertEqual(metric.id, "cpu.load1")
        XCTAssertEqual(metric.display.icon, "cpu")
        XCTAssertEqual(metric.target, "4")
        XCTAssertEqual(metric.alertAbove, "6")
        let state = metric.state
        XCTAssertNil(state.source, "v1 MetricSample carries no automation-linkage field")
        XCTAssertNil(state.history, "v1 has no inline sparkline history")
    }

    // MARK: - MessageRecord: level is already info/warn/error, no severity remap

    func testMessageRecordDecodesLevelDirectly() throws {
        let json = """
        {"message_id":"m1","device_id":"dev_1","level":"warn","text":"disk high","occurred_at":1784476800100}
        """
        let message = try JSONDecoder().decode(SnapshotMessage.self, from: Data(json.utf8))
        XCTAssertEqual(message.state.level, "warn")
        XCTAssertEqual(message.state.serverID, "m1")
    }

    // MARK: - AutomationState: flat executor_kind, unix-ms last_run_at

    func testAutomationStateDecodesFlatExecutorKindAndUnixMsLastRun() throws {
        let json = """
        {"automation_id":"nightly-build","name":"Nightly build","executor_kind":"script",
         "schedule":{"kind":"interval","every_seconds":86400},"state":"active","last_run_at":1784390400000}
        """
        let automation = try JSONDecoder().decode(AutomationInfo.self, from: Data(json.utf8))
        XCTAssertEqual(automation.id, "nightly-build")
        XCTAssertEqual(automation.executor.kind, "script")
        XCTAssertEqual(automation.everyS, 86400)
        XCTAssertTrue(automation.enabled)
        XCTAssertEqual(try XCTUnwrap(automation.lastRun).timeIntervalSince1970, 1_784_390_400, accuracy: 0.001)
        // Absent run_request_id/run_requested_at (older server) default safely.
        XCTAssertEqual(automation.runRequestID, 0)
        XCTAssertNil(automation.runRequestedAt)
    }

    /// Post-contract: AutomationState gains run_request_id (required) and the
    /// display-only run_requested_at. The phone decodes both to stay in sync
    /// but never acts on them (v1-architecture.md §5.1, P0-4).
    func testAutomationStateDecodesRunRequestIDAndRequestedAt() throws {
        let json = """
        {"automation_id":"96b0d5","name":"Nightly build","executor_kind":"script",
         "schedule":{"kind":"interval","every_seconds":86400},"state":"active",
         "last_run_at":1784390400000,"run_request_id":5,"run_requested_at":1784476900000}
        """
        let automation = try JSONDecoder().decode(AutomationInfo.self, from: Data(json.utf8))
        XCTAssertEqual(automation.runRequestID, 5)
        XCTAssertEqual(try XCTUnwrap(automation.runRequestedAt).timeIntervalSince1970, 1_784_476_900, accuracy: 0.001)
    }

    /// run_requested_at is explicitly nullable in the contract; a null must
    /// decode as absent, not as a decode failure.
    func testAutomationStateDecodesNullRunRequestedAt() throws {
        let json = """
        {"automation_id":"nightly-build","name":"Nightly build","executor_kind":"script",
         "schedule":{"kind":"interval","every_seconds":86400},"state":"active",
         "run_request_id":0,"run_requested_at":null}
        """
        let automation = try JSONDecoder().decode(AutomationInfo.self, from: Data(json.utf8))
        XCTAssertEqual(automation.runRequestID, 0)
        XCTAssertNil(automation.runRequestedAt)
    }

    // MARK: - POST /v1/spaces now returns device_id (P0-1)

    func testSpaceCredentialsDecodesDeviceID() throws {
        let json = """
        {"space_id":"k7m3qzx2vt","device_id":"dev_own_8f2a1c",
         "owner_token":"sr1_k7m3qzx2vt_0a47bf1ebf9b6cef76cef2f33d48189f29ea1a552bceaf51"}
        """
        let creds = try JSONDecoder().decode(APIClient.SpaceCredentials.self, from: Data(json.utf8))
        XCTAssertEqual(creds.spaceID, "k7m3qzx2vt")
        XCTAssertEqual(creds.deviceID, "dev_own_8f2a1c")
        XCTAssertTrue(creds.ownerToken.hasPrefix("sr1_"))
    }

    /// An older server that omits device_id must not break onboarding.
    func testSpaceCredentialsDecodesWithoutDeviceID() throws {
        let json = """
        {"space_id":"k7m3qzx2vt","owner_token":"sr1_k7m3qzx2vt_0a47bf1ebf9b6cef76cef2f33d48189f29ea1a552bceaf51"}
        """
        let creds = try JSONDecoder().decode(APIClient.SpaceCredentials.self, from: Data(json.utf8))
        XCTAssertNil(creds.deviceID)
    }

    /// ClientConfig round-trips device_id through its snake_case wire key so
    /// the shared config the CLI/agent reads carries the owner device id.
    func testClientConfigRoundTripsDeviceID() throws {
        let cfg = ClientConfig(server: "https://s", token: "sr1_x", space: "x", deviceID: "dev_own_8f2a1c")
        let data = try JSONEncoder().encode(cfg)
        let json = try XCTUnwrap(String(data: data, encoding: .utf8))
        XCTAssertTrue(json.contains("\"device_id\""), "must serialize as snake_case device_id")
        let decoded = try JSONDecoder().decode(ClientConfig.self, from: data)
        XCTAssertEqual(decoded.deviceID, "dev_own_8f2a1c")
    }

    // MARK: - SeriesPoint: ts/value keys, unix-ms

    func testSeriesPointDecodesTsAndValueKeys() throws {
        let json = #"{"ts": 1784476800100, "value": 42.5}"#
        let point = try JSONDecoder().decode(SeriesPoint.self, from: Data(json.utf8))
        XCTAssertEqual(point.v, 42.5)
        XCTAssertEqual(point.t.timeIntervalSince1970, 1_784_476_800.1, accuracy: 0.001)
    }
}
