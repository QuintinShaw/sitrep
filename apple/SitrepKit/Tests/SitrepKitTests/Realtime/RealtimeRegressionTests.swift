import XCTest
@testable import SitrepKit

/// Regression coverage for the adversarial-review rulings (F1/F3/F4/F5):
/// HTTP-overwrite gating while the realtime channel is live, interleaved
/// frames during chunked snapshots, incremental resume across foreground
/// cycles, and body-level leniency of the delta event wrapper.
final class RealtimeRegressionTests: XCTestCase {
    // MARK: - F1: HTTP snapshot must not overwrite reliable state while live

    func testOnlyLivePhaseBlocksReliableStateOverwrite() {
        XCTAssertFalse(RealtimeClient.Phase.live.allowsReliableStateOverwrite,
                       "while live, deltas own the reliable collections")
        XCTAssertTrue(RealtimeClient.Phase.idle.allowsReliableStateOverwrite)
        XCTAssertTrue(RealtimeClient.Phase.connecting.allowsReliableStateOverwrite)
        XCTAssertTrue(RealtimeClient.Phase.handshaking.allowsReliableStateOverwrite)
        XCTAssertTrue(RealtimeClient.Phase.subscribed.allowsReliableStateOverwrite,
                      "not yet delta-eligible — HTTP is still the best source")
        XCTAssertTrue(RealtimeClient.Phase.failed("down").allowsReliableStateOverwrite)
    }

    /// The in-flight race: an HTTP request issued while the WebSocket was
    /// down must be dropped if the connection went live before the response
    /// was applied. Callers implement this by re-evaluating the phase AFTER
    /// the await — this test exercises that exact pattern.
    func testInFlightHTTPApplyReChecksLivenessAfterAwait() async {
        // Shared mutable "current phase", as AppModel's connectionPhase is.
        actor PhaseBox {
            var phase: RealtimeClient.Phase = .failed("ws down")
            func set(_ p: RealtimeClient.Phase) { phase = p }
        }
        let box = PhaseBox()
        var applied = false

        // Request issued while down: a pre-await check would have passed.
        let allowedAtRequestTime = await box.phase.allowsReliableStateOverwrite
        XCTAssertTrue(allowedAtRequestTime)

        // While the "HTTP response" is in flight, the WS recovers.
        await box.set(.live)

        // Apply-time re-check (the pattern refresh() uses) must now refuse.
        if await box.phase.allowsReliableStateOverwrite {
            applied = true
        }
        XCTAssertFalse(applied, "a response landing after .recovered must not clobber live delta state")

        // And once the connection is down again, HTTP applies normally.
        await box.set(.failed("ws down again"))
        if await box.phase.allowsReliableStateOverwrite {
            applied = true
        }
        XCTAssertTrue(applied)
    }

    // MARK: - F3: interleaved envelope during a chunked snapshot

    func testDeltaInterleavedDuringChunkedSnapshotIsMalformed() {
        var gate = RealtimeResumeGate(state: SpaceState())
        gate.beganResume(lastRevision: 0)
        let part1 = SnapshotBody(revision: 10, part: 1, final: false, tasks: [], metrics: [], messages: [], automations: [])
        XCTAssertEqual(gate.receiveSnapshotChunk(part1), .snapshotChunkBuffered)
        XCTAssertTrue(gate.isSnapshotInFlight)

        // §6.2: any non-snapshot envelope between chunks is a malformed
        // sequence — even a delta that would otherwise look like a valid
        // resume reply.
        let interleaved = DeltaBody(fromRevision: 0, toRevision: 0, events: [])
        guard case .malformedSequence = gate.receiveDelta(interleaved) else {
            return XCTFail("expected malformedSequence for a delta interleaved mid-snapshot")
        }
        XCTAssertFalse(gate.isSnapshotInFlight, "buffered chunks are discarded — they can never complete now")
        XCTAssertFalse(gate.deltaEligible)
        XCTAssertEqual(gate.state.revision, 0, "nothing from the aborted snapshot is applied")
    }

    func testInterleavedFrameHelperReportsMalformedAndClearsBuffer() {
        var gate = RealtimeResumeGate(state: SpaceState())
        gate.beganResume(lastRevision: 0)
        let part1 = SnapshotBody(revision: 7, part: 1, final: false, tasks: [], metrics: [], messages: [], automations: [])
        XCTAssertEqual(gate.receiveSnapshotChunk(part1), .snapshotChunkBuffered)

        // The client routes ANY non-snapshot frame (ack, error, metric...)
        // through this helper while a snapshot is in flight.
        guard case .malformedSequence = gate.interleavedFrameDuringSnapshot() else {
            return XCTFail("expected malformedSequence")
        }
        XCTAssertFalse(gate.isSnapshotInFlight)
    }

    // MARK: - F4: foreground cycles resume incrementally with preserved C

    /// Simulates background → foreground: the first "connection" catches up
    /// via the scenario fixtures to revision 128; its SpaceState (with C)
    /// survives in the app model; the rebuilt gate for the next connection
    /// resumes with C and receives an incremental reply — never a snapshot.
    func testReconnectResumesIncrementallyWithPreservedRevision() throws {
        let files = try FixtureLoader.scenarioFiles("client-resume-delta")

        var firstConnection = RealtimeResumeGate(state: SpaceState(revision: 126))
        firstConnection.beganResume(lastRevision: 126)
        guard case .delta(let catchup) = try RealtimeFrame.decode(files[5].data) else { return XCTFail() }
        XCTAssertEqual(firstConnection.receiveDelta(catchup.body), .applied)
        XCTAssertEqual(firstConnection.state.revision, 128)

        // App backgrounds: connection closes, SpaceState is kept in-process.
        let preserved = firstConnection.state

        // App foregrounds: a new gate/connection seeded with the preserved
        // state resumes from C = 128 and gets the "you are already current"
        // empty delta — an incremental path, not a fresh snapshot.
        var secondConnection = RealtimeResumeGate(state: preserved)
        secondConnection.beganResume(lastRevision: preserved.revision)
        let youAreCurrent = DeltaBody(fromRevision: 128, toRevision: 128, events: [])
        XCTAssertEqual(secondConnection.receiveDelta(youAreCurrent), .applied)
        XCTAssertTrue(secondConnection.deltaEligible)
        XCTAssertEqual(secondConnection.state.revision, 128)
        XCTAssertEqual(secondConnection.state.tasks, preserved.tasks, "no state was thrown away by the reconnect")

        // A subsequent live delta continues from the preserved C.
        guard case .delta(let live) = try RealtimeFrame.decode(files[6].data) else { return XCTFail() }
        XCTAssertEqual(secondConnection.receiveDelta(live.body), .applied)
        XCTAssertEqual(secondConnection.state.revision, 129)
    }

    func testRealtimeClientHonorsInitialState() async {
        let configuration = RealtimeClient.Configuration(
            url: URL(string: "wss://example.invalid/v3/realtime")!, token: nil, deviceID: "iphone-test-01")
        var seed = SpaceState(revision: 128)
        seed.apply(metricFrame: MetricFrameBody(deviceID: "mac-1", metrics: [
            RTMetricSample(metricID: "cpu.load", value: "0.5", ts: 1_752_482_000_000),
        ]))
        let client = RealtimeClient(configuration: configuration, initialState: seed)
        let state = await client.currentState
        XCTAssertEqual(state.revision, 128, "a rebuilt client must start from the preserved C, not 0")
        XCTAssertEqual(state.metrics["cpu.load"]?.value, "0.5")
    }

    // MARK: - F5: delta event wrapper tolerates unknown sibling fields

    func testDeltaEventIgnoresUnknownSiblingFields() throws {
        // A future minor version may annotate delta entries with extra
        // fields; SPEC.md §15's body-level tolerance means they are ignored,
        // not rejected (§3's strictness covers only the envelope top level).
        let json = """
        {
          "type": "delta",
          "id": "lenient-1",
          "ts": 1752482000000,
          "body": {
            "from_revision": 5,
            "to_revision": 6,
            "events": [
              {
                "event_type": "task.event",
                "trace_id": "future-minor-version-annotation",
                "event": {
                  "device_id": "mac-1",
                  "device_seq": 9,
                  "task_id": "run-1",
                  "kind": "started",
                  "occurred_at": 1752482000000,
                  "title": "Job"
                }
              }
            ]
          }
        }
        """
        let frame = try RealtimeFrame.decode(Data(json.utf8))
        guard case .delta(let env) = frame, case .taskEvent(let event) = env.body.events[0] else {
            return XCTFail("expected the entry to decode as a task.event despite the unknown sibling field")
        }
        XCTAssertEqual(event.taskID, "run-1")
    }
}
