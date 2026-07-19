import XCTest
@testable import SitrepKit

/// Drives `RealtimeResumeGate` — the same gating logic `RealtimeClient` uses
/// — directly with the scenario fixtures under `proto/realtime/fixtures/scenarios/`,
/// feeding each directory's messages in filename order (as `tools/validate.js`
/// does) and asserting the terminal state matches each scenario's own
/// README. This exercises the viewer-relevant scenarios end to end without
/// a real WebSocket transport.
final class RealtimeResumeGateScenarioTests: XCTestCase {
    /// Decodes one scenario fixture file, expecting envelope `type` to be
    /// one of the given cases (a lightweight helper since scenario fixtures
    /// are plain envelopes, not the `{sender_role, frame}` wrapper).
    private func decode(_ data: Data) throws -> RealtimeFrame {
        try RealtimeFrame.decode(data)
    }

    // MARK: - client-resume-delta

    /// 126 -> (catch-up delta, 126->128) -> (live delta, 128->129).
    func testClientResumeDelta() throws {
        let files = try FixtureLoader.scenarioFiles("client-resume-delta")
        var gate = RealtimeResumeGate(state: SpaceState(revision: 126))

        // 01 hello offer / 02 hello accept — connection-setup, not gate concerns.
        // 03 subscribe / 04 ack — lease, not gate concerns.
        // 05 resume{last_revision: 126}
        let resumeFrame = try decode(files[4].data)
        guard case .resume(let resumeEnv) = resumeFrame else { return XCTFail("expected resume") }
        XCTAssertEqual(resumeEnv.body.lastRevision, 126)
        gate.beganResume(lastRevision: resumeEnv.body.lastRevision)
        XCTAssertFalse(gate.deltaEligible, "must not be delta-eligible before the resume reply")

        // 06 catch-up delta 126 -> 128 (the resume reply itself)
        guard case .delta(let catchupEnv) = try decode(files[5].data) else { return XCTFail("expected delta") }
        XCTAssertEqual(gate.receiveDelta(catchupEnv.body), .applied)
        XCTAssertTrue(gate.deltaEligible, "connection becomes delta-eligible on its first snapshot-or-delta reply")
        XCTAssertEqual(gate.state.revision, 128)
        XCTAssertEqual(gate.state.tasks["run-2f9a"]?.percent, 40)
        XCTAssertEqual(gate.state.messages.count, 1)

        // 07 live delta 128 -> 129
        guard case .delta(let liveEnv) = try decode(files[6].data) else { return XCTFail("expected delta") }
        XCTAssertEqual(gate.receiveDelta(liveEnv.body), .applied)
        XCTAssertEqual(gate.state.revision, 129)
        XCTAssertEqual(gate.state.tasks["run-2f9a"]?.percent, 60)

        XCTAssertEqual(files.count, 7)
    }

    /// A delta that arrives before the resume reply must be discarded
    /// (SPEC.md §6.3's defense-in-depth rule), never applied and never
    /// advancing C.
    func testClientResumeDeltaDiscardsPreReplyDelta() throws {
        let files = try FixtureLoader.scenarioFiles("client-resume-delta")
        var gate = RealtimeResumeGate(state: SpaceState(revision: 126))
        gate.beganResume(lastRevision: 126)

        // Simulate a (non-conformant-server) delta racing ahead of the
        // reply: feed the LIVE delta (from step 07, from_revision 128)
        // before the catch-up reply (step 06).
        guard case .delta(let liveEnv) = try decode(files[6].data) else { return XCTFail() }
        XCTAssertEqual(gate.receiveDelta(liveEnv.body), .discarded)
        XCTAssertEqual(gate.state.revision, 126, "a pre-reply delta must not advance C")
        XCTAssertFalse(gate.deltaEligible)

        // The real reply still applies normally afterward.
        guard case .delta(let catchupEnv) = try decode(files[5].data) else { return XCTFail() }
        XCTAssertEqual(gate.receiveDelta(catchupEnv.body), .applied)
        XCTAssertEqual(gate.state.revision, 128)
    }

    // MARK: - client-revision-gap-snapshot

    /// last_revision 40, server can't serve incrementally -> two-chunk
    /// snapshot at revision 150.
    func testClientRevisionGapSnapshot() throws {
        let files = try FixtureLoader.scenarioFiles("client-revision-gap-snapshot")
        XCTAssertEqual(files.count, 7)
        var gate = RealtimeResumeGate(state: SpaceState(revision: 999)) // arbitrary stale prior state

        guard case .resume(let resumeEnv) = try decode(files[4].data) else { return XCTFail("expected resume") }
        XCTAssertEqual(resumeEnv.body.lastRevision, 40)
        gate.beganResume(lastRevision: resumeEnv.body.lastRevision)

        guard case .snapshot(let part1) = try decode(files[5].data) else { return XCTFail("expected snapshot part 1") }
        XCTAssertEqual(part1.body.part, 1)
        XCTAssertFalse(part1.body.final)
        XCTAssertEqual(gate.receiveSnapshotChunk(part1.body), .snapshotChunkBuffered)
        XCTAssertFalse(gate.deltaEligible, "nothing applies until the final chunk")
        XCTAssertEqual(gate.state.revision, 999, "prior stale state must not change before the final chunk")

        guard case .snapshot(let part2) = try decode(files[6].data) else { return XCTFail("expected snapshot part 2") }
        XCTAssertEqual(part2.body.part, 2)
        XCTAssertTrue(part2.body.final)
        XCTAssertEqual(gate.receiveSnapshotChunk(part2.body), .applied)

        XCTAssertTrue(gate.deltaEligible)
        XCTAssertEqual(gate.state.revision, 150)
        // Concatenated across both chunks.
        XCTAssertEqual(gate.state.tasks.count, 1)
        XCTAssertEqual(gate.state.automations.count, 1)
        XCTAssertEqual(gate.state.metrics.count, 1)
        XCTAssertEqual(gate.state.messages.count, 1)

        // A live delta with from_revision == 150 is now applicable.
        var followUp = gate
        let liveDelta = DeltaBody(
            fromRevision: 150, toRevision: 151,
            events: [.taskEvent(TaskEventBody(
                deviceID: "mac-quintin-01", deviceSeq: 200, taskID: "run-2f9a", kind: .done, occurredAt: 1_752_483_100_000))])
        XCTAssertEqual(followUp.receiveDelta(liveDelta), .applied)
        XCTAssertEqual(followUp.state.revision, 151)
    }

    /// A non-conformant server interleaving something other than the next
    /// chunk (or ping/pong, which never reaches the gate at all — see
    /// `RealtimeClient.receiveFrame`) between snapshot chunks must be
    /// treated as a malformed sequence.
    func testSnapshotChunkOutOfSequenceIsMalformed() {
        var gate = RealtimeResumeGate(state: SpaceState())
        gate.beganResume(lastRevision: 0)
        let part1 = SnapshotBody(revision: 10, part: 1, final: false, tasks: [], metrics: [], messages: [], automations: [])
        XCTAssertEqual(gate.receiveSnapshotChunk(part1), .snapshotChunkBuffered)

        // Wrong revision.
        let wrongRevision = SnapshotBody(revision: 11, part: 2, final: true, tasks: [], metrics: [], messages: [], automations: [])
        guard case .malformedSequence = gate.receiveSnapshotChunk(wrongRevision) else {
            return XCTFail("expected malformed sequence for a revision change mid-snapshot")
        }
    }

    // MARK: - gap detection -> re-resume, and revision_unavailable -> retry with 0

    func testGapDetectionTriggersReResume() {
        var gate = RealtimeResumeGate(state: SpaceState())
        gate.beganResume(lastRevision: 0)
        let empty = DeltaBody(fromRevision: 0, toRevision: 0, events: [])
        XCTAssertEqual(gate.receiveDelta(empty), .applied)
        XCTAssertTrue(gate.deltaEligible)

        // A delta arrives with from_revision > C: a gap.
        let gapped = DeltaBody(
            fromRevision: 5, toRevision: 6,
            events: [.taskEvent(TaskEventBody(deviceID: "d1", deviceSeq: 1, taskID: "t1", kind: .started, occurredAt: 1_752_000_000_000))])
        guard case .gapDetected(let resendFrom) = gate.receiveDelta(gapped) else {
            return XCTFail("expected a gap")
        }
        XCTAssertEqual(resendFrom, 0, "must resend resume with the last revision actually applied")
        XCTAssertEqual(gate.state.revision, 0, "the gapped delta itself is never applied")
        XCTAssertFalse(gate.deltaEligible, "not delta-eligible again until the fresh resume's reply")

        // Simulate the caller re-issuing resume and the server replying.
        gate.beganResume(lastRevision: resendFrom)
        let reply = DeltaBody(fromRevision: 0, toRevision: 1,
                               events: [.taskEvent(TaskEventBody(deviceID: "d1", deviceSeq: 1, taskID: "t1", kind: .started, occurredAt: 1_752_000_000_000))])
        XCTAssertEqual(gate.receiveDelta(reply), .applied)
        XCTAssertEqual(gate.state.revision, 1)
    }

    /// A stray live delta racing an outstanding re-resume (from_revision
    /// doesn't match what was requested) must be discarded, not misapplied
    /// as if it were the reply and not re-triggering yet another resume.
    func testRacingDeltaDuringOutstandingResumeIsDiscarded() {
        var gate = RealtimeResumeGate(state: SpaceState(revision: 5))
        gate.beganResume(lastRevision: 5)
        let racing = DeltaBody(
            fromRevision: 7, toRevision: 8,
            events: [.taskEvent(TaskEventBody(deviceID: "d1", deviceSeq: 9, taskID: "t1", kind: .progress, occurredAt: 1_752_000_000_000, percent: 10))])
        XCTAssertEqual(gate.receiveDelta(racing), .discarded)
        XCTAssertEqual(gate.state.revision, 5)
        XCTAssertNotNil(gate.awaitingResumeReply, "still waiting for the true reply")
    }

    func testRevisionUnavailableRetriesWithZero() {
        var gate = RealtimeResumeGate(state: SpaceState(revision: 200))
        gate.beganResume(lastRevision: 200)
        let error = ErrorBody(code: .revisionUnavailable, message: "gone", retryable: true, fatal: false)
        XCTAssertTrue(gate.receiveErrorAnswersOutstandingResume(error))
        XCTAssertEqual(gate.awaitingResumeReply, 0)

        let snapshot = SnapshotBody(revision: 5, part: 1, final: true, tasks: [], metrics: [], messages: [], automations: [])
        XCTAssertEqual(gate.receiveSnapshotChunk(snapshot), .applied)
        XCTAssertEqual(gate.state.revision, 5)
    }

    // MARK: - interest-lease-expiry (viewer-visible parts: subscribe/renew acks and the broadcast commands)

    func testInterestLeaseExpiryScenarioShapes() throws {
        let files = try FixtureLoader.scenarioFiles("interest-lease-expiry")
        XCTAssertEqual(files.count, 6)

        guard case .ack(let ack1) = try decode(files[1].data), let lease1 = ack1.body.lease else {
            return XCTFail("expected subscribe ack with lease")
        }
        XCTAssertEqual(lease1.expiresAt - ack1.ts, 45000) // within [30000, 60000] of envelope ts

        guard case .command(let throttle) = try decode(files[2].data) else { return XCTFail("expected throttle command") }
        XCTAssertEqual(throttle.body.origin, .server)
        XCTAssertEqual(throttle.body.action, .throttle)
        XCTAssertNil(throttle.body.targetDeviceID, "broadcasts to every source device in the space")
        // A viewer must never be allowed to originate this itself.
        XCTAssertFalse(RealtimeAuthorization.clientMaySend(.command(throttle), as: .viewer))

        guard case .ack(let ack2) = try decode(files[4].data), let lease2 = ack2.body.lease else {
            return XCTFail("expected second viewer's subscribe ack with lease")
        }
        XCTAssertTrue((30_000...60_000).contains(lease2.expiresAt - ack2.ts))

        guard case .command(let resumeRate) = try decode(files[5].data) else { return XCTFail("expected resume_rate command") }
        XCTAssertEqual(resumeRate.body.origin, .server)
        XCTAssertEqual(resumeRate.body.action, .resumeRate)
    }

    // MARK: - duplicate-connection-supersede

    func testDuplicateConnectionSupersedeScenarioShapes() throws {
        let files = try FixtureLoader.scenarioFiles("duplicate-connection-supersede")
        XCTAssertEqual(files.count, 5)

        guard case .hello(let offer1) = try decode(files[0].data), case .offer(let o1) = offer1.body else {
            return XCTFail("expected hello offer")
        }
        guard case .hello(let offer2) = try decode(files[2].data), case .offer(let o2) = offer2.body else {
            return XCTFail("expected second hello offer")
        }
        XCTAssertEqual(o1.deviceID, o2.deviceID, "same device_id opens a second connection")

        guard case .error(let superseded) = try decode(files[4].data) else { return XCTFail("expected superseded error") }
        XCTAssertEqual(superseded.body.code, .superseded)
        XCTAssertTrue(superseded.body.fatal)
        XCTAssertFalse(superseded.body.retryable, "not retryable on the dying connection itself")
    }
}
