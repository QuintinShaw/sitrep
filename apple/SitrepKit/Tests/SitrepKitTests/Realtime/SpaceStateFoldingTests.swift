import XCTest
@testable import SitrepKit

/// SPEC.md §6.4: two viewers — one that replayed every delta, one that took
/// a snapshot — MUST converge on identical task and automation state for
/// the same event history. These tests build one event history, fold it
/// two ways (delta-by-delta vs. a directly-applied snapshot equivalent to
/// what a server would have computed), and assert the two `SpaceState`
/// values agree on `tasks`/`automations`.
final class SpaceStateFoldingTests: XCTestCase {
    private func taskEvent(
        seq: Int, kind: RTTaskKind, occurredAt: Int, title: String? = nil, percent: Int? = nil,
        step: String? = nil, message: String? = nil, display: RTDisplayHints? = nil
    ) -> TaskEventBody {
        TaskEventBody(
            deviceID: "mac-1", deviceSeq: seq, taskID: "run-1", kind: kind, occurredAt: occurredAt,
            title: title, percent: percent, step: step, message: message, display: display)
    }

    private func applyAsDeltaStream(_ events: [DeltaEvent]) -> SpaceState {
        var state = SpaceState()
        for (index, event) in events.enumerated() {
            state.apply(delta: DeltaBody(fromRevision: index, toRevision: index + 1, events: [event]))
        }
        return state
    }

    /// started -> progress(40, "uploading") -> progress(80, no title change) -> done("all good").
    func testTaskFoldingConvergesDeltaVsSnapshot() {
        let events: [DeltaEvent] = [
            .taskEvent(taskEvent(seq: 1, kind: .started, occurredAt: 1000, title: "Nightly backup", display: RTDisplayHints(icon: "moon"))),
            .taskEvent(taskEvent(seq: 2, kind: .progress, occurredAt: 2000, percent: 40, step: "uploading")),
            .taskEvent(taskEvent(seq: 3, kind: .progress, occurredAt: 3000, title: "", percent: 80)), // empty title must NOT overwrite
            .taskEvent(taskEvent(seq: 4, kind: .done, occurredAt: 4000, message: "all good")),
        ]

        let viaDeltas = applyAsDeltaStream(events)

        // A snapshot is the server's already-folded state at this revision;
        // construct it by hand per the exact §6.4 rules and apply it
        // wholesale, simulating "the other viewer took a snapshot".
        let expectedFolded = RTTaskState(
            taskID: "run-1", deviceID: "mac-1", title: "Nightly backup", state: .done,
            percent: 100, step: nil, message: "all good", updatedAt: 4000,
            display: RTDisplayHints(icon: "moon"))
        var viaSnapshot = SpaceState()
        viaSnapshot.apply(snapshot: SnapshotBody(
            revision: 4, part: 1, final: true, tasks: [expectedFolded], metrics: [], messages: [], automations: []))

        XCTAssertEqual(viaDeltas.tasks, viaSnapshot.tasks)
        XCTAssertEqual(viaDeltas.tasks["run-1"], expectedFolded)
    }

    func testTaskFoldingFailedKeepsLastPercentAndClearsStep() {
        let events: [DeltaEvent] = [
            .taskEvent(taskEvent(seq: 1, kind: .started, occurredAt: 1000, title: "Job")),
            .taskEvent(taskEvent(seq: 2, kind: .progress, occurredAt: 2000, percent: 55, step: "step 2")),
            .taskEvent(taskEvent(seq: 3, kind: .failed, occurredAt: 3000, message: "boom")),
        ]
        let state = applyAsDeltaStream(events)
        let task = try! XCTUnwrap(state.tasks["run-1"])
        XCTAssertEqual(task.state, .failed)
        XCTAssertEqual(task.percent, 55, "percent keeps its last running value on failure")
        XCTAssertNil(task.step, "step is cleared on done/failed")
        XCTAssertEqual(task.message, "boom")
    }

    func testTaskFoldingMessageStaysAbsentWhileRunning() {
        let events: [DeltaEvent] = [
            .taskEvent(taskEvent(seq: 1, kind: .started, occurredAt: 1000, title: "Job")),
            .taskEvent(taskEvent(seq: 2, kind: .progress, occurredAt: 2000, percent: 10)),
        ]
        let state = applyAsDeltaStream(events)
        XCTAssertNil(state.tasks["run-1"]?.message)
    }

    func testTaskFoldingDisplayReplacesWholesaleNotMerged() {
        let events: [DeltaEvent] = [
            .taskEvent(taskEvent(seq: 1, kind: .started, occurredAt: 1000, title: "Job", display: RTDisplayHints(icon: "a", tint: "blue"))),
            .taskEvent(taskEvent(seq: 2, kind: .progress, occurredAt: 2000, percent: 10, display: RTDisplayHints(tint: "red"))),
        ]
        let state = applyAsDeltaStream(events)
        // The second event's display object wins WHOLESALE: `icon` is gone,
        // not preserved from the first event.
        XCTAssertEqual(state.tasks["run-1"]?.display, RTDisplayHints(tint: "red"))
    }

    // MARK: - automation folding (config.event)

    func testAutomationUpsertThenRemoveConvergesDeltaVsSnapshot() {
        let automationV1 = RTAutomationState(
            automationID: "auto-1", name: "Disk watch", executorKind: "script",
            scheduleEverySeconds: 300, state: .active, lastRunAt: nil)
        let automationV2 = RTAutomationState(
            automationID: "auto-1", name: "Disk watch (paused)", executorKind: "script",
            scheduleEverySeconds: 600, state: .paused, lastRunAt: 5000)

        let events: [DeltaEvent] = [
            .configEvent(ConfigEventBody(kind: .upserted, automationID: "auto-1", automation: automationV1, occurredAt: 1000)),
            .configEvent(ConfigEventBody(kind: .upserted, automationID: "auto-1", automation: automationV2, occurredAt: 2000)),
        ]
        let viaDeltas = applyAsDeltaStream(events)
        XCTAssertEqual(viaDeltas.automations["auto-1"], automationV2, "upsert replaces wholesale, no per-field merge")

        let removed = events + [.configEvent(ConfigEventBody(kind: .removed, automationID: "auto-1", occurredAt: 3000))]
        let viaDeltasRemoved = applyAsDeltaStream(removed)
        XCTAssertNil(viaDeltasRemoved.automations["auto-1"])

        var viaSnapshot = SpaceState()
        viaSnapshot.apply(snapshot: SnapshotBody(
            revision: 2, part: 1, final: true, tasks: [], metrics: [], messages: [], automations: [automationV2]))
        XCTAssertEqual(viaDeltas.automations, viaSnapshot.automations)
    }

    // MARK: - message.event append-only history

    func testMessageEventsAppendInRevisionOrder() {
        let events: [DeltaEvent] = [
            .messageEvent(MessageEventBody(deviceID: "mac-1", deviceSeq: 1, messageID: "m1", level: .info, text: "first", occurredAt: 1000)),
            .messageEvent(MessageEventBody(deviceID: "mac-1", deviceSeq: 2, messageID: "m2", level: .warn, text: "second", occurredAt: 2000)),
        ]
        let state = applyAsDeltaStream(events)
        XCTAssertEqual(state.messages.map(\.text), ["first", "second"])
    }

    // MARK: - metric.frame ts-based discard

    func testMetricFrameDiscardsStaleOrDuplicateTimestamps() {
        var state = SpaceState()
        state.apply(metricFrame: MetricFrameBody(deviceID: "mac-1", metrics: [
            RTMetricSample(metricID: "cpu.load", value: "0.5", ts: 2000),
        ]))
        // Same ts again: discarded (ts <= last-applied ts).
        state.apply(metricFrame: MetricFrameBody(deviceID: "mac-1", metrics: [
            RTMetricSample(metricID: "cpu.load", value: "0.9", ts: 2000),
        ]))
        XCTAssertEqual(state.metrics["cpu.load"]?.value, "0.5")

        // Earlier ts: discarded.
        state.apply(metricFrame: MetricFrameBody(deviceID: "mac-1", metrics: [
            RTMetricSample(metricID: "cpu.load", value: "0.1", ts: 1000),
        ]))
        XCTAssertEqual(state.metrics["cpu.load"]?.value, "0.5")

        // Later ts: applied.
        state.apply(metricFrame: MetricFrameBody(deviceID: "mac-1", metrics: [
            RTMetricSample(metricID: "cpu.load", value: "0.7", ts: 3000),
        ]))
        XCTAssertEqual(state.metrics["cpu.load"]?.value, "0.7")
    }
}
