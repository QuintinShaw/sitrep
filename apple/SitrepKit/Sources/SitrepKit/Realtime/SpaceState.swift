import Foundation

/// Unified realtime read model for one space: `tasks`/`messages`/
/// `automations`/`metrics` plus the local `revision` (`C` in SPEC.md §6.3).
///
/// Two convergent paths build this exact same shape (§6.4): `apply(snapshot:)`
/// replaces it wholesale from a resume reply's folded state, and
/// `apply(delta:)` folds reliable events into it one at a time. Both MUST
/// reach identical `tasks`/`automations` state for the same event history —
/// that equivalence is what `SpaceStateFoldingTests` asserts.
///
/// This is a plain value type with no transport/connection knowledge;
/// `RealtimeClient` owns the connection state machine and mutates one of
/// these as frames arrive.
public struct SpaceState: Sendable, Equatable {
    /// Local current revision (`C`). 0 before the first resume reply.
    public private(set) var revision: Int

    public private(set) var tasks: [String: RTTaskState] = [:]
    public private(set) var automations: [String: RTAutomationState] = [:]
    /// Oldest-first. Capped locally (`messageWindow`) so a very long-lived
    /// connection's delta-appended history doesn't grow unbounded — the
    /// server's own snapshot window (SPEC.md §6.4 recommends N=200) bounds
    /// what a fresh snapshot carries, but nothing bounds how many
    /// `message.event`s a live connection can accumulate over days, so the
    /// client enforces its own cap defensively. This is a client-local
    /// memory bound, not a protocol requirement.
    public private(set) var messages: [RTMessageRecord] = []
    /// Latest known sample per metric_id. `metric.frame` is best-effort and
    /// out-of-band from `revision` (§12); ordering is enforced purely by
    /// `ts`, independent of arrival or revision order.
    public private(set) var metrics: [String: RTMetricSample] = [:]

    public var messageWindow: Int = 200

    public init(revision: Int = 0) {
        self.revision = revision
    }

    /// Replaces all four collections wholesale from an aggregated (already
    /// chunk-reassembled) snapshot, and sets `revision` to the snapshot's
    /// revision. This is always safe to call — a snapshot is the
    /// authoritative folded state at its revision, never a partial update.
    public mutating func apply(snapshot: SnapshotBody) {
        revision = snapshot.revision
        tasks = Dictionary(uniqueKeysWithValues: snapshot.tasks.map { ($0.taskID, $0) })
        automations = Dictionary(uniqueKeysWithValues: snapshot.automations.map { ($0.automationID, $0) })
        messages = Array(snapshot.messages.suffix(messageWindow))
        metrics = Dictionary(uniqueKeysWithValues: snapshot.metrics.map { ($0.metricID, $0) })
    }

    /// Folds one delta's events into state and advances `revision` to
    /// `delta.toRevision`. The caller (`RealtimeClient`) is responsible for
    /// the §6.3 gating decision (apply only when `fromRevision == revision`)
    /// — this method unconditionally applies, so tests and the client share
    /// one folding implementation without duplicating the gate.
    public mutating func apply(delta: DeltaBody) {
        for event in delta.events {
            switch event {
            case .taskEvent(let e): foldTaskEvent(e)
            case .messageEvent(let e): foldMessageEvent(e)
            case .configEvent(let e): foldConfigEvent(e)
            case .unknown: break // §15-style forward-compat: ignore what we don't recognize.
            }
        }
        revision = delta.toRevision
    }

    /// `metric.frame` is out-of-band from `revision` (§12): apply
    /// independent of delta/snapshot state, discarding any sample whose
    /// `ts` does not strictly advance past the last-applied `ts` for that
    /// `metric_id`.
    public mutating func apply(metricFrame: MetricFrameBody) {
        for sample in metricFrame.metrics {
            if let existing = metrics[sample.metricID], sample.ts <= existing.ts { continue }
            metrics[sample.metricID] = sample
        }
    }

    // MARK: - §6.4 deterministic folding

    private mutating func foldTaskEvent(_ e: TaskEventBody) {
        var state = tasks[e.taskID] ?? RTTaskState(
            taskID: e.taskID, deviceID: e.deviceID, title: nil, state: .running,
            percent: nil, step: nil, message: nil, updatedAt: e.occurredAt, display: nil)

        switch e.kind {
        case .started, .progress, .step:
            state.state = .running
        case .done:
            state.state = .done
        case .failed:
            state.state = .failed
        }

        if let title = e.title, !title.isEmpty {
            state.title = title
        }

        switch e.kind {
        case .started, .progress, .step:
            if let percent = e.percent { state.percent = percent }
            if let step = e.step { state.step = step }
        case .done:
            state.percent = 100
            state.step = nil
        case .failed:
            // percent keeps its last running value; step cleared.
            state.step = nil
        }

        switch e.kind {
        case .done, .failed:
            if let message = e.message { state.message = message }
        case .started, .progress, .step:
            break // message stays absent while running (never set here).
        }

        if let display = e.display { state.display = display }
        state.updatedAt = e.occurredAt

        tasks[e.taskID] = state
    }

    private mutating func foldMessageEvent(_ e: MessageEventBody) {
        let record = RTMessageRecord(
            messageID: e.messageID, deviceID: e.deviceID, level: e.level, text: e.text, occurredAt: e.occurredAt)
        messages.append(record)
        if messages.count > messageWindow {
            messages.removeFirst(messages.count - messageWindow)
        }
    }

    private mutating func foldConfigEvent(_ e: ConfigEventBody) {
        switch e.kind {
        case .upserted:
            if let automation = e.automation {
                automations[e.automationID] = automation
            }
        case .removed:
            automations.removeValue(forKey: e.automationID)
        }
    }
}
