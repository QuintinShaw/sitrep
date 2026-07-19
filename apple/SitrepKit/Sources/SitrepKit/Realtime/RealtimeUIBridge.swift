import Foundation

// Adapters from the realtime wire model (RTTaskState/RTMetricSample/...) to
// the existing poll-era UI models (TaskState/MetricState/EventLogEntry/
// AutomationInfo), so SitrepApp's views keep reading the same types whether
// their data came from an HTTP snapshot or from `SpaceState`. This mirrors
// the existing `SnapshotMetric.state`/`SnapshotMessage.state` adapters in
// Models.swift, which do the equivalent job for the REST `/v2/snapshot`
// response shape.

private extension Date {
    init(msSinceEpoch ms: Int) { self.init(timeIntervalSince1970: Double(ms) / 1000) }
}

public extension RTTaskState {
    var uiState: TaskState {
        let status: TaskStatus
        switch state {
        case .running: status = .running
        case .done: status = .done
        case .failed: status = .failed
        }
        var t = TaskState(
            sourceID: taskID, title: title ?? "", status: status, percent: percent, step: step,
            updatedAt: Date(msSinceEpoch: updatedAt))
        t.icon = display?.icon
        t.tint = display?.tint
        t.template = display?.template
        return t
    }
}

public extension RTMetricSample {
    var uiState: MetricState {
        var m = MetricState(key: metricID, value: value, label: label, updatedAt: Date(msSinceEpoch: ts))
        m.icon = display?.icon
        m.tint = display?.tint
        m.template = display?.template
        m.target = target
        m.min = min
        m.max = max
        m.alertAbove = alertAbove
        m.alertBelow = alertBelow
        return m
    }
}

public extension RTMessageRecord {
    var uiEvent: EventLogEntry {
        EventLogEntry(serverID: messageID, text: text, level: level.rawValue, ts: Date(msSinceEpoch: occurredAt), source: nil)
    }
}

public extension RTAutomationState {
    var uiInfo: AutomationInfo {
        AutomationInfo(
            id: automationID, name: name,
            executor: .init(kind: executorKind),
            schedule: .init(kind: "interval", everySeconds: scheduleEverySeconds),
            state: state == .active ? .active : .paused,
            lastRun: lastRunAt.map { Date(msSinceEpoch: $0) })
    }
}

public extension SpaceState {
    var uiTasks: [TaskState] { tasks.values.map(\.uiState).sorted { $0.updatedAt > $1.updatedAt } }
    var uiMetrics: [MetricState] { metrics.values.map(\.uiState).sorted { $0.key < $1.key } }
    var uiEvents: [EventLogEntry] { messages.map(\.uiEvent) }
    var uiAutomations: [AutomationInfo] { automations.values.map(\.uiInfo).sorted { $0.name < $1.name } }
}
