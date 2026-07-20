import Foundation

// Mirrors docs/api/v1/openapi.yaml's component schemas (the frozen v1 HTTP
// contract). Keep field names/wire shapes in lockstep with that file; the
// server is the source of truth. Several v1 fields are Unix-ms integers
// where the old /v2 surface used ISO date strings (task/metric/message/
// automation timestamps) — only the top-level `generated_at` on a snapshot
// stays an ISO string. Each type below decodes accordingly with a manual
// `init(from:)` rather than relying on a single shared date strategy.

private extension Date {
    /// v1 timestamps outside `generated_at` are Unix milliseconds, not ISO
    /// strings (docs/api/v1/openapi.yaml) — this sidesteps APIClient's
    /// shared ISO-string date-decoding strategy for exactly those fields.
    init(unixMs ms: Int) { self.init(timeIntervalSince1970: Double(ms) / 1000) }
}

public enum TaskStatus: String, Codable, Sendable {
    case running, done, failed
}

/// Mirrors `#/components/schemas/TaskState` (v1-architecture.md §7). The
/// wire's `task_id`/`state` keys map onto this type's existing
/// `sourceID`/`status` Swift-side names to minimize disruption across the
/// many views that already read `TaskState.sourceID`.
public struct TaskState: Identifiable, Sendable, Equatable {
    public var sourceID: String
    public var title: String
    public var status: TaskStatus
    public var percent: Int?
    public var step: String?
    public var updatedAt: Date
    /// Not part of the v1 `TaskState` schema (no `started_at` field) — always
    /// nil when decoded from the wire; a timer-template Live Activity keeps
    /// its own local start time independent of this.
    public var startedAt: Date?
    public var icon: String?
    public var tint: String?
    public var template: String?

    public var id: String { sourceID }

    public init(sourceID: String, title: String, status: TaskStatus, percent: Int? = nil, step: String? = nil, updatedAt: Date) {
        self.sourceID = sourceID
        self.title = title
        self.status = status
        self.percent = percent
        self.step = step
        self.updatedAt = updatedAt
    }
}

extension TaskState: Codable {
    enum CodingKeys: String, CodingKey {
        case sourceID = "task_id"
        case title, state, percent, step, display
        case updatedAt = "updated_at"
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        sourceID = try c.decode(String.self, forKey: .sourceID)
        title = try c.decodeIfPresent(String.self, forKey: .title) ?? ""
        status = try c.decode(TaskStatus.self, forKey: .state)
        percent = try c.decodeIfPresent(Int.self, forKey: .percent)
        step = try c.decodeIfPresent(String.self, forKey: .step)
        updatedAt = Date(unixMs: try c.decode(Int.self, forKey: .updatedAt))
        startedAt = nil
        if let display = try c.decodeIfPresent(RTDisplayHints.self, forKey: .display) {
            icon = display.icon
            tint = display.tint
            template = display.template
        }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(sourceID, forKey: .sourceID)
        try c.encode(title, forKey: .title)
        try c.encode(status, forKey: .state)
        try c.encodeIfPresent(percent, forKey: .percent)
        try c.encodeIfPresent(step, forKey: .step)
        try c.encode(Int(updatedAt.timeIntervalSince1970 * 1000), forKey: .updatedAt)
        if icon != nil || tint != nil || template != nil {
            try c.encode(RTDisplayHints(icon: icon, tint: tint, template: template), forKey: .display)
        }
    }
}

public enum TaskCommand: String, Sendable {
    case pause, resume, stop
}

/// Mirrors `#/components/schemas/DeviceInfo`. `created_at`/`last_seen` are
/// ISO date-time strings the app never parses (displayed nowhere as a
/// `Date`), so they stay `String` — no decode change needed for v1.
public struct DeviceInfo: Codable, Identifiable, Sendable {
    public var id: String
    public var name: String
    public var role: String
    public var platform: String?
    public var createdAt: String
    public var lastSeen: String?

    enum CodingKeys: String, CodingKey {
        case id, name, role, platform
        case createdAt = "created_at"
        case lastSeen = "last_seen"
    }
}

public struct MetricState: Codable, Identifiable, Sendable, Equatable {
    public var key: String
    public var value: String
    public var label: String?
    public var updatedAt: Date
    public var icon: String?
    public var tint: String?
    public var template: String?
    public var target: String?
    public var min: String?
    public var max: String?
    /// Alert lines: the server notifies once per crossing; clients draw them.
    public var alertAbove: String?
    public var alertBelow: String?
    /// Source that last updated this metric ("a<id>" for automation runs) —
    /// links the metric to its schedule controls. Always nil under v1: the
    /// frozen `MetricSample`/`metric_sample` shapes carry no automation
    /// linkage field (see the port's final report for this contract gap).
    public var source: String?
    /// Inline sparkline history. Always nil under v1: `GET /v1/snapshot` and
    /// `GET /v1/metrics/:id` no longer embed one (contract gap, same note as
    /// `source` above) — only `GET /v1/metrics/:id/series` carries history now.
    public var history: [Double]?

    /// Gauge geometry derived from value/target/min/max; nil when the value
    /// isn't numeric.
    public var gauge: (value: Double, range: ClosedRange<Double>)? {
        guard let v = Double(value) else { return nil }
        let lo = min.flatMap(Double.init) ?? 0
        let hi = max.flatMap(Double.init) ?? target.flatMap(Double.init) ?? Swift.max(v, 1)
        guard hi > lo else { return nil }
        return (Swift.min(Swift.max(v, lo), hi), lo...hi)
    }

    public var id: String { key }

    public init(key: String, value: String, label: String? = nil, updatedAt: Date) {
        self.key = key
        self.value = value
        self.label = label
        self.updatedAt = updatedAt
    }

    enum CodingKeys: String, CodingKey {
        case key, value, label, icon, tint, template, target, min, max, source, history
        case alertAbove = "alert_above"
        case alertBelow = "alert_below"
        case updatedAt = "updated_at"
    }
}

/// Mirrors `#/components/schemas/AutomationState`. `executor_kind` is a flat
/// string on the wire; this type keeps its existing nested
/// `executor.kind` Swift shape (unchanged call sites) and remaps on decode.
public struct AutomationInfo: Identifiable, Sendable, Equatable {
    public var id: String
    public var name: String
    public var executor: Executor
    public var schedule: Schedule
    public var state: State
    public var lastRun: Date?
    /// Monotonic per-automation run counter, incremented by
    /// `POST /v1/automations/:id/run` (v1-architecture.md §5.1, P0-4). The
    /// resident agent — not the phone — keys off this to run exactly once
    /// when it advances. The app decodes it to stay wire-in-sync but never
    /// acts on it. Defaults to 0 ("never triggered") if an older server omits it.
    public var runRequestID: Int
    /// DISPLAY-ONLY unix-ms of the most recent `/run` (may be null/absent).
    /// The agent keys off `runRequestID`, never this timestamp.
    public var runRequestedAt: Date?

    public init(id: String, name: String, executor: Executor, schedule: Schedule, state: State,
                lastRun: Date? = nil, runRequestID: Int = 0, runRequestedAt: Date? = nil) {
        self.id = id
        self.name = name
        self.executor = executor
        self.schedule = schedule
        self.state = state
        self.lastRun = lastRun
        self.runRequestID = runRequestID
        self.runRequestedAt = runRequestedAt
    }

    public struct Executor: Sendable, Equatable {
        public var kind: String
        public init(kind: String) { self.kind = kind }
    }

    public struct Schedule: Codable, Sendable, Equatable {
        public var kind: String
        public var everySeconds: Int

        public init(kind: String, everySeconds: Int) {
            self.kind = kind
            self.everySeconds = everySeconds
        }

        enum CodingKeys: String, CodingKey {
            case kind
            case everySeconds = "every_seconds"
        }
    }

    public enum State: String, Codable, Sendable {
        case active, paused
    }

    public var everyS: Int { schedule.everySeconds }
    public var enabled: Bool { state == .active }
}

extension AutomationInfo: Codable {
    enum CodingKeys: String, CodingKey {
        case id = "automation_id"
        case name
        case executorKind = "executor_kind"
        case schedule, state
        case lastRun = "last_run_at"
        case runRequestID = "run_request_id"
        case runRequestedAt = "run_requested_at"
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        id = try c.decode(String.self, forKey: .id)
        name = try c.decode(String.self, forKey: .name)
        executor = Executor(kind: try c.decode(String.self, forKey: .executorKind))
        schedule = try c.decode(Schedule.self, forKey: .schedule)
        state = try c.decode(State.self, forKey: .state)
        lastRun = try c.decodeIfPresent(Int.self, forKey: .lastRun).map(Date.init(unixMs:))
        runRequestID = try c.decodeIfPresent(Int.self, forKey: .runRequestID) ?? 0
        runRequestedAt = try c.decodeIfPresent(Int.self, forKey: .runRequestedAt).map(Date.init(unixMs:))
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(id, forKey: .id)
        try c.encode(name, forKey: .name)
        try c.encode(executor.kind, forKey: .executorKind)
        try c.encode(schedule, forKey: .schedule)
        try c.encode(state, forKey: .state)
        try c.encodeIfPresent(lastRun.map { Int($0.timeIntervalSince1970 * 1000) }, forKey: .lastRun)
        try c.encode(runRequestID, forKey: .runRequestID)
        try c.encodeIfPresent(runRequestedAt.map { Int($0.timeIntervalSince1970 * 1000) }, forKey: .runRequestedAt)
    }
}

/// Minimal type-erased JSON value for PATCH bodies.
public enum AnyEncodableValue: Encodable, Sendable {
    case int(Int)
    case bool(Bool)
    case string(String)
    indirect case dictionary([String: AnyEncodableValue])

    public func encode(to encoder: Encoder) throws {
        var c = encoder.singleValueContainer()
        switch self {
        case .int(let v): try c.encode(v)
        case .bool(let v): try c.encode(v)
        case .string(let v): try c.encode(v)
        case .dictionary(let v): try c.encode(v)
        }
    }
}

/// The v1 read model (`GET /v1/snapshot`, docs/api/v1/openapi.yaml
/// `#/components/schemas/Snapshot`, v1-architecture.md §7). Supporting
/// concepts are nested under their owner, so the application never fetches
/// or guesses global parameters/alerts.
public struct SpaceSnapshot: Sendable {
    public var spaceRevision: Int
    public var generatedAt: Date
    public var capabilities: Capabilities
    public var presence: PresenceInfo
    public var tasks: [TaskState]
    public var metrics: [SnapshotMetric]
    public var messages: [SnapshotMessage]
    public var automations: [AutomationInfo]

    /// `#/components/schemas/Capabilities` (v1-architecture.md §8) — the
    /// transport/delivery kill switches. `wsTransportEnabled` is what feeds
    /// `RealtimeCapabilityGate`; this REPLACES the old top-level
    /// `realtime_enabled` boolean.
    public struct Capabilities: Sendable, Equatable {
        public var wsTransportEnabled: Bool
        public var apnsDeliveryEnabled: Bool
        public var protocolVersions: [Int]

        public init(wsTransportEnabled: Bool = false, apnsDeliveryEnabled: Bool = false, protocolVersions: [Int] = []) {
            self.wsTransportEnabled = wsTransportEnabled
            self.apnsDeliveryEnabled = apnsDeliveryEnabled
            self.protocolVersions = protocolVersions
        }
    }

    public init(
        spaceRevision: Int, generatedAt: Date, capabilities: Capabilities, presence: PresenceInfo,
        tasks: [TaskState], metrics: [SnapshotMetric], messages: [SnapshotMessage], automations: [AutomationInfo]
    ) {
        self.spaceRevision = spaceRevision
        self.generatedAt = generatedAt
        self.capabilities = capabilities
        self.presence = presence
        self.tasks = tasks
        self.metrics = metrics
        self.messages = messages
        self.automations = automations
    }
}

extension SpaceSnapshot: Decodable {
    private enum CodingKeys: String, CodingKey {
        case spaceRevision = "space_revision", generatedAt = "generated_at"
        case capabilities, presence, tasks, metrics, messages, automations
    }
    private enum CapabilitiesKeys: String, CodingKey {
        case wsTransportEnabled = "ws_transport_enabled"
        case apnsDeliveryEnabled = "apns_delivery_enabled"
        case protocolVersions = "protocol_versions"
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        spaceRevision = try container.decode(Int.self, forKey: .spaceRevision)
        // generated_at is the one field still an ISO string (v1-architecture.md
        // §7); this decode uses APIClient's shared custom Date strategy.
        generatedAt = try container.decode(Date.self, forKey: .generatedAt)
        // Missing/absent `capabilities` (an old/misbehaving server) must decode
        // as "nothing enabled" — never inferred from anything else, mirroring
        // the P0 fix this replaces (ModelsTests).
        if let capContainer = try? container.nestedContainer(keyedBy: CapabilitiesKeys.self, forKey: .capabilities) {
            capabilities = Capabilities(
                wsTransportEnabled: try capContainer.decodeIfPresent(Bool.self, forKey: .wsTransportEnabled) ?? false,
                apnsDeliveryEnabled: try capContainer.decodeIfPresent(Bool.self, forKey: .apnsDeliveryEnabled) ?? false,
                protocolVersions: try capContainer.decodeIfPresent([Int].self, forKey: .protocolVersions) ?? [])
        } else {
            capabilities = Capabilities()
        }
        presence = try container.decodeIfPresent(PresenceInfo.self, forKey: .presence) ?? PresenceInfo()
        tasks = try container.decode([TaskState].self, forKey: .tasks)
        metrics = try container.decode([SnapshotMetric].self, forKey: .metrics)
        messages = try container.decode([SnapshotMessage].self, forKey: .messages)
        automations = try container.decode([AutomationInfo].self, forKey: .automations)
    }
}

/// `#/components/schemas/MetricSample`. Note the v1 shape is FLATTER than
/// the old `/v2` snapshot's metric entry: `target`/`min`/`max`/`alert_above`/
/// `alert_below` are siblings of `display`, not nested inside it, and the
/// timestamp key is `ts` (Unix ms), not `updated_at` (ISO string).
public struct SnapshotMetric: Sendable {
    public var id: String
    public var value: String
    public var label: String?
    public var updatedAt: Date
    public var display: Display
    public var target: String?
    public var min: String?
    public var max: String?
    public var alertAbove: String?
    public var alertBelow: String?

    public struct Display: Sendable {
        public var icon: String?
        public var tint: String?
        public var template: String?
    }

    public var state: MetricState {
        var result = MetricState(key: id, value: value, label: label, updatedAt: updatedAt)
        result.icon = display.icon
        result.tint = display.tint
        result.template = display.template
        result.target = target
        result.min = min
        result.max = max
        result.alertAbove = alertAbove
        result.alertBelow = alertBelow
        // v1's MetricSample carries no automation-linkage field and no
        // inline history array (contract gap vs. the old /v2 shape — see
        // the port's final report). Client-local display overrides (icon/
        // tint/template/alert lines) are layered on top separately by
        // `MetricPreferencesStore.apply(to:)`.
        result.source = nil
        result.history = nil
        return result
    }
}

extension SnapshotMetric: Decodable {
    private enum CodingKeys: String, CodingKey {
        case id = "metric_id", value, label, display, target, min, max
        case updatedAt = "ts"
        case alertAbove = "alert_above", alertBelow = "alert_below"
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        id = try c.decode(String.self, forKey: .id)
        value = try c.decode(String.self, forKey: .value)
        label = try c.decodeIfPresent(String.self, forKey: .label)
        updatedAt = Date(unixMs: try c.decode(Int.self, forKey: .updatedAt))
        let hints = try c.decodeIfPresent(RTDisplayHints.self, forKey: .display)
        display = Display(icon: hints?.icon, tint: hints?.tint, template: hints?.template)
        target = try c.decodeIfPresent(String.self, forKey: .target)
        min = try c.decodeIfPresent(String.self, forKey: .min)
        max = try c.decodeIfPresent(String.self, forKey: .max)
        alertAbove = try c.decodeIfPresent(String.self, forKey: .alertAbove)
        alertBelow = try c.decodeIfPresent(String.self, forKey: .alertBelow)
    }
}

/// `#/components/schemas/MessageRecord`. `level` is already `info`/`warn`/
/// `error` on the wire (unlike the old `/v2` `severity` field, which used
/// `critical`/`warning`/`info` and needed remapping) — no translation needed.
public struct SnapshotMessage: Sendable {
    public var id: String
    public var deviceID: String
    public var level: String
    public var text: String
    public var occurredAt: Date

    public var state: EventLogEntry {
        EventLogEntry(serverID: id, text: text, level: level, ts: occurredAt, source: nil)
    }
}

extension SnapshotMessage: Decodable {
    private enum CodingKeys: String, CodingKey {
        case id = "message_id", deviceID = "device_id", level, text
        case occurredAt = "occurred_at"
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        id = try c.decode(String.self, forKey: .id)
        deviceID = try c.decode(String.self, forKey: .deviceID)
        level = try c.decode(String.self, forKey: .level)
        text = try c.decode(String.self, forKey: .text)
        occurredAt = Date(unixMs: try c.decode(Int.self, forKey: .occurredAt))
    }
}

/// One timestamped point of a metric's history
/// (`GET /v1/metrics/:id/series`). The wire uses `ts` (Unix ms) / `value`,
/// not the Swift-side short names `t`/`v`.
public struct SeriesPoint: Sendable, Identifiable, Equatable {
    public var t: Date
    public var v: Double
    public var id: Date { t }

    public init(t: Date, v: Double) {
        self.t = t
        self.v = v
    }
}

extension SeriesPoint: Codable {
    private enum CodingKeys: String, CodingKey { case t = "ts", v = "value" }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        t = Date(unixMs: try c.decode(Int.self, forKey: .t))
        v = try c.decode(Double.self, forKey: .v)
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(Int(t.timeIntervalSince1970 * 1000), forKey: .t)
        try c.encode(v, forKey: .v)
    }
}

public enum SeriesRange: String, CaseIterable, Sendable {
    case hour = "1h", sixHours = "6h", day = "1d", week = "1w", month = "1m", year = "1y"

    public var label: String {
        switch self {
        case .hour: "时"
        case .sixHours: "6时"
        case .day: "日"
        case .week: "周"
        case .month: "月"
        case .year: "年"
        }
    }
}

/// Computer-side heartbeats (`#/components/schemas/Presence`,
/// v1-architecture.md §7.1) — non-folded, outside `space_revision`
/// accounting. `sourcesOnline` is new in v1 (no v2 equivalent); the
/// green/amber/red status pill (`PresencePill` in MainTabView.swift) still
/// derives its state from `ingestLastSeen`/`agentLastSeen` alone, unchanged.
public struct PresenceInfo: Sendable, Equatable {
    public var ingestLastSeen: Date?
    public var agentLastSeen: Date?
    public var sourcesOnline: Int

    public init(ingestLastSeen: Date? = nil, agentLastSeen: Date? = nil, sourcesOnline: Int = 0) {
        self.ingestLastSeen = ingestLastSeen
        self.agentLastSeen = agentLastSeen
        self.sourcesOnline = sourcesOnline
    }
}

extension PresenceInfo: Decodable {
    private enum CodingKeys: String, CodingKey {
        case ingestLastSeen = "ingest_last_seen"
        case agentLastSeen = "agent_last_seen"
        case sourcesOnline = "sources_online"
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        ingestLastSeen = try c.decodeIfPresent(Int.self, forKey: .ingestLastSeen).map(Date.init(unixMs:))
        agentLastSeen = try c.decodeIfPresent(Int.self, forKey: .agentLastSeen).map(Date.init(unixMs:))
        sourcesOnline = try c.decodeIfPresent(Int.self, forKey: .sourcesOnline) ?? 0
    }
}

public struct EventLogEntry: Codable, Identifiable, Sendable, Equatable {
    /// Server-assigned id (deletable); older entries may predate it.
    public var serverID: String?
    public var text: String
    public var level: String
    public var ts: Date
    /// Emitting source ("a<id>" for automation runs) — the grouping key.
    public var source: String?

    public var id: String { serverID ?? "\(ts.timeIntervalSince1970)-\(text)" }

    public init(serverID: String?, text: String, level: String, ts: Date, source: String?) {
        self.serverID = serverID
        self.text = text
        self.level = level
        self.ts = ts
        self.source = source
    }

    enum CodingKeys: String, CodingKey {
        case serverID = "id"
        case text, level, ts, source
    }
}
