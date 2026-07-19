import Foundation

// Mirrors proto/schema/event.schema.json. Keep field names in lockstep with
// the spec; the daemon and server are the source of truth.

public enum TaskStatus: String, Codable, Sendable {
    case running, done, failed
}

public struct TaskState: Codable, Identifiable, Sendable, Equatable {
    public var sourceID: String
    public var title: String
    public var status: TaskStatus
    public var percent: Int?
    public var step: String?
    public var updatedAt: Date
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

    enum CodingKeys: String, CodingKey {
        case sourceID = "source_id"
        case title, status, percent, step, icon, tint, template
        case updatedAt = "updated_at"
        case startedAt = "started_at"
    }
}

public enum TaskCommand: String, Sendable {
    case pause, resume, stop
}

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
    /// links the metric to its schedule controls.
    public var source: String?
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

public struct AutomationInfo: Codable, Identifiable, Sendable, Equatable {
    public var id: String
    public var name: String
    public var executor: Executor
    public var schedule: Schedule
    public var state: State
    public var lastRun: Date?

    public init(id: String, name: String, executor: Executor, schedule: Schedule, state: State, lastRun: Date? = nil) {
        self.id = id
        self.name = name
        self.executor = executor
        self.schedule = schedule
        self.state = state
        self.lastRun = lastRun
    }

    public struct Executor: Codable, Sendable, Equatable {
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

    enum CodingKeys: String, CodingKey {
        case id, name, executor, schedule, state
        case lastRun = "last_run_at"
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

/// The v2 read model. Supporting concepts are nested under their owner, so
/// the application never fetches or guesses global parameters/alerts.
public struct SpaceSnapshot: Codable, Sendable {
    public var version: Int
    public var generatedAt: Date
    public var presence: PresenceInfo
    public var tasks: [TaskState]
    public var metrics: [SnapshotMetric]
    public var messages: [SnapshotMessage]
    public var automations: [AutomationInfo]
    /// Server-declared realtime capability gate. Older servers never send
    /// this field, and its absence must mean "no realtime" — decode
    /// missing/null as `false`, never infer availability from anything else
    /// (e.g. the presence of a `/v3/realtime` URL shape).
    public var realtimeEnabled: Bool

    enum CodingKeys: String, CodingKey {
        case version, presence, tasks, metrics, messages, automations
        case generatedAt = "generated_at"
        case realtimeEnabled = "realtime_enabled"
    }

    public init(
        version: Int, generatedAt: Date, presence: PresenceInfo, tasks: [TaskState],
        metrics: [SnapshotMetric], messages: [SnapshotMessage], automations: [AutomationInfo],
        realtimeEnabled: Bool = false
    ) {
        self.version = version
        self.generatedAt = generatedAt
        self.presence = presence
        self.tasks = tasks
        self.metrics = metrics
        self.messages = messages
        self.automations = automations
        self.realtimeEnabled = realtimeEnabled
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        version = try container.decode(Int.self, forKey: .version)
        generatedAt = try container.decode(Date.self, forKey: .generatedAt)
        presence = try container.decode(PresenceInfo.self, forKey: .presence)
        tasks = try container.decode([TaskState].self, forKey: .tasks)
        metrics = try container.decode([SnapshotMetric].self, forKey: .metrics)
        messages = try container.decode([SnapshotMessage].self, forKey: .messages)
        automations = try container.decode([AutomationInfo].self, forKey: .automations)
        realtimeEnabled = try container.decodeIfPresent(Bool.self, forKey: .realtimeEnabled) ?? false
    }
}

public struct SnapshotMetric: Codable, Sendable {
    public var id: String
    public var value: String
    public var label: String?
    public var updatedAt: Date
    public var automationID: String?
    public var display: Display
    public var thresholds: [Threshold]
    public var history: [Double]?

    public struct Display: Codable, Sendable {
        public var icon: String?
        public var tint: String?
        public var style: String?
        public var target: String?
        public var min: String?
        public var max: String?
    }

    public struct Threshold: Codable, Identifiable, Sendable {
        public var id: String
        public var direction: String
        public var value: String
        public var severity: String
    }

    enum CodingKeys: String, CodingKey {
        case id, value, label, display, thresholds, history
        case updatedAt = "updated_at"
        case automationID = "automation_id"
    }

    public var state: MetricState {
        var result = MetricState(key: id, value: value, label: label, updatedAt: updatedAt)
        result.icon = display.icon
        result.tint = display.tint
        result.template = display.style
        result.target = display.target
        result.min = display.min
        result.max = display.max
        result.alertAbove = thresholds.first { $0.direction == "above" }?.value
        result.alertBelow = thresholds.first { $0.direction == "below" }?.value
        result.source = automationID.map { "a\($0)" }
        result.history = history
        return result
    }
}

public struct SnapshotMessage: Codable, Sendable {
    public var id: String
    public var body: String
    public var severity: String
    public var createdAt: Date
    public var automationID: String?

    enum CodingKeys: String, CodingKey {
        case id, body, severity
        case createdAt = "created_at"
        case automationID = "automation_id"
    }

    public var state: EventLogEntry {
        EventLogEntry(
            serverID: id,
            text: body,
            level: severity == "critical" ? "error" : severity == "warning" ? "warn" : "info",
            ts: createdAt,
            source: automationID.map { "a\($0)" }
        )
    }
}

/// One timestamped point of a metric's history.
public struct SeriesPoint: Codable, Sendable, Identifiable, Equatable {
    public var t: Date
    public var v: Double
    public var id: Date { t }
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

/// Computer-side heartbeats included in the space snapshot.
public struct PresenceInfo: Codable, Sendable, Equatable {
    public var ingestLastSeen: Date?
    public var agentLastSeen: Date?
    public var now: Date?

    enum CodingKeys: String, CodingKey {
        case ingestLastSeen = "ingest_last_seen"
        case agentLastSeen = "agent_last_seen"
        case now
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
