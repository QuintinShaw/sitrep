import Foundation

// Swift mirror of proto/realtime/. This is a derived artifact: field names,
// enum values, and validation rules MUST match the frozen schemas under
// proto/realtime/ byte-for-byte in semantics. Any deviation is a bug, not a
// style choice. Do not "clean up" a name here without checking the schema
// first.
//
// Strictness split (SPEC.md §3, §15):
// - The envelope's top level (type/id/ts/body) is permanently strict: an
//   unknown top-level field is always `malformed`. `Envelope` enforces this
//   explicitly (see RealtimeEnvelope.swift).
// - Fields *inside* `body` are lenient at runtime: a receiver MUST ignore
//   unrecognized body fields. Swift's `Decodable` already does this for any
//   JSON key not named in a type's `CodingKeys`, so the body structs below
//   get that leniency for free and only need to enforce the fields this
//   version of the protocol actually defines (required-ness, bounds, enums).

/// Every error a fixture in `proto/realtime/fixtures/invalid/` is expected to
/// produce when decoded by a conformant receiver.
public enum RealtimeDecodingError: Error, Sendable, Equatable, CustomStringConvertible {
    /// The envelope or body violates a constraint this protocol version
    /// defines (§13 `malformed`): an unknown top-level envelope field, a
    /// missing required field, a value outside its schema bound, or a
    /// same-message cross-field rule (e.g. `command` action/field
    /// exclusivity, `ack` requiring `acked` or `in_reply_to`).
    case malformed(String)
    /// `envelope.type` is not one of the 14 known message types. Per §15
    /// this is NOT malformed — a receiver MUST ignore it (forward
    /// compatibility with a future additive message type).
    case unknownType(String)

    public var description: String {
        switch self {
        case .malformed(let reason): "realtime malformed: \(reason)"
        case .unknownType(let type): "realtime unknown type (ignored per SPEC.md §15): \(type)"
        }
    }
}

/// Schema-bound checks mirroring `common.schema.json` `$defs`. Every check
/// here corresponds to a named constraint in that file so a reviewer can
/// diff the two side by side.
enum RealtimeValidation {
    /// `$defs/unix_ms_timestamp`: 13-digit millisecond bound. This is the
    /// mechanical guard against the historical seconds/milliseconds bug —
    /// see `fixtures/invalid/message-event-timestamp-in-seconds.json`.
    static func unixMs(_ value: Int, field: String) throws -> Int {
        guard value >= 1_000_000_000_000 && value <= 9_999_999_999_999 else {
            throw RealtimeDecodingError.malformed("\(field) is not a Unix ms timestamp (13 digits): \(value)")
        }
        return value
    }

    static func maxLength(_ s: String, _ n: Int, field: String) throws -> String {
        guard s.count <= n else {
            throw RealtimeDecodingError.malformed("\(field) exceeds max length \(n): \(s.count) chars")
        }
        return s
    }

    static func length(_ s: String, _ range: ClosedRange<Int>, field: String) throws -> String {
        guard range.contains(s.count) else {
            throw RealtimeDecodingError.malformed("\(field) length \(s.count) outside \(range)")
        }
        return s
    }

    static func minimum(_ v: Int, _ lower: Int, field: String) throws -> Int {
        guard v >= lower else {
            throw RealtimeDecodingError.malformed("\(field) \(v) below minimum \(lower)")
        }
        return v
    }

    static func range(_ v: Int, _ r: ClosedRange<Int>, field: String) throws -> Int {
        guard r.contains(v) else {
            throw RealtimeDecodingError.malformed("\(field) \(v) outside \(r)")
        }
        return v
    }

    static func nonEmpty<T>(_ arr: [T], field: String) throws -> [T] {
        guard !arr.isEmpty else {
            throw RealtimeDecodingError.malformed("\(field) must not be empty")
        }
        return arr
    }

    static func maxItems<T>(_ arr: [T], _ n: Int, field: String) throws -> [T] {
        guard arr.count <= n else {
            throw RealtimeDecodingError.malformed("\(field) exceeds max items \(n): \(arr.count)")
        }
        return arr
    }

    static func uniqueItems<T: Hashable>(_ arr: [T], field: String) throws -> [T] {
        guard Set(arr).count == arr.count else {
            throw RealtimeDecodingError.malformed("\(field) contains duplicate items")
        }
        return arr
    }

    static func pattern(_ s: String, _ pattern: String, field: String) throws -> String {
        guard s.range(of: pattern, options: .regularExpression) != nil,
              s.range(of: pattern, options: .regularExpression) == s.startIndex..<s.endIndex
        else {
            throw RealtimeDecodingError.malformed("\(field) does not match pattern \(pattern): \(s)")
        }
        return s
    }
}

/// A dynamic `CodingKey` used to enumerate the actual keys present in a JSON
/// object, so we can detect an unknown top-level envelope field — something
/// a fixed `CodingKeys` enum cannot do, since it only recognizes keys it
/// already knows about.
struct AnyKey: CodingKey, Hashable {
    var stringValue: String
    var intValue: Int?
    init?(stringValue: String) { self.stringValue = stringValue; self.intValue = nil }
    init(_ stringValue: String) { self.stringValue = stringValue; self.intValue = nil }
    init?(intValue: Int) { self.stringValue = "\(intValue)"; self.intValue = intValue }
}

/// Minimal arbitrary-JSON value, used only for `command.body.params` —
/// protocol v1's sole explicitly-open-ended field (§8: "additional
/// properties here are explicitly permitted and MUST be ignored by a
/// receiver that does not recognize them").
public enum JSONValue: Codable, Sendable, Equatable {
    case string(String)
    case number(Double)
    case bool(Bool)
    case object([String: JSONValue])
    case array([JSONValue])
    case null

    public init(from decoder: Decoder) throws {
        let c = try decoder.singleValueContainer()
        if c.decodeNil() { self = .null; return }
        if let v = try? c.decode(Bool.self) { self = .bool(v); return }
        if let v = try? c.decode(Double.self) { self = .number(v); return }
        if let v = try? c.decode(String.self) { self = .string(v); return }
        if let v = try? c.decode([String: JSONValue].self) { self = .object(v); return }
        if let v = try? c.decode([JSONValue].self) { self = .array(v); return }
        throw DecodingError.dataCorruptedError(in: c, debugDescription: "unsupported JSON value")
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.singleValueContainer()
        switch self {
        case .string(let v): try c.encode(v)
        case .number(let v): try c.encode(v)
        case .bool(let v): try c.encode(v)
        case .object(let v): try c.encode(v)
        case .array(let v): try c.encode(v)
        case .null: try c.encodeNil()
        }
    }
}

// MARK: - Shared enums (common.schema.json $defs with an `enum`)

/// `$defs/device_role`.
public enum RTDeviceRole: String, Codable, Sendable, Equatable {
    case source, viewer
}

/// `$defs/message_level`.
public enum RTMessageLevel: String, Codable, Sendable, Equatable {
    case info, warn, error
}

/// `$defs/task_kind` — the sync-layer lifecycle phase a `task.event`
/// reports, distinct from the raw stdout verbs in proto/SPEC.md.
public enum RTTaskKind: String, Codable, Sendable, Equatable {
    case started, progress, step, done, failed
}

/// Interest-lease topic filter (`subscribe`/`interest.renew` `topics`).
public enum RTTopic: String, Codable, Sendable, Equatable {
    case task, metric, message
}

/// `command.body.origin`.
public enum RTCommandOrigin: String, Codable, Sendable, Equatable {
    case viewer, server
}

/// `command.body.action`.
public enum RTCommandAction: String, Codable, Sendable, Equatable {
    case pause, resume, stop, runNow = "run_now", throttle, resumeRate = "resume_rate"
}

/// `error.body.code` — the 13 codes in SPEC.md §13.
public enum RTErrorCode: String, Codable, Sendable, Equatable {
    case versionUnsupported = "version_unsupported"
    case helloRequired = "hello_required"
    case unauthenticated
    case unauthorized
    case malformed
    case rateLimited = "rate_limited"
    case frameTooLarge = "frame_too_large"
    case batchTooLarge = "batch_too_large"
    case revisionUnavailable = "revision_unavailable"
    case commandExpired = "command_expired"
    case sequenceGap = "sequence_gap"
    case superseded
    case internalError = "internal_error"
}

// MARK: - Shared object $defs

/// `$defs/display_hints`. Purely cosmetic; a renderer ignores hints it does
/// not understand.
public struct RTDisplayHints: Codable, Sendable, Equatable {
    public var icon: String?
    public var tint: String?
    public var template: String?

    public init(icon: String? = nil, tint: String? = nil, template: String? = nil) {
        self.icon = icon
        self.tint = tint
        self.template = template
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        icon = try c.decodeIfPresent(String.self, forKey: .icon)
        tint = try c.decodeIfPresent(String.self, forKey: .tint)
        template = try c.decodeIfPresent(String.self, forKey: .template)
        if let icon { try _ = RealtimeValidation.maxLength(icon, 64, field: "display.icon") }
        if let tint { try _ = RealtimeValidation.maxLength(tint, 32, field: "display.tint") }
        if let template { try _ = RealtimeValidation.maxLength(template, 32, field: "display.template") }
    }

    private enum CodingKeys: String, CodingKey { case icon, tint, template }
}

/// `$defs/task_state` — full folded state of one task, as carried in a
/// `snapshot` and as the type SpaceState keeps per task after folding
/// `task.event`s (§6.4).
public struct RTTaskState: Codable, Sendable, Equatable, Identifiable {
    public var taskID: String
    public var deviceID: String
    public var title: String?
    public var state: RTTaskRunState
    public var percent: Int?
    public var step: String?
    public var message: String?
    public var updatedAt: Int
    public var display: RTDisplayHints?

    public var id: String { taskID }

    public init(taskID: String, deviceID: String, title: String? = nil, state: RTTaskRunState,
                percent: Int? = nil, step: String? = nil, message: String? = nil,
                updatedAt: Int, display: RTDisplayHints? = nil) {
        self.taskID = taskID
        self.deviceID = deviceID
        self.title = title
        self.state = state
        self.percent = percent
        self.step = step
        self.message = message
        self.updatedAt = updatedAt
        self.display = display
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        taskID = try RealtimeValidation.length(c.decode(String.self, forKey: .taskID), 1...256, field: "task_id")
        deviceID = try RealtimeValidation.length(c.decode(String.self, forKey: .deviceID), 1...128, field: "device_id")
        title = try c.decodeIfPresent(String.self, forKey: .title)
        if let title { try _ = RealtimeValidation.maxLength(title, 2048, field: "title") }
        state = try c.decode(RTTaskRunState.self, forKey: .state)
        percent = try c.decodeIfPresent(Int.self, forKey: .percent)
        if let percent { try _ = RealtimeValidation.range(percent, 0...100, field: "percent") }
        step = try c.decodeIfPresent(String.self, forKey: .step)
        if let step { try _ = RealtimeValidation.maxLength(step, 2048, field: "step") }
        message = try c.decodeIfPresent(String.self, forKey: .message)
        if let message { try _ = RealtimeValidation.maxLength(message, 2048, field: "message") }
        updatedAt = try RealtimeValidation.unixMs(c.decode(Int.self, forKey: .updatedAt), field: "updated_at")
        display = try c.decodeIfPresent(RTDisplayHints.self, forKey: .display)
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(taskID, forKey: .taskID)
        try c.encode(deviceID, forKey: .deviceID)
        try c.encodeIfPresent(title, forKey: .title)
        try c.encode(state, forKey: .state)
        try c.encodeIfPresent(percent, forKey: .percent)
        try c.encodeIfPresent(step, forKey: .step)
        try c.encodeIfPresent(message, forKey: .message)
        try c.encode(updatedAt, forKey: .updatedAt)
        try c.encodeIfPresent(display, forKey: .display)
    }

    private enum CodingKeys: String, CodingKey {
        case taskID = "task_id", deviceID = "device_id", title, state, percent, step, message
        case updatedAt = "updated_at", display
    }
}

/// The folded `state` enum (`running`/`done`/`failed`), distinct from
/// `RTTaskKind` (the raw event kind that produces it, §6.4).
public enum RTTaskRunState: String, Codable, Sendable, Equatable {
    case running, done, failed
}

/// `$defs/metric_sample`.
public struct RTMetricSample: Codable, Sendable, Equatable, Identifiable {
    public var metricID: String
    public var value: String
    public var label: String?
    public var ts: Int
    public var display: RTDisplayHints?
    public var target: String?
    public var min: String?
    public var max: String?
    public var alertAbove: String?
    public var alertBelow: String?

    public var id: String { metricID }

    public init(metricID: String, value: String, label: String? = nil, ts: Int,
                display: RTDisplayHints? = nil, target: String? = nil, min: String? = nil,
                max: String? = nil, alertAbove: String? = nil, alertBelow: String? = nil) {
        self.metricID = metricID
        self.value = value
        self.label = label
        self.ts = ts
        self.display = display
        self.target = target
        self.min = min
        self.max = max
        self.alertAbove = alertAbove
        self.alertBelow = alertBelow
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        metricID = try RealtimeValidation.pattern(
            c.decode(String.self, forKey: .metricID), "^[a-z0-9_.-]{1,64}$", field: "metric_id")
        value = try RealtimeValidation.maxLength(c.decode(String.self, forKey: .value), 256, field: "value")
        label = try c.decodeIfPresent(String.self, forKey: .label)
        if let label { try _ = RealtimeValidation.maxLength(label, 256, field: "label") }
        ts = try RealtimeValidation.unixMs(c.decode(Int.self, forKey: .ts), field: "ts")
        display = try c.decodeIfPresent(RTDisplayHints.self, forKey: .display)
        target = try c.decodeIfPresent(String.self, forKey: .target)
        min = try c.decodeIfPresent(String.self, forKey: .min)
        max = try c.decodeIfPresent(String.self, forKey: .max)
        alertAbove = try c.decodeIfPresent(String.self, forKey: .alertAbove)
        alertBelow = try c.decodeIfPresent(String.self, forKey: .alertBelow)
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(metricID, forKey: .metricID)
        try c.encode(value, forKey: .value)
        try c.encodeIfPresent(label, forKey: .label)
        try c.encode(ts, forKey: .ts)
        try c.encodeIfPresent(display, forKey: .display)
        try c.encodeIfPresent(target, forKey: .target)
        try c.encodeIfPresent(min, forKey: .min)
        try c.encodeIfPresent(max, forKey: .max)
        try c.encodeIfPresent(alertAbove, forKey: .alertAbove)
        try c.encodeIfPresent(alertBelow, forKey: .alertBelow)
    }

    private enum CodingKeys: String, CodingKey {
        case metricID = "metric_id", value, label, ts, display, target, min, max
        case alertAbove = "alert_above", alertBelow = "alert_below"
    }
}

/// `$defs/automation_state`.
public struct RTAutomationState: Codable, Sendable, Equatable, Identifiable {
    public var automationID: String
    public var name: String
    public var executorKind: String
    public var scheduleEverySeconds: Int
    public var state: RTAutomationRunState
    public var lastRunAt: Int?

    public var id: String { automationID }

    public init(automationID: String, name: String, executorKind: String, scheduleEverySeconds: Int,
                state: RTAutomationRunState, lastRunAt: Int? = nil) {
        self.automationID = automationID
        self.name = name
        self.executorKind = executorKind
        self.scheduleEverySeconds = scheduleEverySeconds
        self.state = state
        self.lastRunAt = lastRunAt
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        automationID = try RealtimeValidation.length(c.decode(String.self, forKey: .automationID), 1...128, field: "automation_id")
        name = try RealtimeValidation.length(c.decode(String.self, forKey: .name), 1...256, field: "name")
        executorKind = try c.decode(String.self, forKey: .executorKind)
        guard ["script", "agent", "hybrid"].contains(executorKind) else {
            throw RealtimeDecodingError.malformed("automation.executor_kind invalid: \(executorKind)")
        }
        let scheduleContainer = try c.nestedContainer(keyedBy: ScheduleKeys.self, forKey: .schedule)
        let kind = try scheduleContainer.decode(String.self, forKey: .kind)
        guard kind == "interval" else {
            throw RealtimeDecodingError.malformed("automation.schedule.kind must be 'interval': \(kind)")
        }
        scheduleEverySeconds = try RealtimeValidation.minimum(
            scheduleContainer.decode(Int.self, forKey: .everySeconds), 1, field: "schedule.every_seconds")
        state = try c.decode(RTAutomationRunState.self, forKey: .state)
        lastRunAt = try c.decodeIfPresent(Int.self, forKey: .lastRunAt)
        if let lastRunAt { try _ = RealtimeValidation.unixMs(lastRunAt, field: "last_run_at") }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(automationID, forKey: .automationID)
        try c.encode(name, forKey: .name)
        try c.encode(executorKind, forKey: .executorKind)
        var scheduleContainer = c.nestedContainer(keyedBy: ScheduleKeys.self, forKey: .schedule)
        try scheduleContainer.encode("interval", forKey: .kind)
        try scheduleContainer.encode(scheduleEverySeconds, forKey: .everySeconds)
        try c.encode(state, forKey: .state)
        try c.encodeIfPresent(lastRunAt, forKey: .lastRunAt)
    }

    private enum CodingKeys: String, CodingKey {
        case automationID = "automation_id", name
        case executorKind = "executor_kind", schedule, state
        case lastRunAt = "last_run_at"
    }
    private enum ScheduleKeys: String, CodingKey { case kind, everySeconds = "every_seconds" }
}

public enum RTAutomationRunState: String, Codable, Sendable, Equatable {
    case active, paused
}

/// `$defs/message_record`.
public struct RTMessageRecord: Codable, Sendable, Equatable, Identifiable {
    public var messageID: String
    public var deviceID: String
    public var level: RTMessageLevel
    public var text: String
    public var occurredAt: Int

    public var id: String { messageID }

    public init(messageID: String, deviceID: String, level: RTMessageLevel, text: String, occurredAt: Int) {
        self.messageID = messageID
        self.deviceID = deviceID
        self.level = level
        self.text = text
        self.occurredAt = occurredAt
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        messageID = try RealtimeValidation.length(c.decode(String.self, forKey: .messageID), 1...128, field: "message_id")
        deviceID = try RealtimeValidation.length(c.decode(String.self, forKey: .deviceID), 1...128, field: "device_id")
        level = try c.decode(RTMessageLevel.self, forKey: .level)
        text = try RealtimeValidation.maxLength(c.decode(String.self, forKey: .text), 2048, field: "text")
        occurredAt = try RealtimeValidation.unixMs(c.decode(Int.self, forKey: .occurredAt), field: "occurred_at")
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(messageID, forKey: .messageID)
        try c.encode(deviceID, forKey: .deviceID)
        try c.encode(level, forKey: .level)
        try c.encode(text, forKey: .text)
        try c.encode(occurredAt, forKey: .occurredAt)
    }

    private enum CodingKeys: String, CodingKey {
        case messageID = "message_id", deviceID = "device_id", level, text
        case occurredAt = "occurred_at"
    }
}
