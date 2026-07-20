import Foundation

// One type per messages/*.schema.json body. Ordered to match SPEC.md §4's
// table. Each body's Decodable init enforces exactly the constraints its
// schema file expresses (required fields via non-optional properties,
// everything else via explicit checks); unrecognized extra keys are ignored
// for free by Codable, matching the body-level leniency in SPEC.md §15.

// MARK: - hello

/// `hello{body.stage: "offer"}` — sent by the connecting device, in both
/// directions of the realtime protocol (only source/viewer send offers).
public struct HelloOffer: Codable, Sendable, Equatable {
    public var deviceID: String
    public var role: RTDeviceRole
    public var protocolVersions: [Int]
    public var capabilities: [String]

    public init(deviceID: String, role: RTDeviceRole, protocolVersions: [Int], capabilities: [String] = []) {
        self.deviceID = deviceID
        self.role = role
        self.protocolVersions = protocolVersions
        self.capabilities = capabilities
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        deviceID = try RealtimeValidation.pattern(
            c.decode(String.self, forKey: .deviceID), "^[A-Za-z0-9_-]{1,128}$", field: "device_id")
        role = try c.decode(RTDeviceRole.self, forKey: .role)
        protocolVersions = try RealtimeValidation.uniqueItems(
            RealtimeValidation.nonEmpty(c.decode([Int].self, forKey: .protocolVersions), field: "protocol_versions"),
            field: "protocol_versions")
        capabilities = try c.decodeIfPresent([String].self, forKey: .capabilities) ?? []
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(deviceID, forKey: .deviceID)
        try c.encode(role, forKey: .role)
        try c.encode(protocolVersions, forKey: .protocolVersions)
        try c.encode(capabilities, forKey: .capabilities)
    }

    private enum CodingKeys: String, CodingKey {
        case deviceID = "device_id", role
        case protocolVersions = "protocol_versions", capabilities
    }
}

/// `hello{body.stage: "accept"}` — server-only.
public struct HelloAccept: Codable, Sendable, Equatable {
    public var protocolVersion: Int
    public var sessionID: String
    public var heartbeatIntervalMs: Int
    public var capabilities: [String]

    public init(protocolVersion: Int, sessionID: String, heartbeatIntervalMs: Int, capabilities: [String] = []) {
        self.protocolVersion = protocolVersion
        self.sessionID = sessionID
        self.heartbeatIntervalMs = heartbeatIntervalMs
        self.capabilities = capabilities
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        protocolVersion = try RealtimeValidation.minimum(
            c.decode(Int.self, forKey: .protocolVersion), 1, field: "protocol_version")
        sessionID = try RealtimeValidation.length(c.decode(String.self, forKey: .sessionID), 1...64, field: "session_id")
        heartbeatIntervalMs = try RealtimeValidation.range(
            c.decode(Int.self, forKey: .heartbeatIntervalMs), 1000...300_000, field: "heartbeat_interval_ms")
        capabilities = try c.decodeIfPresent([String].self, forKey: .capabilities) ?? []
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(protocolVersion, forKey: .protocolVersion)
        try c.encode(sessionID, forKey: .sessionID)
        try c.encode(heartbeatIntervalMs, forKey: .heartbeatIntervalMs)
        try c.encode(capabilities, forKey: .capabilities)
    }

    private enum CodingKeys: String, CodingKey {
        case protocolVersion = "protocol_version", sessionID = "session_id"
        case heartbeatIntervalMs = "heartbeat_interval_ms", capabilities
    }
}

/// `hello.body`: `oneOf(offer, accept)` discriminated by `stage`.
public enum HelloBody: RealtimeBody {
    case offer(HelloOffer)
    case accept(HelloAccept)

    public static let messageType = "hello"

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        let stage = try c.decode(String.self, forKey: .stage)
        switch stage {
        case "offer": self = .offer(try HelloOffer(from: decoder))
        case "accept": self = .accept(try HelloAccept(from: decoder))
        default: throw RealtimeDecodingError.malformed("hello.stage must be 'offer' or 'accept': \(stage)")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .offer(let o):
            try c.encode("offer", forKey: .stage)
            try o.encode(to: encoder)
        case .accept(let a):
            try c.encode("accept", forKey: .stage)
            try a.encode(to: encoder)
        }
    }

    private enum CodingKeys: String, CodingKey { case stage }
}

// MARK: - resume

public struct ResumeBody: RealtimeBody {
    public static let messageType = "resume"

    /// The space_revision this viewer last fully applied. 0 means "no prior
    /// state, send me a snapshot" (§6.3).
    public var lastRevision: Int

    public init(lastRevision: Int) { self.lastRevision = lastRevision }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        lastRevision = try RealtimeValidation.minimum(
            c.decode(Int.self, forKey: .lastRevision), 0, field: "last_revision")
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(lastRevision, forKey: .lastRevision)
    }

    private enum CodingKeys: String, CodingKey { case lastRevision = "last_revision" }
}

// MARK: - snapshot

public struct SnapshotBody: RealtimeBody {
    public static let messageType = "snapshot"

    public var revision: Int
    public var part: Int
    public var final: Bool
    public var tasks: [RTTaskState]
    public var metrics: [RTMetricSample]
    public var messages: [RTMessageRecord]
    public var automations: [RTAutomationState]

    public init(revision: Int, part: Int, final: Bool, tasks: [RTTaskState], metrics: [RTMetricSample],
                messages: [RTMessageRecord], automations: [RTAutomationState]) {
        self.revision = revision
        self.part = part
        self.final = final
        self.tasks = tasks
        self.metrics = metrics
        self.messages = messages
        self.automations = automations
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        revision = try RealtimeValidation.minimum(c.decode(Int.self, forKey: .revision), 0, field: "revision")
        part = try RealtimeValidation.minimum(c.decode(Int.self, forKey: .part), 1, field: "part")
        final = try c.decode(Bool.self, forKey: .final)
        tasks = try c.decode([RTTaskState].self, forKey: .tasks)
        metrics = try c.decode([RTMetricSample].self, forKey: .metrics)
        messages = try c.decode([RTMessageRecord].self, forKey: .messages)
        automations = try c.decode([RTAutomationState].self, forKey: .automations)
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(revision, forKey: .revision)
        try c.encode(part, forKey: .part)
        try c.encode(final, forKey: .final)
        try c.encode(tasks, forKey: .tasks)
        try c.encode(metrics, forKey: .metrics)
        try c.encode(messages, forKey: .messages)
        try c.encode(automations, forKey: .automations)
    }

    private enum CodingKeys: String, CodingKey { case revision, part, final, tasks, metrics, messages, automations }
}

// MARK: - delta

/// One entry of `delta.body.events`: `oneOf` tagged by `event_type`. An
/// `event_type` this build does not recognize is kept as `.unknown` rather
/// than failing the whole delta — a future minor version could add a new
/// reliable event kind, and dropping just that one entry (while the
/// revision still advances by the full `events.length`, per §6.2's exact
/// arithmetic) is safer than losing revision continuity over it. This is a
/// defensive interpretation beyond what SPEC.md §15 states explicitly (which
/// only names *envelope* `type`, not nested `event_type`) — flagged in the
/// handoff as a place a protocol owner may want to rule on explicitly.
public enum DeltaEvent: Sendable, Equatable {
    case taskEvent(TaskEventBody)
    case messageEvent(MessageEventBody)
    case configEvent(ConfigEventBody)
    case unknown(eventType: String)
}

extension DeltaEvent: Codable {
    private enum CodingKeys: String, CodingKey { case eventType = "event_type", event }

    public init(from decoder: Decoder) throws {
        // Only `event_type` and `event` are defined in v1; unknown sibling
        // fields are ignored, matching SPEC.md §15's body-level tolerance
        // (the top-level envelope strictness of §3 does not extend to
        // objects nested inside `body`).
        let c = try decoder.container(keyedBy: CodingKeys.self)
        let type = try c.decode(String.self, forKey: .eventType)
        switch type {
        case "task.event": self = .taskEvent(try c.decode(TaskEventBody.self, forKey: .event))
        case "message.event": self = .messageEvent(try c.decode(MessageEventBody.self, forKey: .event))
        case "config.event": self = .configEvent(try c.decode(ConfigEventBody.self, forKey: .event))
        default: self = .unknown(eventType: type)
        }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .taskEvent(let b):
            try c.encode("task.event", forKey: .eventType)
            try c.encode(b, forKey: .event)
        case .messageEvent(let b):
            try c.encode("message.event", forKey: .eventType)
            try c.encode(b, forKey: .event)
        case .configEvent(let b):
            try c.encode("config.event", forKey: .eventType)
            try c.encode(b, forKey: .event)
        case .unknown(let type):
            // Never constructed from an outbound encode in this client (we
            // never originate delta), but keep encode total.
            try c.encode(type, forKey: .eventType)
            try c.encode([String: String](), forKey: .event)
        }
    }
}

public struct DeltaBody: RealtimeBody {
    public static let messageType = "delta"

    public var fromRevision: Int
    public var toRevision: Int
    public var events: [DeltaEvent]

    public init(fromRevision: Int, toRevision: Int, events: [DeltaEvent]) {
        self.fromRevision = fromRevision
        self.toRevision = toRevision
        self.events = events
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        fromRevision = try RealtimeValidation.minimum(c.decode(Int.self, forKey: .fromRevision), 0, field: "from_revision")
        toRevision = try RealtimeValidation.minimum(c.decode(Int.self, forKey: .toRevision), 0, field: "to_revision")
        events = try c.decode([DeltaEvent].self, forKey: .events)
        // §6.2: to_revision - from_revision MUST equal events.length exactly.
        guard toRevision - fromRevision == events.count else {
            throw RealtimeDecodingError.malformed(
                "delta arithmetic violated: to_revision(\(toRevision)) - from_revision(\(fromRevision)) != events.length(\(events.count))")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(fromRevision, forKey: .fromRevision)
        try c.encode(toRevision, forKey: .toRevision)
        try c.encode(events, forKey: .events)
    }

    private enum CodingKeys: String, CodingKey {
        case fromRevision = "from_revision", toRevision = "to_revision", events
    }
}

// MARK: - ack

public struct DeviceSeqPair: Codable, Sendable, Equatable {
    public var deviceID: String
    public var deviceSeq: Int

    public init(deviceID: String, deviceSeq: Int) {
        self.deviceID = deviceID
        self.deviceSeq = deviceSeq
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        deviceID = try RealtimeValidation.pattern(
            c.decode(String.self, forKey: .deviceID), "^[A-Za-z0-9_-]{1,128}$", field: "device_id")
        deviceSeq = try RealtimeValidation.minimum(c.decode(Int.self, forKey: .deviceSeq), 1, field: "device_seq")
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(deviceID, forKey: .deviceID)
        try c.encode(deviceSeq, forKey: .deviceSeq)
    }

    private enum CodingKeys: String, CodingKey { case deviceID = "device_id", deviceSeq = "device_seq" }
}

public struct LeaseInfo: Codable, Sendable, Equatable {
    /// Absolute deadline; the viewer must `interest.renew` before this to
    /// keep the lease alive (SPEC.md §7).
    public var expiresAt: Int

    public init(expiresAt: Int) { self.expiresAt = expiresAt }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        expiresAt = try RealtimeValidation.unixMs(c.decode(Int.self, forKey: .expiresAt), field: "lease.expires_at")
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(expiresAt, forKey: .expiresAt)
    }

    private enum CodingKeys: String, CodingKey { case expiresAt = "expires_at" }
}

public struct AckBody: RealtimeBody {
    public static let messageType = "ack"

    public var acked: [DeviceSeqPair]?
    public var inReplyTo: String?
    public var lease: LeaseInfo?

    public init(acked: [DeviceSeqPair]? = nil, inReplyTo: String? = nil, lease: LeaseInfo? = nil) {
        self.acked = acked
        self.inReplyTo = inReplyTo
        self.lease = lease
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        acked = try c.decodeIfPresent([DeviceSeqPair].self, forKey: .acked)
        if let acked { try _ = RealtimeValidation.nonEmpty(acked, field: "acked") }
        inReplyTo = try c.decodeIfPresent(String.self, forKey: .inReplyTo)
        lease = try c.decodeIfPresent(LeaseInfo.self, forKey: .lease)
        // anyOf(acked, in_reply_to) — see fixtures/invalid/ack-neither-acked-nor-in-reply-to.json.
        guard acked != nil || inReplyTo != nil else {
            throw RealtimeDecodingError.malformed("ack.body must carry 'acked' and/or 'in_reply_to'")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encodeIfPresent(acked, forKey: .acked)
        try c.encodeIfPresent(inReplyTo, forKey: .inReplyTo)
        try c.encodeIfPresent(lease, forKey: .lease)
    }

    private enum CodingKeys: String, CodingKey { case acked, inReplyTo = "in_reply_to", lease }
}

// MARK: - task.event

public struct TaskEventBody: RealtimeBody {
    public static let messageType = "task.event"

    public var deviceID: String
    public var deviceSeq: Int
    public var taskID: String
    public var kind: RTTaskKind
    public var occurredAt: Int
    public var title: String?
    public var percent: Int?
    public var step: String?
    public var message: String?
    public var display: RTDisplayHints?

    public init(deviceID: String, deviceSeq: Int, taskID: String, kind: RTTaskKind, occurredAt: Int,
                title: String? = nil, percent: Int? = nil, step: String? = nil, message: String? = nil,
                display: RTDisplayHints? = nil) {
        self.deviceID = deviceID
        self.deviceSeq = deviceSeq
        self.taskID = taskID
        self.kind = kind
        self.occurredAt = occurredAt
        self.title = title
        self.percent = percent
        self.step = step
        self.message = message
        self.display = display
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        deviceID = try RealtimeValidation.pattern(
            c.decode(String.self, forKey: .deviceID), "^[A-Za-z0-9_-]{1,128}$", field: "device_id")
        deviceSeq = try RealtimeValidation.minimum(c.decode(Int.self, forKey: .deviceSeq), 1, field: "device_seq")
        taskID = try RealtimeValidation.length(c.decode(String.self, forKey: .taskID), 1...256, field: "task_id")
        kind = try c.decode(RTTaskKind.self, forKey: .kind)
        occurredAt = try RealtimeValidation.unixMs(c.decode(Int.self, forKey: .occurredAt), field: "occurred_at")
        title = try c.decodeIfPresent(String.self, forKey: .title)
        if let title { try _ = RealtimeValidation.maxLength(title, 2048, field: "title") }
        percent = try c.decodeIfPresent(Int.self, forKey: .percent)
        if let percent { try _ = RealtimeValidation.range(percent, 0...100, field: "percent") }
        step = try c.decodeIfPresent(String.self, forKey: .step)
        if let step { try _ = RealtimeValidation.maxLength(step, 2048, field: "step") }
        message = try c.decodeIfPresent(String.self, forKey: .message)
        if let message { try _ = RealtimeValidation.maxLength(message, 2048, field: "message") }
        display = try c.decodeIfPresent(RTDisplayHints.self, forKey: .display)
        // if kind == progress then percent required.
        if kind == .progress && percent == nil {
            throw RealtimeDecodingError.malformed("task.event.body.percent required when kind == 'progress'")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(deviceID, forKey: .deviceID)
        try c.encode(deviceSeq, forKey: .deviceSeq)
        try c.encode(taskID, forKey: .taskID)
        try c.encode(kind, forKey: .kind)
        try c.encode(occurredAt, forKey: .occurredAt)
        try c.encodeIfPresent(title, forKey: .title)
        try c.encodeIfPresent(percent, forKey: .percent)
        try c.encodeIfPresent(step, forKey: .step)
        try c.encodeIfPresent(message, forKey: .message)
        try c.encodeIfPresent(display, forKey: .display)
    }

    private enum CodingKeys: String, CodingKey {
        case deviceID = "device_id", deviceSeq = "device_seq", taskID = "task_id", kind
        case occurredAt = "occurred_at", title, percent, step, message, display
    }
}

// MARK: - message.event

public struct MessageEventBody: RealtimeBody {
    public static let messageType = "message.event"

    public var deviceID: String
    public var deviceSeq: Int
    public var messageID: String
    public var level: RTMessageLevel
    public var text: String
    public var occurredAt: Int
    public var automationID: String?

    public init(deviceID: String, deviceSeq: Int, messageID: String, level: RTMessageLevel, text: String,
                occurredAt: Int, automationID: String? = nil) {
        self.deviceID = deviceID
        self.deviceSeq = deviceSeq
        self.messageID = messageID
        self.level = level
        self.text = text
        self.occurredAt = occurredAt
        self.automationID = automationID
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        deviceID = try RealtimeValidation.pattern(
            c.decode(String.self, forKey: .deviceID), "^[A-Za-z0-9_-]{1,128}$", field: "device_id")
        deviceSeq = try RealtimeValidation.minimum(c.decode(Int.self, forKey: .deviceSeq), 1, field: "device_seq")
        messageID = try RealtimeValidation.length(c.decode(String.self, forKey: .messageID), 1...128, field: "message_id")
        level = try c.decode(RTMessageLevel.self, forKey: .level)
        text = try RealtimeValidation.maxLength(c.decode(String.self, forKey: .text), 2048, field: "text")
        occurredAt = try RealtimeValidation.unixMs(c.decode(Int.self, forKey: .occurredAt), field: "occurred_at")
        automationID = try c.decodeIfPresent(String.self, forKey: .automationID)
        if let automationID { try _ = RealtimeValidation.maxLength(automationID, 128, field: "automation_id") }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(deviceID, forKey: .deviceID)
        try c.encode(deviceSeq, forKey: .deviceSeq)
        try c.encode(messageID, forKey: .messageID)
        try c.encode(level, forKey: .level)
        try c.encode(text, forKey: .text)
        try c.encode(occurredAt, forKey: .occurredAt)
        try c.encodeIfPresent(automationID, forKey: .automationID)
    }

    private enum CodingKeys: String, CodingKey {
        case deviceID = "device_id", deviceSeq = "device_seq", messageID = "message_id"
        case level, text, occurredAt = "occurred_at", automationID = "automation_id"
    }
}

// MARK: - metric.frame

public struct MetricFrameBody: RealtimeBody {
    public static let messageType = "metric.frame"

    public var deviceID: String
    public var metrics: [RTMetricSample]

    public init(deviceID: String, metrics: [RTMetricSample]) {
        self.deviceID = deviceID
        self.metrics = metrics
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        deviceID = try RealtimeValidation.pattern(
            c.decode(String.self, forKey: .deviceID), "^[A-Za-z0-9_-]{1,128}$", field: "device_id")
        let decoded = try c.decode([RTMetricSample].self, forKey: .metrics)
        metrics = try RealtimeValidation.maxItems(
            RealtimeValidation.nonEmpty(decoded, field: "metrics"), 64, field: "metrics")
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(deviceID, forKey: .deviceID)
        try c.encode(metrics, forKey: .metrics)
    }

    private enum CodingKeys: String, CodingKey { case deviceID = "device_id", metrics }
}

// MARK: - config.event

public struct ConfigEventBody: RealtimeBody {
    public static let messageType = "config.event"

    public enum Kind: String, Codable, Sendable, Equatable {
        case upserted = "automation.upserted"
        case removed = "automation.removed"
    }

    public var kind: Kind
    public var automationID: String
    public var automation: RTAutomationState?
    public var occurredAt: Int

    public init(kind: Kind, automationID: String, automation: RTAutomationState? = nil, occurredAt: Int) {
        self.kind = kind
        self.automationID = automationID
        self.automation = automation
        self.occurredAt = occurredAt
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        kind = try c.decode(Kind.self, forKey: .kind)
        automationID = try RealtimeValidation.length(c.decode(String.self, forKey: .automationID), 1...128, field: "automation_id")
        automation = try c.decodeIfPresent(RTAutomationState.self, forKey: .automation)
        occurredAt = try RealtimeValidation.unixMs(c.decode(Int.self, forKey: .occurredAt), field: "occurred_at")
        if kind == .upserted && automation == nil {
            throw RealtimeDecodingError.malformed("config.event.body.automation required when kind == 'automation.upserted'")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(kind, forKey: .kind)
        try c.encode(automationID, forKey: .automationID)
        try c.encodeIfPresent(automation, forKey: .automation)
        try c.encode(occurredAt, forKey: .occurredAt)
    }

    private enum CodingKeys: String, CodingKey {
        case kind, automationID = "automation_id", automation, occurredAt = "occurred_at"
    }
}

// MARK: - subscribe / unsubscribe / interest.renew

/// Shared body shape for `subscribe`, sent as a viewer declares (or wholly
/// replaces) its device's interest lease.
public struct SubscribeBody: RealtimeBody {
    public static let messageType = "subscribe"

    public var topics: [RTTopic]

    public init(topics: [RTTopic] = []) { self.topics = topics }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        let decoded = try c.decodeIfPresent([RTTopic].self, forKey: .topics) ?? []
        topics = try RealtimeValidation.uniqueItems(decoded, field: "topics")
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(topics, forKey: .topics)
    }

    private enum CodingKeys: String, CodingKey { case topics }
}

public struct UnsubscribeBody: RealtimeBody {
    public static let messageType = "unsubscribe"

    public init() {}
    public init(from decoder: Decoder) throws { _ = try decoder.container(keyedBy: AnyKey.self) }
    public func encode(to encoder: Encoder) throws { _ = encoder.container(keyedBy: AnyKey.self) }
}

/// Same shape as `SubscribeBody` (interest.renew.schema.json literally
/// `$ref`s subscribe's body) but a distinct Swift type since `messageType`
/// differs and each schema file is versioned independently.
public struct InterestRenewBody: RealtimeBody {
    public static let messageType = "interest.renew"

    public var topics: [RTTopic]

    public init(topics: [RTTopic] = []) { self.topics = topics }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        let decoded = try c.decodeIfPresent([RTTopic].self, forKey: .topics) ?? []
        topics = try RealtimeValidation.uniqueItems(decoded, field: "topics")
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(topics, forKey: .topics)
    }

    private enum CodingKeys: String, CodingKey { case topics }
}

// MARK: - command

public struct CommandBody: RealtimeBody {
    public static let messageType = "command"

    public var commandID: String
    public var origin: RTCommandOrigin
    public var issuedByDeviceID: String?
    public var action: RTCommandAction
    public var taskID: String?
    public var automationID: String?
    public var targetDeviceID: String?
    public var ttlMs: Int
    public var params: [String: JSONValue]?

    public init(commandID: String, origin: RTCommandOrigin, issuedByDeviceID: String? = nil,
                action: RTCommandAction, taskID: String? = nil, automationID: String? = nil,
                targetDeviceID: String? = nil, ttlMs: Int, params: [String: JSONValue]? = nil) {
        self.commandID = commandID
        self.origin = origin
        self.issuedByDeviceID = issuedByDeviceID
        self.action = action
        self.taskID = taskID
        self.automationID = automationID
        self.targetDeviceID = targetDeviceID
        self.ttlMs = ttlMs
        self.params = params
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        commandID = try RealtimeValidation.length(c.decode(String.self, forKey: .commandID), 1...64, field: "command_id")
        origin = try c.decode(RTCommandOrigin.self, forKey: .origin)
        issuedByDeviceID = try c.decodeIfPresent(String.self, forKey: .issuedByDeviceID)
        action = try c.decode(RTCommandAction.self, forKey: .action)
        taskID = try c.decodeIfPresent(String.self, forKey: .taskID)
        if let taskID { try _ = RealtimeValidation.length(taskID, 1...256, field: "task_id") }
        automationID = try c.decodeIfPresent(String.self, forKey: .automationID)
        if let automationID { try _ = RealtimeValidation.length(automationID, 1...128, field: "automation_id") }
        targetDeviceID = try c.decodeIfPresent(String.self, forKey: .targetDeviceID)
        ttlMs = try RealtimeValidation.range(c.decode(Int.self, forKey: .ttlMs), 1...86_400_000, field: "ttl_ms")
        params = try c.decodeIfPresent([String: JSONValue].self, forKey: .params)

        // §8 per-action required/forbidden field matrix.
        switch action {
        case .pause, .resume, .stop:
            guard origin == .viewer else {
                throw RealtimeDecodingError.malformed("command.action \(action) requires origin 'viewer'")
            }
            guard issuedByDeviceID != nil else {
                throw RealtimeDecodingError.malformed("command.action \(action) requires issued_by_device_id")
            }
            guard taskID != nil else {
                throw RealtimeDecodingError.malformed("command.action \(action) requires task_id")
            }
            guard automationID == nil else {
                throw RealtimeDecodingError.malformed("command.action \(action) forbids automation_id")
            }
        case .runNow:
            guard origin == .viewer else {
                throw RealtimeDecodingError.malformed("command.action run_now requires origin 'viewer'")
            }
            guard issuedByDeviceID != nil else {
                throw RealtimeDecodingError.malformed("command.action run_now requires issued_by_device_id")
            }
            guard automationID != nil else {
                throw RealtimeDecodingError.malformed("command.action run_now requires automation_id")
            }
            guard taskID == nil else {
                throw RealtimeDecodingError.malformed("command.action run_now forbids task_id")
            }
        case .throttle, .resumeRate:
            guard origin == .server else {
                throw RealtimeDecodingError.malformed("command.action \(action) requires origin 'server'")
            }
            guard taskID == nil && automationID == nil else {
                throw RealtimeDecodingError.malformed("command.action \(action) forbids task_id and automation_id")
            }
        }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(commandID, forKey: .commandID)
        try c.encode(origin, forKey: .origin)
        try c.encodeIfPresent(issuedByDeviceID, forKey: .issuedByDeviceID)
        try c.encode(action, forKey: .action)
        try c.encodeIfPresent(taskID, forKey: .taskID)
        try c.encodeIfPresent(automationID, forKey: .automationID)
        try c.encodeIfPresent(targetDeviceID, forKey: .targetDeviceID)
        try c.encode(ttlMs, forKey: .ttlMs)
        try c.encodeIfPresent(params, forKey: .params)
    }

    private enum CodingKeys: String, CodingKey {
        case commandID = "command_id", origin, issuedByDeviceID = "issued_by_device_id", action
        case taskID = "task_id", automationID = "automation_id", targetDeviceID = "target_device_id"
        case ttlMs = "ttl_ms", params
    }
}

// MARK: - error

public struct ErrorBody: RealtimeBody {
    public static let messageType = "error"

    public var code: RTErrorCode
    public var message: String
    public var inReplyTo: String?
    public var retryable: Bool
    public var fatal: Bool

    public init(code: RTErrorCode, message: String, inReplyTo: String? = nil, retryable: Bool, fatal: Bool) {
        self.code = code
        self.message = message
        self.inReplyTo = inReplyTo
        self.retryable = retryable
        self.fatal = fatal
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        code = try c.decode(RTErrorCode.self, forKey: .code)
        message = try RealtimeValidation.maxLength(c.decode(String.self, forKey: .message), 500, field: "message")
        inReplyTo = try c.decodeIfPresent(String.self, forKey: .inReplyTo)
        retryable = try c.decode(Bool.self, forKey: .retryable)
        fatal = try c.decode(Bool.self, forKey: .fatal)
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(code, forKey: .code)
        try c.encode(message, forKey: .message)
        try c.encodeIfPresent(inReplyTo, forKey: .inReplyTo)
        try c.encode(retryable, forKey: .retryable)
        try c.encode(fatal, forKey: .fatal)
    }

    private enum CodingKeys: String, CodingKey {
        case code, message, inReplyTo = "in_reply_to", retryable, fatal
    }
}
