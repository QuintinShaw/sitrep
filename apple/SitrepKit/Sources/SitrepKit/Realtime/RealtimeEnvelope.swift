import Foundation

/// A type that can sit inside `Envelope.body`. `messageType` is the constant
/// the schema pins to `envelope.type` for this body shape (envelope.schema.json
/// §3 / the `type` enum in SPEC.md §4).
public protocol RealtimeBody: Codable, Sendable, Equatable {
    static var messageType: String { get }
}

/// The generic envelope shared by every frame (SPEC.md §3): exactly
/// `type`/`id`/`ts`/`body`, strictly — an unknown top-level field is always
/// `malformed` and this can never be relaxed by a future minor version. That
/// strictness is what keeps a credential from ever being smuggled in at the
/// envelope level (see `fixtures/invalid/envelope-carries-credential-field.json`).
public struct Envelope<Body: RealtimeBody>: Sendable, Equatable {
    public var id: String
    public var ts: Int
    public var body: Body

    public init(id: String, ts: Int, body: Body) {
        self.id = id
        self.ts = ts
        self.body = body
    }
}

extension Envelope: Codable {
    private enum CodingKeys: String, CodingKey { case type, id, ts, body }

    public init(from decoder: Decoder) throws {
        // Strict top-level check: exactly {type, id, ts, body}, no more, no
        // less. A fixed CodingKeys-keyed container can't see keys it doesn't
        // name, so this uses a dynamic-key container to enumerate what's
        // actually present in the JSON object.
        let dyn = try decoder.container(keyedBy: AnyKey.self)
        let present = Set(dyn.allKeys.map(\.stringValue))
        let allowed: Set<String> = ["type", "id", "ts", "body"]
        guard present == allowed else {
            let extra = present.subtracting(allowed)
            let missing = allowed.subtracting(present)
            throw RealtimeDecodingError.malformed(
                "envelope must carry exactly type/id/ts/body"
                    + (extra.isEmpty ? "" : "; unexpected field(s): \(extra.sorted().joined(separator: ", "))")
                    + (missing.isEmpty ? "" : "; missing field(s): \(missing.sorted().joined(separator: ", "))")
            )
        }

        let c = try decoder.container(keyedBy: CodingKeys.self)
        let type = try c.decode(String.self, forKey: .type)
        guard type == Body.messageType else {
            throw RealtimeDecodingError.malformed("expected envelope.type '\(Body.messageType)', got '\(type)'")
        }
        id = try RealtimeValidation.pattern(
            c.decode(String.self, forKey: .id), "^[A-Za-z0-9_-]{1,64}$", field: "id")
        ts = try RealtimeValidation.unixMs(c.decode(Int.self, forKey: .ts), field: "ts")
        body = try c.decode(Body.self, forKey: .body)
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(Body.messageType, forKey: .type)
        try c.encode(id, forKey: .id)
        try c.encode(ts, forKey: .ts)
        try c.encode(body, forKey: .body)
    }
}

/// One of the 14 message types (SPEC.md §4), fully decoded. `RealtimeFrame`
/// is the unit `RealtimeClient` sends/receives; a value round-trips through
/// `decode(_:)`/`encoded()` byte-for-byte in semantics (field order may
/// differ, JSON does not guarantee it).
public enum RealtimeFrame: Sendable, Equatable {
    case hello(Envelope<HelloBody>)
    case resume(Envelope<ResumeBody>)
    case snapshot(Envelope<SnapshotBody>)
    case delta(Envelope<DeltaBody>)
    case ack(Envelope<AckBody>)
    case taskEvent(Envelope<TaskEventBody>)
    case messageEvent(Envelope<MessageEventBody>)
    case metricFrame(Envelope<MetricFrameBody>)
    case configEvent(Envelope<ConfigEventBody>)
    case subscribe(Envelope<SubscribeBody>)
    case unsubscribe(Envelope<UnsubscribeBody>)
    case interestRenew(Envelope<InterestRenewBody>)
    case command(Envelope<CommandBody>)
    case error(Envelope<ErrorBody>)

    private struct TypePeek: Decodable { let type: String }

    /// Decodes one JSON frame (one WebSocket text frame carries exactly one
    /// envelope, SPEC.md §11). Throws `RealtimeDecodingError.unknownType`
    /// for a `type` outside the 14 known ones (§15: a receiver MUST ignore
    /// this, not treat it as malformed) and `.malformed` for every other
    /// schema/sequence violation.
    public static func decode(_ data: Data) throws -> RealtimeFrame {
        let decoder = JSONDecoder()
        let peek: TypePeek
        do {
            peek = try decoder.decode(TypePeek.self, from: data)
        } catch {
            throw RealtimeDecodingError.malformed("envelope missing/invalid 'type': \(error)")
        }
        switch peek.type {
        case HelloBody.messageType: return .hello(try decoder.decode(Envelope<HelloBody>.self, from: data))
        case ResumeBody.messageType: return .resume(try decoder.decode(Envelope<ResumeBody>.self, from: data))
        case SnapshotBody.messageType: return .snapshot(try decoder.decode(Envelope<SnapshotBody>.self, from: data))
        case DeltaBody.messageType: return .delta(try decoder.decode(Envelope<DeltaBody>.self, from: data))
        case AckBody.messageType: return .ack(try decoder.decode(Envelope<AckBody>.self, from: data))
        case TaskEventBody.messageType: return .taskEvent(try decoder.decode(Envelope<TaskEventBody>.self, from: data))
        case MessageEventBody.messageType: return .messageEvent(try decoder.decode(Envelope<MessageEventBody>.self, from: data))
        case MetricFrameBody.messageType: return .metricFrame(try decoder.decode(Envelope<MetricFrameBody>.self, from: data))
        case ConfigEventBody.messageType: return .configEvent(try decoder.decode(Envelope<ConfigEventBody>.self, from: data))
        case SubscribeBody.messageType: return .subscribe(try decoder.decode(Envelope<SubscribeBody>.self, from: data))
        case UnsubscribeBody.messageType: return .unsubscribe(try decoder.decode(Envelope<UnsubscribeBody>.self, from: data))
        case InterestRenewBody.messageType: return .interestRenew(try decoder.decode(Envelope<InterestRenewBody>.self, from: data))
        case CommandBody.messageType: return .command(try decoder.decode(Envelope<CommandBody>.self, from: data))
        case ErrorBody.messageType: return .error(try decoder.decode(Envelope<ErrorBody>.self, from: data))
        default:
            throw RealtimeDecodingError.unknownType(peek.type)
        }
    }

    public func encoded() throws -> Data {
        let encoder = JSONEncoder()
        switch self {
        case .hello(let e): return try encoder.encode(e)
        case .resume(let e): return try encoder.encode(e)
        case .snapshot(let e): return try encoder.encode(e)
        case .delta(let e): return try encoder.encode(e)
        case .ack(let e): return try encoder.encode(e)
        case .taskEvent(let e): return try encoder.encode(e)
        case .messageEvent(let e): return try encoder.encode(e)
        case .metricFrame(let e): return try encoder.encode(e)
        case .configEvent(let e): return try encoder.encode(e)
        case .subscribe(let e): return try encoder.encode(e)
        case .unsubscribe(let e): return try encoder.encode(e)
        case .interestRenew(let e): return try encoder.encode(e)
        case .command(let e): return try encoder.encode(e)
        case .error(let e): return try encoder.encode(e)
        }
    }

    /// The envelope `id` shared by every case, for logging/`in_reply_to`
    /// correlation.
    public var envelopeID: String {
        switch self {
        case .hello(let e): e.id
        case .resume(let e): e.id
        case .snapshot(let e): e.id
        case .delta(let e): e.id
        case .ack(let e): e.id
        case .taskEvent(let e): e.id
        case .messageEvent(let e): e.id
        case .metricFrame(let e): e.id
        case .configEvent(let e): e.id
        case .subscribe(let e): e.id
        case .unsubscribe(let e): e.id
        case .interestRenew(let e): e.id
        case .command(let e): e.id
        case .error(let e): e.id
        }
    }
}
