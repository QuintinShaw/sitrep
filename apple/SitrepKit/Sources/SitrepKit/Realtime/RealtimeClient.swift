import Foundation

/// Errors `RealtimeClient` raises itself (as opposed to a `RTErrorCode` the
/// server sent, which arrives wrapped in `.serverError`).
public enum RealtimeClientError: Error, Sendable {
    case timeout
    case protocolViolation(String)
    case serverError(ErrorBody)
}

/// A viewer-role client for the Sitrep realtime protocol
/// (`proto/realtime/SPEC.md`). Owns one WebSocket connection at a time and
/// drives the mandatory `hello → subscribe → resume` gate, snapshot chunk
/// reassembly, delta application, interest-lease renewal, and reconnect
/// with backoff. SitrepApp is a viewer only (it observes a Mac Agent's
/// tasks; it never executes or reports them), so this client never sends
/// `task.event`/`message.event`/`metric.frame` uplink — only `hello`,
/// `subscribe`, `unsubscribe`, `resume`, `interest.renew`, and viewer
/// `command`s.
///
/// All connection state lives inside this actor; callers observe it through
/// the three `AsyncStream`s (`phases`, `states`, `notices`) rather than by
/// reading actor-isolated properties directly.
public actor RealtimeClient {
    /// Connection lifecycle, per the brief: idle/connecting/handshaking/
    /// subscribed/live/failed.
    public enum Phase: Sendable, Equatable {
        case idle
        case connecting
        case handshaking
        case subscribed
        case live
        case failed(String)
    }

    /// One-off, non-state notifications a UI layer may want to surface.
    public enum Notice: Sendable, Equatable {
        /// This device's credential/connection was superseded by another
        /// connection (SPEC.md §9.4). SitrepApp never intentionally opens a
        /// second concurrent connection, so this always means "surprising"
        /// per §9.4's client guidance — surface a one-time, non-disruptive
        /// banner ("this session may be in use elsewhere"), don't reconnect
        /// in a tight loop.
        case superseded
        case serverError(RTErrorCode, message: String)
        case revisionGap(from: Int)
        /// WS has failed for `fallbackAfterFailures` consecutive backoff
        /// cycles; the caller should fall back to low-frequency HTTP
        /// polling until `.recovered`.
        case fellBackToPolling(reason: String)
        /// WS reached `.live` again after a fallback; the caller should stop
        /// the HTTP polling fallback.
        case recovered
    }

    public struct Configuration: Sendable {
        /// The realtime WebSocket endpoint (see `APIClient.realtimeURL`).
        public var url: URL
        /// Presented once at connection establishment (SPEC.md §10), as an
        /// `Authorization: Bearer` header — never repeated inside an
        /// envelope afterward.
        public var token: String?
        public var deviceID: String
        public var protocolVersions: [Int]
        public var topics: [RTTopic]

        public init(url: URL, token: String?, deviceID: String, protocolVersions: [Int] = [1], topics: [RTTopic] = []) {
            self.url = url
            self.token = token
            self.deviceID = deviceID
            self.protocolVersions = protocolVersions
            self.topics = topics
        }
    }

    /// Consecutive failed connection attempts (full backoff cycles) before
    /// signalling `.fellBackToPolling`.
    public var fallbackAfterFailures = 3

    private let configuration: Configuration

    private var runTask: Task<Void, Never>?
    private var socket: URLSessionWebSocketTask?
    private var leaseRenewalTask: Task<Void, Never>?
    private var watchdogTask: Task<Void, Never>?

    /// All resume/delta/snapshot gating and folding decisions are delegated
    /// to this shared, transport-agnostic state machine (SPEC.md §6.2/§6.3)
    /// — see `RealtimeResumeGateScenarioTests` for the same logic driven
    /// directly by scenario fixtures.
    private var gate: RealtimeResumeGate
    private var phase: Phase = .idle {
        didSet { phaseContinuation.yield(phase) }
    }

    private var consecutiveFailures = 0
    private var firedFallback = false
    private var lastInboundAt: Int = 0

    private let phaseContinuation: AsyncStream<Phase>.Continuation
    public nonisolated let phases: AsyncStream<Phase>
    private let noticeContinuation: AsyncStream<Notice>.Continuation
    public nonisolated let notices: AsyncStream<Notice>
    private let stateContinuation: AsyncStream<SpaceState>.Continuation
    public nonisolated let states: AsyncStream<SpaceState>

    public init(configuration: Configuration, initialState: SpaceState = SpaceState()) {
        self.configuration = configuration
        self.gate = RealtimeResumeGate(state: initialState)
        var pc: AsyncStream<Phase>.Continuation!
        self.phases = AsyncStream(bufferingPolicy: .bufferingNewest(1)) { pc = $0 }
        self.phaseContinuation = pc
        var nc: AsyncStream<Notice>.Continuation!
        self.notices = AsyncStream(bufferingPolicy: .bufferingNewest(8)) { nc = $0 }
        self.noticeContinuation = nc
        var sc: AsyncStream<SpaceState>.Continuation!
        self.states = AsyncStream(bufferingPolicy: .bufferingNewest(1)) { sc = $0 }
        self.stateContinuation = sc
    }

    public var currentState: SpaceState { gate.state }
    public var currentPhase: Phase { phase }

    // MARK: - Lifecycle

    /// Idempotent: does nothing if already running. Call when the app enters
    /// the foreground.
    public func start() {
        guard runTask == nil else { return }
        consecutiveFailures = 0
        firedFallback = false
        runTask = Task { [weak self] in await self?.runLoop() }
    }

    /// Tears down the connection and stops reconnecting. The interest lease
    /// naturally expires on the server side (SPEC.md §7) — this deliberately
    /// does not send `unsubscribe`, matching the brief: backgrounding closes
    /// the connection and lets the lease lapse rather than releasing it
    /// explicitly. Call when the app leaves the foreground.
    public func stop() {
        runTask?.cancel()
        runTask = nil
        teardownConnection()
        phase = .idle
    }

    private func runLoop() async {
        while !Task.isCancelled {
            do {
                try await connectOnce()
            } catch is CancellationError {
                break
            } catch {
                consecutiveFailures += 1
                phase = .failed(String(describing: error))
                if consecutiveFailures >= fallbackAfterFailures && !firedFallback {
                    firedFallback = true
                    noticeContinuation.yield(.fellBackToPolling(reason: String(describing: error)))
                }
            }
            guard !Task.isCancelled else { break }
            if consecutiveFailures > 0 {
                try? await Task.sleep(for: .seconds(backoffDelay(attempt: consecutiveFailures)))
            }
        }
        phase = .idle
    }

    private func backoffDelay(attempt: Int) -> Double {
        let capped = min(attempt, 6)
        let base = min(30.0, pow(2.0, Double(capped)))
        return base + Double.random(in: 0...(base * 0.3))
    }

    // MARK: - One connection's lifetime

    private func connectOnce() async throws {
        phase = .connecting
        var request = URLRequest(url: configuration.url)
        if let token = configuration.token, !token.isEmpty {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        let socket = URLSession.shared.webSocketTask(with: request)
        self.socket = socket
        socket.resume()
        defer { teardownConnection() }

        phase = .handshaking
        let accept = try await performHandshake(on: socket)
        startHeartbeat(on: socket, intervalMs: accept.heartbeatIntervalMs)

        let subscribeID = newEnvelopeID()
        try await sendFrame(
            .subscribe(Envelope(id: subscribeID, ts: nowMs(), body: SubscribeBody(topics: configuration.topics))),
            on: socket)
        let ackFrame = try await receiveFrame(on: socket)
        guard case .ack(let ackEnvelope) = ackFrame, let lease = ackEnvelope.body.lease else {
            if case .error(let e) = ackFrame { throw RealtimeClientError.serverError(e.body) }
            throw RealtimeClientError.protocolViolation("expected subscribe ack with lease, got \(ackFrame)")
        }
        phase = .subscribed
        scheduleLeaseRenewal(expiresAt: lease.expiresAt, on: socket)

        let lastRevision = gate.state.revision
        gate.beganResume(lastRevision: lastRevision)
        try await sendFrame(
            .resume(Envelope(id: newEnvelopeID(), ts: nowMs(), body: ResumeBody(lastRevision: lastRevision))),
            on: socket)

        while !Task.isCancelled {
            let frame = try await receiveFrame(on: socket)
            try await dispatch(frame, on: socket)
        }
    }

    private func teardownConnection() {
        socket?.cancel(with: .normalClosure, reason: nil)
        socket = nil
        leaseRenewalTask?.cancel()
        leaseRenewalTask = nil
        watchdogTask?.cancel()
        watchdogTask = nil
    }

    // MARK: - Handshake (§9)

    private func performHandshake(on socket: URLSessionWebSocketTask) async throws -> HelloAccept {
        let offer = HelloOffer(deviceID: configuration.deviceID, role: .viewer, protocolVersions: configuration.protocolVersions)
        try await sendFrame(.hello(Envelope(id: newEnvelopeID(), ts: nowMs(), body: .offer(offer))), on: socket)
        // §9.1: the client MUST NOT send anything else until the accept
        // arrives; we simply don't send anything else here. Recommended 10s
        // timeout on the client side.
        let frame = try await withTimeout(seconds: 10) { try await self.receiveFrame(on: socket) }
        switch frame {
        case .hello(let env):
            guard case .accept(let accept) = env.body else {
                throw RealtimeClientError.protocolViolation("client received a hello offer, expected accept")
            }
            return accept
        case .error(let env):
            throw RealtimeClientError.serverError(env.body)
        default:
            throw RealtimeClientError.protocolViolation("expected hello accept, got \(frame)")
        }
    }

    // MARK: - Frame dispatch (post-handshake, post-subscribe)

    private func dispatch(_ frame: RealtimeFrame, on socket: URLSessionWebSocketTask) async throws {
        switch frame {
        case .snapshot(let env):
            switch gate.receiveSnapshotChunk(env.body) {
            case .applied:
                markDeltaEligible()
                stateContinuation.yield(gate.state)
            case .snapshotChunkBuffered, .discarded:
                break
            case .gapDetected:
                break // not reachable for a snapshot chunk
            case .malformedSequence(let reason):
                throw RealtimeClientError.protocolViolation("snapshot chunk out of sequence: \(reason)")
            }
        case .delta(let env):
            switch gate.receiveDelta(env.body) {
            case .applied:
                markDeltaEligible()
                stateContinuation.yield(gate.state)
            case .discarded:
                break
            case .gapDetected(let resendLastRevision):
                noticeContinuation.yield(.revisionGap(from: resendLastRevision))
                try await sendFrame(
                    .resume(Envelope(id: newEnvelopeID(), ts: nowMs(), body: ResumeBody(lastRevision: resendLastRevision))),
                    on: socket)
            case .snapshotChunkBuffered, .malformedSequence:
                break // not reachable for a delta
            }
        case .metricFrame(let env):
            gate.receiveMetricFrame(env.body)
            stateContinuation.yield(gate.state)
        case .ack(let env):
            if let lease = env.body.lease {
                scheduleLeaseRenewal(expiresAt: lease.expiresAt, on: socket)
            }
        case .error(let env):
            try await handleError(env.body, on: socket)
        case .command, .hello, .subscribe, .unsubscribe, .interestRenew, .taskEvent, .messageEvent, .configEvent, .resume:
            // A conformant server never sends any of these to a viewer
            // (§10.1: server-only or source-only in the other direction).
            // Ignore defensively rather than tearing down the connection.
            break
        }
    }

    private func markDeltaEligible() {
        phase = .live
        if consecutiveFailures > 0 { consecutiveFailures = 0 }
        if firedFallback {
            firedFallback = false
            noticeContinuation.yield(.recovered)
        }
    }

    private func handleError(_ body: ErrorBody, on socket: URLSessionWebSocketTask) async throws {
        // §6.3: revision_unavailable answering our outstanding resume ⇒
        // retry with last_revision: 0, staying in the "awaiting reply" state.
        if gate.receiveErrorAnswersOutstandingResume(body) {
            try await sendFrame(
                .resume(Envelope(id: newEnvelopeID(), ts: nowMs(), body: ResumeBody(lastRevision: 0))),
                on: socket)
            return
        }

        if body.code == .superseded {
            noticeContinuation.yield(.superseded)
        } else {
            noticeContinuation.yield(.serverError(body.code, message: body.message))
        }

        if body.fatal {
            throw RealtimeClientError.serverError(body)
        }
    }

    // MARK: - Interest lease (§7)

    /// Renews at 1/3 of the window remaining before `expiresAt`, as
    /// instructed. Rescheduled every time we learn a fresh `expiresAt` (the
    /// initial subscribe ack, or any subsequent renew ack) — see `dispatch`.
    private func scheduleLeaseRenewal(expiresAt: Int, on socket: URLSessionWebSocketTask) {
        leaseRenewalTask?.cancel()
        let window = max(expiresAt - nowMs(), 0)
        let fireAfterMs = window - window / 3
        leaseRenewalTask = Task { [weak self] in
            try? await Task.sleep(for: .milliseconds(max(fireAfterMs, 0)))
            guard !Task.isCancelled else { return }
            try? await self?.sendFrame(
                .interestRenew(Envelope(
                    id: self?.newEnvelopeID() ?? UUID().uuidString, ts: self?.nowMs() ?? 0,
                    body: InterestRenewBody(topics: self?.configuration.topics ?? []))),
                on: socket)
        }
    }

    // MARK: - Heartbeat (§9.3)

    private func startHeartbeat(on socket: URLSessionWebSocketTask, intervalMs: Int) {
        watchdogTask?.cancel()
        lastInboundAt = nowMs()
        watchdogTask = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(for: .milliseconds(intervalMs))
                guard !Task.isCancelled, let self else { return }
                try? await self.sendPing(on: socket)
                let idleFor = await self.millisecondsSinceLastInbound()
                if idleFor > intervalMs * 2 {
                    // RECOMMENDED in §9.3: no pong (or any traffic) within
                    // 2x the interval ⇒ treat the connection as dead.
                    socket.cancel(with: .abnormalClosure, reason: nil)
                    return
                }
            }
        }
    }

    private func sendPing(on socket: URLSessionWebSocketTask) async throws {
        try await socket.send(.string("ping"))
    }

    private func millisecondsSinceLastInbound() -> Int { nowMs() - lastInboundAt }

    // MARK: - Transport helpers

    private func sendFrame(_ frame: RealtimeFrame, on socket: URLSessionWebSocketTask) async throws {
        let data = try frame.encoded()
        guard let text = String(data: data, encoding: .utf8) else {
            throw RealtimeClientError.protocolViolation("failed to encode outbound frame as UTF-8")
        }
        try await socket.send(.string(text))
    }

    /// Reads the next real envelope off the socket, transparently answering
    /// `ping` with `pong` and dropping stray `pong`/unrecognized-type frames
    /// (§9.3, §15) without surfacing them to `dispatch`.
    private func receiveFrame(on socket: URLSessionWebSocketTask) async throws -> RealtimeFrame {
        while true {
            let message = try await socket.receive()
            lastInboundAt = nowMs()
            let data: Data
            switch message {
            case .string(let text):
                if text == "ping" {
                    try? await socket.send(.string("pong"))
                    continue
                }
                if text == "pong" {
                    continue
                }
                guard let d = text.data(using: .utf8) else { continue }
                data = d
            case .data(let d):
                data = d
            @unknown default:
                continue
            }
            do {
                return try RealtimeFrame.decode(data)
            } catch RealtimeDecodingError.unknownType {
                continue // §15: ignore, do not treat as malformed
            }
        }
    }

    private func withTimeout<T: Sendable>(seconds: Double, operation: @escaping @Sendable () async throws -> T) async throws -> T {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask { try await operation() }
            group.addTask {
                try await Task.sleep(for: .seconds(seconds))
                throw RealtimeClientError.timeout
            }
            guard let result = try await group.next() else { throw RealtimeClientError.timeout }
            group.cancelAll()
            return result
        }
    }

    private func nowMs() -> Int { Int(Date().timeIntervalSince1970 * 1000) }
    private func newEnvelopeID() -> String { UUID().uuidString }
}
