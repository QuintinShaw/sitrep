import Foundation

/// Errors `RealtimeClient` raises itself (as opposed to a `RTErrorCode` the
/// server sent, which arrives wrapped in `.serverError`).
public enum RealtimeClientError: Error, Sendable {
    case timeout
    case protocolViolation(String)
    case serverError(ErrorBody)
    /// The WS upgrade itself was rejected with `503
    /// {"error": "transport_unavailable"}` (v1-architecture.md §8.1) — this
    /// deployment currently has `WS_TRANSPORT_ENABLED=false`. Distinct from
    /// every other connect failure: it is a capability fact, not a
    /// transient network hiccup, so the run loop treats it as terminal
    /// (`isTerminal`) instead of feeding the ordinary backoff/retry cycle.
    case transportUnavailable
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
        /// The WS upgrade got `503 transport_unavailable` (v1-architecture.md
        /// §8.1): the caller MUST NOT keep retrying the socket — fall back
        /// to HTTP snapshot polling against the same space immediately, and
        /// treat this like a capability refresh reporting `false` (reset
        /// `RealtimeCapabilityGate` so the next snapshot reporting `true`
        /// re-probes cleanly rather than being masked by a stale cached
        /// `true`).
        case transportUnavailable
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
    /// §13: `malformed` is non-fatal — a single undecodable frame is
    /// skipped, not a reason to drop the connection. This counts the
    /// current run of consecutive malformed frames so a persistently
    /// broken peer still trips a circuit breaker (see `receiveFrame`).
    private var consecutiveMalformedFrames = 0
    /// Number of consecutive malformed frames after which the connection is
    /// torn down and re-established (circuit breaker).
    private let malformedFrameCircuitBreaker = 5
    /// Distinguishes which `runLoop` invocation currently owns the
    /// connection resources (`runTask`, `socket`, `leaseRenewalTask`,
    /// `watchdogTask`). A stale generation — a loop still unwinding after a
    /// stop()/start() cycle on the same instance — must neither clear the
    /// newer loop's task handle nor tear down or replace its connection
    /// resources (see `ConnectionTeardownPolicy`).
    private var runGeneration = 0

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
    /// the foreground. Also the restart path after a terminal stop (see
    /// `runLoop`): once the loop has exited — including after a
    /// fatal-and-not-retryable server error — a later `start()` begins a
    /// fresh reconnect cycle.
    public func start() {
        guard runTask == nil else { return }
        consecutiveFailures = 0
        firedFallback = false
        runGeneration += 1
        let generation = runGeneration
        runTask = Task { [weak self] in await self?.runLoop(generation: generation) }
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

    /// True for a server error whose own `retryable`/`fatal` flags say
    /// "connection closes AND retrying the same thing cannot succeed" —
    /// `superseded`, `unauthenticated`, `version_unsupported` (§13). Per
    /// ruling, these stop the run loop instead of feeding the reconnect
    /// cycle: the notice has already been surfaced, and reconnection waits
    /// for an explicit user action or the next foreground `start()`.
    private func isTerminal(_ error: Error) -> Bool {
        if case RealtimeClientError.serverError(let body) = error {
            return body.fatal && !body.retryable
        }
        if case RealtimeClientError.transportUnavailable = error {
            return true
        }
        return false
    }

    private func runLoop(generation: Int) async {
        var terminal = false
        while !Task.isCancelled {
            do {
                try await connectOnce(generation: generation)
            } catch is CancellationError {
                break
            } catch {
                if case RealtimeClientError.transportUnavailable = error {
                    // A capability fact, not a network hiccup: fall back to
                    // HTTP immediately rather than waiting out
                    // `fallbackAfterFailures` backoff cycles, and don't feed
                    // consecutiveFailures — a later explicit start() (once a
                    // snapshot proves capability again) begins a clean run.
                    phase = .failed(String(describing: error))
                    noticeContinuation.yield(.transportUnavailable)
                    terminal = true
                    break
                }
                if isTerminal(error) {
                    phase = .failed(String(describing: error))
                    terminal = true
                    break
                }
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
        // Only the newest loop may release the handle (a stale loop exiting
        // after stop()+start() must not clobber its successor's), and a
        // terminal exit keeps the `.failed` phase visible instead of
        // resetting to `.idle`.
        if runGeneration == generation {
            runTask = nil
            if !terminal { phase = .idle }
        }
    }

    private func backoffDelay(attempt: Int) -> Double {
        let capped = min(attempt, 6)
        let base = min(30.0, pow(2.0, Double(capped)))
        return base + Double.random(in: 0...(base * 0.3))
    }

    // MARK: - One connection's lifetime

    private func connectOnce(generation: Int) async throws {
        phase = .connecting
        consecutiveMalformedFrames = 0
        var request = URLRequest(url: configuration.url)
        if let token = configuration.token, !token.isEmpty {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        let socket = URLSession.shared.webSocketTask(with: request)
        self.socket = socket
        socket.resume()
        // A stale generation's deferred teardown (this connectOnce resuming
        // from suspension after a stop()/start() cycle) must not cancel the
        // NEWER generation's socket/lease/watchdog. It still closes its own
        // socket, which it holds by reference.
        defer { teardownConnection(requestedBy: generation, ownSocket: socket) }

        phase = .handshaking
        let accept: HelloAccept
        do {
            accept = try await performHandshake(on: socket)
        } catch {
            throw Self.classifyConnectFailure(error, response: socket.response as? HTTPURLResponse)
        }
        startHeartbeat(on: socket, intervalMs: accept.heartbeatIntervalMs, generation: generation)

        let subscribeID = newEnvelopeID()
        try await sendFrame(
            .subscribe(Envelope(id: subscribeID, ts: nowMs(), body: SubscribeBody(topics: configuration.topics))),
            on: socket)
        let ackFrame = try await receiveFrame(on: socket)
        guard case .ack(let ackEnvelope) = ackFrame, let lease = ackEnvelope.body.lease else {
            if case .error(let e) = ackFrame {
                // Route through handleError so the notice is surfaced and
                // fatal errors (incl. terminal ones) throw with the server's
                // own retryable/fatal semantics attached.
                try await handleError(e.body, on: socket)
            }
            throw RealtimeClientError.protocolViolation("expected subscribe ack with lease, got \(ackFrame)")
        }
        phase = .subscribed
        scheduleLeaseRenewal(expiresAt: lease.expiresAt, on: socket, generation: generation)

        let lastRevision = gate.state.revision
        gate.beganResume(lastRevision: lastRevision)
        try await sendFrame(
            .resume(Envelope(id: newEnvelopeID(), ts: nowMs(), body: ResumeBody(lastRevision: lastRevision))),
            on: socket)

        while !Task.isCancelled {
            let frame = try await receiveFrame(on: socket)
            try await dispatch(frame, on: socket, generation: generation)
        }
    }

    /// Tears down the shared connection resources — but only when the
    /// requesting generation still owns them (`requestedBy: nil` is the
    /// unconditional form used by `stop()`). A stale generation's deferred
    /// call limits itself to closing the socket it created (`ownSocket`),
    /// leaving the newer generation's resources untouched.
    private func teardownConnection(requestedBy generation: Int? = nil, ownSocket: URLSessionWebSocketTask? = nil) {
        guard ConnectionTeardownPolicy.shouldTearDownShared(requestedBy: generation, current: runGeneration) else {
            ownSocket?.cancel(with: .normalClosure, reason: nil)
            return
        }
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
            // handleError surfaces the notice and, for fatal codes (e.g.
            // version_unsupported), throws serverError so the run loop can
            // apply the terminal/retryable distinction.
            try await handleError(env.body, on: socket)
            throw RealtimeClientError.protocolViolation("handshake answered with a non-fatal error; reconnecting")
        default:
            throw RealtimeClientError.protocolViolation("expected hello accept, got \(frame)")
        }
    }

    // MARK: - Frame dispatch (post-handshake, post-subscribe)

    private func dispatch(_ frame: RealtimeFrame, on socket: URLSessionWebSocketTask, generation: Int) async throws {
        // §6.2: while a chunked snapshot is in flight, only further snapshot
        // chunks (and ping/pong, which never reach dispatch) are legal on
        // this connection. Anything else is a malformed sequence: drop the
        // buffered chunks and reconnect; the fresh connection resumes anew.
        if gate.isSnapshotInFlight {
            guard case .snapshot = frame else {
                _ = gate.interleavedFrameDuringSnapshot()
                throw RealtimeClientError.protocolViolation(
                    "non-snapshot envelope interleaved during a chunked snapshot")
            }
        }
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
                scheduleLeaseRenewal(expiresAt: lease.expiresAt, on: socket, generation: generation)
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
    /// A stale generation (its connection already superseded by a
    /// stop()/start() cycle) must not replace the current generation's
    /// renewal task.
    private func scheduleLeaseRenewal(expiresAt: Int, on socket: URLSessionWebSocketTask, generation: Int) {
        guard generation == runGeneration else { return }
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

    private func startHeartbeat(on socket: URLSessionWebSocketTask, intervalMs: Int, generation: Int) {
        guard generation == runGeneration else { return }
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
                let frame = try RealtimeFrame.decode(data)
                consecutiveMalformedFrames = 0
                return frame
            } catch RealtimeDecodingError.unknownType {
                continue // §15: ignore, do not treat as malformed
            } catch {
                // §13: `malformed` is non-fatal — skip this frame and keep
                // the connection. The circuit breaker only trips on a run
                // of consecutive malformed frames, which indicates the
                // stream itself is broken rather than one bad envelope.
                consecutiveMalformedFrames += 1
                if consecutiveMalformedFrames >= malformedFrameCircuitBreaker {
                    consecutiveMalformedFrames = 0
                    throw RealtimeClientError.protocolViolation(
                        "\(malformedFrameCircuitBreaker) consecutive malformed frames; reconnecting (last: \(error))")
                }
                continue
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

    /// Pure reclassification of a handshake failure: if the WS upgrade was
    /// rejected with HTTP `503` (v1-architecture.md §8.1's
    /// `transport_unavailable`, surfaced here since `URLSessionWebSocketTask`
    /// answers a non-101 upgrade response as a task failure rather than a
    /// readable frame), report `.transportUnavailable` instead of the raw
    /// transport error so the run loop can treat it as a capability fact
    /// rather than a retryable network hiccup. `nonisolated` and `static` —
    /// no actor state involved — so this is directly unit-testable with a
    /// synthetic `HTTPURLResponse`, without a live socket.
    static func classifyConnectFailure(_ error: Error, response: HTTPURLResponse?) -> Error {
        guard let response, response.statusCode == 503 else { return error }
        return RealtimeClientError.transportUnavailable
    }
}

/// Pure decision logic for generation-scoped teardown of the shared
/// connection resources (`socket`/`leaseRenewalTask`/`watchdogTask`),
/// extracted so the guard itself is directly unit-testable without a live
/// transport (`RealtimeGenerationTests`).
enum ConnectionTeardownPolicy {
    /// - `requestedBy == nil`: the unconditional form (`stop()`); always
    ///   tears down.
    /// - `requestedBy == current`: the owning generation's own teardown;
    ///   proceeds.
    /// - `requestedBy < current`: a stale generation unwinding after a
    ///   stop()/start() cycle; it must NOT touch the newer generation's
    ///   shared resources (it may still close the socket it created and
    ///   holds by reference).
    static func shouldTearDownShared(requestedBy generation: Int?, current: Int) -> Bool {
        guard let generation else { return true }
        return generation == current
    }
}

public extension RealtimeClient.Phase {
    /// Whether an HTTP snapshot result may overwrite the four reliable
    /// collections (tasks/metrics/messages-events/automations). While the
    /// realtime connection is `.live`, deltas own that state and an HTTP
    /// response — possibly computed seconds ago — must not clobber it; in
    /// every other phase the HTTP path is the best available source.
    ///
    /// Callers MUST evaluate this AFTER their HTTP `await` returns, not
    /// before issuing the request: the phase can flip to `.live` while the
    /// request is in flight, and it is the state of the world at apply time
    /// that decides. REST-only concepts the realtime protocol does not
    /// carry (e.g. `presence`) are exempt and always apply.
    var allowsReliableStateOverwrite: Bool { self != .live }
}
