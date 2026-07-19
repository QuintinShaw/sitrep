import Foundation

/// The pure decision logic behind SPEC.md §6.2/§6.3's resume/delta/snapshot
/// gating and §6.4's folding, extracted from `RealtimeClient` so it has
/// exactly one implementation shared by the live client and by tests that
/// drive it directly with decoded fixtures — no WebSocket transport
/// required. `RealtimeClient` is the only thing that owns a socket; this
/// type only ever sees already-decoded `RealtimeFrame` bodies and reports
/// what happened, never performs I/O itself.
public struct RealtimeResumeGate: Sendable, Equatable {
    public private(set) var state: SpaceState
    /// True once this connection has received a snapshot-or-delta reply to
    /// its most recent `resume` (SPEC.md §6.2/§6.3) — i.e. it may now accept
    /// live deltas via the ordinary `from_revision` comparison.
    public private(set) var deltaEligible = false
    /// Non-nil while a resume reply (initial, or a gap-triggered re-resume)
    /// is outstanding; holds the `last_revision` that was requested, so a
    /// racing stray delta (one that doesn't match) can be told apart from
    /// the actual reply.
    public private(set) var awaitingResumeReply: Int?
    private var pendingSnapshotChunks: [SnapshotBody] = []

    public init(state: SpaceState = SpaceState()) {
        self.state = state
    }

    public enum Outcome: Sendable, Equatable {
        /// The frame was folded into `state` (a snapshot final chunk, a
        /// delta that matched, or the resume reply itself).
        case applied
        /// Silently ignored per SPEC.md §6.3: a pre-reply delta, a stale
        /// `from_revision < C` delta, or a stray that doesn't match an
        /// outstanding resume request.
        case discarded
        /// `from_revision > C` on an already-eligible connection: the
        /// caller MUST send a fresh `resume{last_revision: resendLastRevision}`.
        case gapDetected(resendLastRevision: Int)
        /// A non-final snapshot chunk was buffered; nothing applied yet.
        case snapshotChunkBuffered
        /// The peer violated §6.2's chunk sequencing (SPEC.md: "a viewer
        /// that receives any non-ping/pong envelope between chunks MUST
        /// treat it as malformed"; the same applies to a genuinely
        /// out-of-sequence chunk). The caller should close and reconnect.
        case malformedSequence(String)
    }

    /// True while a chunked snapshot is being reassembled (a non-final
    /// chunk has arrived and the `final` one has not). Per SPEC.md §6.2 the
    /// server MUST NOT interleave any other envelope in this window; the
    /// caller must route any non-snapshot frame it receives during it to
    /// `interleavedFrameDuringSnapshot()` instead of processing it.
    public var isSnapshotInFlight: Bool { !pendingSnapshotChunks.isEmpty }

    /// SPEC.md §6.2: "a viewer that receives any non-`ping`/`pong` envelope
    /// between chunks MUST treat it as a malformed sequence and MAY close
    /// the connection and reconnect." Discards the buffered chunks (they
    /// can never be completed by a conformant continuation now) and reports
    /// the malformed sequence; the caller closes and reconnects, and the
    /// next connection resumes afresh.
    public mutating func interleavedFrameDuringSnapshot() -> Outcome {
        pendingSnapshotChunks = []
        return .malformedSequence("non-snapshot envelope interleaved during a chunked snapshot")
    }

    /// Call once, immediately after sending `resume{last_revision: N}` —
    /// including the very first resume on a fresh connection and any
    /// gap-triggered re-resume.
    public mutating func beganResume(lastRevision: Int) {
        awaitingResumeReply = lastRevision
        deltaEligible = false
        pendingSnapshotChunks = []
    }

    public mutating func receiveSnapshotChunk(_ chunk: SnapshotBody) -> Outcome {
        if let first = pendingSnapshotChunks.first {
            guard chunk.revision == first.revision, chunk.part == pendingSnapshotChunks.count + 1 else {
                return .malformedSequence(
                    "expected revision \(first.revision) part \(pendingSnapshotChunks.count + 1), got revision \(chunk.revision) part \(chunk.part)")
            }
        } else {
            guard chunk.part == 1 else {
                return .malformedSequence("snapshot chunk sequence must start at part 1, got \(chunk.part)")
            }
        }
        pendingSnapshotChunks.append(chunk)
        guard chunk.final else { return .snapshotChunkBuffered }

        let aggregated = SnapshotBody(
            revision: chunk.revision, part: 1, final: true,
            tasks: pendingSnapshotChunks.flatMap(\.tasks),
            metrics: pendingSnapshotChunks.flatMap(\.metrics),
            messages: pendingSnapshotChunks.flatMap(\.messages),
            automations: pendingSnapshotChunks.flatMap(\.automations))
        pendingSnapshotChunks = []
        state.apply(snapshot: aggregated)
        awaitingResumeReply = nil
        deltaEligible = true
        return .applied
    }

    public mutating func receiveDelta(_ body: DeltaBody) -> Outcome {
        // §6.2: nothing but further snapshot chunks may arrive while a
        // chunked snapshot is being reassembled.
        if isSnapshotInFlight { return interleavedFrameDuringSnapshot() }
        if let requested = awaitingResumeReply {
            guard body.fromRevision == requested else { return .discarded }
            state.apply(delta: body)
            awaitingResumeReply = nil
            deltaEligible = true
            return .applied
        }
        guard deltaEligible else { return .discarded }

        if body.fromRevision < state.revision {
            return .discarded
        } else if body.fromRevision == state.revision {
            state.apply(delta: body)
            return .applied
        } else {
            let resendFrom = state.revision
            awaitingResumeReply = resendFrom
            deltaEligible = false
            return .gapDetected(resendLastRevision: resendFrom)
        }
    }

    public mutating func receiveMetricFrame(_ body: MetricFrameBody) {
        state.apply(metricFrame: body)
    }

    /// `true` if this error is the reply to our outstanding resume
    /// (`revision_unavailable`, SPEC.md §6.3's decision table): the caller
    /// MUST retry with `resume{last_revision: 0}`. The gate stays in the
    /// "awaiting reply" state (now for revision 0) either way.
    public mutating func receiveErrorAnswersOutstandingResume(_ body: ErrorBody) -> Bool {
        guard awaitingResumeReply != nil, body.code == .revisionUnavailable else { return false }
        awaitingResumeReply = 0
        return true
    }
}
