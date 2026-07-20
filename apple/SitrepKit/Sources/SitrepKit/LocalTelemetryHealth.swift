import Foundation

/// The local health signal the Sitrep daemon writes to disk so a GUI can
/// surface a storage-plane anomaly the user would otherwise lose silently.
///
/// Motivation (integration review N1): the daemon's persistent `device_seq`
/// allocator is backed by a local outbox DB. If that DB becomes unwritable,
/// the daemon can no longer allocate the monotonic `device_seq` that
/// `POST /v1/events` dedup/fold depends on — and silently dropping events in
/// that state is NOT acceptable "self-healing." The daemon instead writes an
/// explicit health file; the menu bar reads it and shows a clear error so the
/// user knows task reports may be getting dropped.
///
/// ## Contract, one file per component (v1-architecture.md §14)
/// A single shared `health.json`, read-modify-written by multiple
/// independent short-lived daemon processes, could race: one process
/// recomputing the whole file from only what it locally knows could
/// silently clobber another process's currently-reported issue for a
/// *different* component. v1 replaces it with a directory, one small file
/// per component, so concurrent processes touching different components
/// never race with each other at all.
///
/// - **Path:** `~/.config/sitrep/health.d/<component>.json` (e.g.
///   `outbox.json`, `device_seq.json`, `outbox_open.json` — the set of
///   component names is extensible; any name is a valid file).
/// - **Shape (per file):** `{ "ok": bool, "reason": string }` — `reason`
///   optional, present to explain a `false`.
/// - **Absence == healthy**, at both levels: the entire `health.d/`
///   directory being absent, or one specific component's file being
///   absent, both mean that component (or, for a missing directory, every
///   component) is healthy. A component is never "unknown/error" by
///   default — only an explicit `ok: false` file signals a problem.
/// - **Staleness resolves a component without an active recovery write:** a
///   component file only contributes to the aggregate warning if it is
///   BOTH `ok: false` AND not stale. A file is stale when
///   `now - mtime > staleAfter` (5 minutes) — this is what lets a
///   short-lived process's failure self-heal even if it crashes before it
///   can write a follow-up `{"ok": true}`, instead of leaving a permanent
///   false alarm.
/// - **Migration:** the old single `~/.config/sitrep/health.json` is
///   deleted, replaced entirely by `health.d/`. No dual-write, no
///   back-compat reader for the old path (unreleased product).
public struct LocalTelemetryHealth: Codable, Sendable, Equatable {
    /// `false` signals a storage-plane anomaly (e.g. the outbox DB is
    /// unwritable and `device_seq` can't be allocated) — events may be
    /// dropped until it clears.
    public var ok: Bool
    /// Human-readable explanation for a `false`, surfaced in the menu.
    public var reason: String?

    public init(ok: Bool, reason: String? = nil) {
        self.ok = ok
        self.reason = reason
    }

    enum CodingKeys: String, CodingKey {
        case ok, reason
    }

    /// Pure parse from raw file bytes — decode logic kept separate from I/O
    /// so it is directly unit-testable without touching the filesystem.
    /// A malformed/empty payload throws (the caller treats a decode failure
    /// as "no usable signal," i.e. healthy, exactly like an absent file).
    public static func parse(_ data: Data) throws -> LocalTelemetryHealth {
        try JSONDecoder().decode(LocalTelemetryHealth.self, from: data)
    }

    /// The anomaly this signal represents, if any: non-nil only when a
    /// signal was present AND reported `ok: false`. The associated string is
    /// the `reason` (or a generic fallback) for direct display in the menu.
    public var anomalyReason: String? {
        guard !ok else { return nil }
        let trimmed = reason?.trimmingCharacters(in: .whitespacesAndNewlines)
        if let trimmed, !trimmed.isEmpty { return trimmed }
        return "本地遥测存储异常，任务上报可能丢失"
    }
}

/// Reads and aggregates `~/.config/sitrep/health.d/*.json` (v1-architecture.md
/// §14) into a single warning string for the menu bar. Cross-platform in its
/// pure form (`aggregate(directory:now:)` takes an explicit URL); the
/// canonical on-disk location (`directoryURL`) is macOS-only, since the
/// daemon that writes it only runs on the Mac.
public enum LocalTelemetryHealthDirectory {
    /// `HEALTH_STALE_AFTER_MS` (v1-architecture.md §14) = 300000 ms = 5 min.
    public static let staleAfter: TimeInterval = 5 * 60

    #if os(macOS)
    /// The canonical on-disk location, shared with `ClientConfig`'s
    /// `~/.config/sitrep` directory.
    public static var directoryURL: URL {
        FileManager.default.homeDirectoryForCurrentUser
            .appending(path: ".config/sitrep/health.d")
    }
    #endif

    /// One component's parsed, non-stale `ok: false` report, ready for
    /// display.
    public struct FailingComponent: Equatable, Sendable {
        /// The file's base name without `.json` (e.g. "outbox", "device_seq").
        public var component: String
        public var reason: String
    }

    /// Scans `directory`, parses every `*.json` file, and returns the
    /// components that are BOTH `ok: false` AND not stale as of `now` —
    /// sorted by component name for a deterministic display order. An
    /// absent directory, a directory with only `ok: true`/absent files, or
    /// one where every `ok: false` file is stale all yield `[]` (healthy).
    /// Malformed individual files are treated as "no usable signal" (same
    /// as absent) and skipped, not surfaced as an error — matching the
    /// per-file `parse` contract.
    public static func failingComponents(in directory: URL, now: Date = Date()) -> [FailingComponent] {
        let fm = FileManager.default
        guard let entries = try? fm.contentsOfDirectory(
            at: directory, includingPropertiesForKeys: [.contentModificationDateKey]
        ) else { return [] } // absent directory == healthy

        var failing: [FailingComponent] = []
        for url in entries where url.pathExtension == "json" {
            guard let data = try? Data(contentsOf: url),
                  let health = try? LocalTelemetryHealth.parse(data),
                  let reason = health.anomalyReason
            else { continue } // absent/malformed/ok:true == healthy for this file

            guard let mtime = (try? url.resourceValues(forKeys: [.contentModificationDateKey]))?
                .contentModificationDate
            else { continue }
            // Stale ok:false is treated as resolved (§14): a short-lived
            // process's failure ages out of the aggregate after
            // `staleAfter` with no fresh re-report, instead of becoming a
            // permanent false alarm.
            guard now.timeIntervalSince(mtime) <= staleAfter else { continue }

            let component = url.deletingPathExtension().lastPathComponent
            failing.append(FailingComponent(component: component, reason: reason))
        }
        return failing.sorted { $0.component < $1.component }
    }

    /// Combines `failingComponents(in:now:)` into the single warning string
    /// the menu bar shows — `nil` when healthy. With more than one failing
    /// component, each is prefixed with its component name so the user can
    /// tell which is failing; with exactly one, the bare reason is shown
    /// (matching the prior single-file design's message).
    public static func aggregate(directory: URL, now: Date = Date()) -> String? {
        let failing = failingComponents(in: directory, now: now)
        guard !failing.isEmpty else { return nil }
        if failing.count == 1 { return failing[0].reason }
        return failing.map { "\($0.component)：\($0.reason)" }.joined(separator: "；")
    }

    #if os(macOS)
    /// Reads and aggregates the daemon's health directory from its
    /// canonical macOS location.
    public static func aggregate(now: Date = Date()) -> String? {
        aggregate(directory: directoryURL, now: now)
    }
    #endif
}
