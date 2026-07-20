import Foundation
import Observation
import SitrepKit

/// Polls the server and exposes space state to the UI. On first launch this
/// silently creates an anonymous space (Onboarding) and starts supervising
/// the Go agent that executes automations.
@MainActor
@Observable
final class StatusModel {
    private(set) var tasks: [TaskState] = []
    private(set) var metrics: [MetricState] = []
    private(set) var automations: [AutomationInfo] = []
    private(set) var lastError: String?
    private(set) var configured = false
    private(set) var serverURL = ""
    /// Non-nil when the daemon reported a local telemetry storage anomaly
    /// (N1): its `device_seq` outbox DB is unwritable, so task/metric events
    /// may be silently dropped. The string is the aggregated warning text
    /// (naming each unhealthy component when more than one is failing).
    /// Read from `~/.config/sitrep/health.d/*.json` each poll — absence
    /// (of the directory, or of any given component file) means "healthy,"
    /// never an error, and a component's `ok: false` file that is stale
    /// (mtime older than 5 minutes) is treated as resolved (see
    /// `LocalTelemetryHealthDirectory`, v1-architecture.md §14).
    private(set) var telemetryStorageError: String?

    var client: APIClient?
    private var pollTask: Task<Void, Never>?

    var runningCount: Int { tasks.filter { $0.status == .running }.count }
    var hasFailure: Bool {
        tasks.contains { $0.status == .failed && $0.updatedAt > Date(timeIntervalSinceNow: -3600) }
    }
    /// Whether the menu-bar glyph should flag an alarm — a task failure OR a
    /// local telemetry storage anomaly (either means the user needs to look).
    var needsAttention: Bool { hasFailure || telemetryStorageError != nil }

    init() {
        pollTask = Task { [weak self] in
            guard let self else { return }
            if let cfg = await Onboarding.ensureSpace() {
                self.serverURL = cfg.server
                self.client = cfg.makeClient()
                self.configured = self.client != nil
                AgentSupervisor.shared.start()
            }
            while !Task.isCancelled {
                await self.refresh()
                try? await Task.sleep(for: .seconds(2))
            }
        }
    }

    func refresh() async {
        // The daemon's storage-health signal is independent of server
        // reachability — surface it even when the space isn't configured or
        // the snapshot fetch fails, since a dead local outbox is exactly the
        // case where the server has stopped hearing from this Mac.
        refreshTelemetryHealth()
        guard let client else { return }
        do {
            let snapshot = try await client.snapshot()
            tasks = snapshot.tasks.sorted { $0.updatedAt > $1.updatedAt }
            // v1 has no server-side metric display preference route
            // (v1-architecture.md §2.3, §13.2) — this Mac's own local
            // overrides layer on top, independent of the phone's.
            metrics = snapshot.metrics
                .map { MetricPreferencesStore.apply(to: $0.state) }
                .sorted { $0.key < $1.key }
            automations = snapshot.automations.sorted { $0.name < $1.name }
            lastError = nil
        } catch {
            lastError = error.localizedDescription
        }
    }

    /// Reads and aggregates the daemon's `health.d/` directory off the main
    /// actor (small blocking directory listing + per-file reads), then
    /// publishes the result. `nil` means healthy/no-signal (directory or
    /// component file absent, all `ok: true`, or all `ok: false` files
    /// stale), so this clears the warning.
    private func refreshTelemetryHealth() {
        telemetryStorageError = LocalTelemetryHealthDirectory.aggregate()
    }

    /// Restyle a metric from the Mac. v1 keeps display preferences entirely
    /// client-local (no server round-trip) — this menu bar app and the
    /// phone app each store their own; the two intentionally do NOT cascade
    /// to one another in v1 (v1-architecture.md §2.3, §13.2).
    func setStyle(metric: MetricState, tint: String? = nil, template: String? = nil) async {
        MetricPreferencesStore.update(metric.key, tint: tint, template: template)
        await refresh()
    }

    func setAutomation(_ automation: AutomationInfo, everyS: Int? = nil, enabled: Bool? = nil) async {
        try? await client?.patchAutomation(id: automation.id, everyS: everyS, enabled: enabled)
        await refresh()
    }

    func deleteAutomation(_ automation: AutomationInfo) async {
        try? await client?.deleteAutomation(id: automation.id)
        await refresh()
    }
}
