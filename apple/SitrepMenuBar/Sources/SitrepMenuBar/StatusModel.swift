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

    var client: APIClient?
    private var pollTask: Task<Void, Never>?

    var runningCount: Int { tasks.filter { $0.status == .running }.count }
    var hasFailure: Bool {
        tasks.contains { $0.status == .failed && $0.updatedAt > Date(timeIntervalSinceNow: -3600) }
    }

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
        guard let client else { return }
        do {
            let snapshot = try await client.snapshot()
            tasks = snapshot.tasks.sorted { $0.updatedAt > $1.updatedAt }
            metrics = snapshot.metrics.map(\.state).sorted { $0.key < $1.key }
            automations = snapshot.automations.sorted { $0.name < $1.name }
            lastError = nil
        } catch {
            lastError = error.localizedDescription
        }
    }

    /// Restyle a metric from the Mac — the pref cascades to the phone app,
    /// its widgets, and this panel on the next poll.
    func setStyle(metric: MetricState, tint: String? = nil, template: String? = nil) async {
        try? await client?.updateMetric(id: metric.key, tint: tint, template: template)
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
