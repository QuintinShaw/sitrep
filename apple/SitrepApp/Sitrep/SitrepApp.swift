import ActivityKit
import SwiftUI
import SitrepKit
import UserNotifications
import WidgetKit

final class AppDelegate: NSObject, UIApplicationDelegate, UNUserNotificationCenterDelegate {
    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        UNUserNotificationCenter.current().delegate = self
        return true
    }

    func application(
        _ application: UIApplication,
        didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data
    ) {
        let hex = deviceToken.hexString
        Task { await PushRegistrar.shared.uploadAlertToken(hex) }
    }

    /// Tapping an event/alert notification lands on the timeline, not just
    /// the app's front door.
    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse,
        withCompletionHandler completionHandler: @escaping () -> Void
    ) {
        NotificationCenter.default.post(name: .sitrepOpenEvents, object: nil)
        completionHandler()
    }

    /// Show banners even when the app is foreground.
    nonisolated func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification,
        withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void
    ) {
        completionHandler([.banner, .sound])
    }
}

@main
struct SitrepApp: App {
    @UIApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @State private var model = AppModel()

    init() {
        // Must run at launch, before any UI: when a Live Activity is created
        // by push-to-start, the system launches the app in the background and
        // this is our only chance to observe and upload the activity's push
        // token.
        Task { await PushRegistrar.shared.bootstrapFromDefaults() }
    }

    var body: some Scene {
        WindowGroup {
            MainTabView(model: model)
                .task { await model.start() }
        }
    }
}

@MainActor
@Observable
final class AppModel {
    private static let credentialSchemaVersion = 2
    private static let credentialSchemaVersionKey = "sitrep.credentialSchemaVersion"

    var tasks: [TaskState] = []
    var metrics: [MetricState] = []
    var events: [EventLogEntry] = []
    var automations: [AutomationInfo] = []
    var needsOnboarding = false
    var pendingJoinPayload: String?
    var deepLinkTask: String?
    var presence: PresenceInfo?
    /// Wall-clock of the last SUCCESSFUL refresh — staleness must be
    /// computed against this, not against server timestamps.
    var lastSyncAt: Date?

    /// The realtime WebSocket connection's lifecycle (SitrepKit's
    /// `RealtimeClient`). Drives the subtle sync-status indicator; `.live`
    /// means tasks/metrics/events/automations are updating from `delta`s in
    /// real time rather than from the HTTP fallback poll below.
    var connectionPhase: RealtimeClient.Phase = .idle
    /// One-shot: this device's realtime connection was superseded by
    /// another connection using the same credential (SPEC.md §9.4). Surfaced
    /// once, non-disruptively, then cleared by the view that shows it.
    var supersededNotice = false

    private var realtimeClient: RealtimeClient?
    private var realtimeObservers: [Task<Void, Never>] = []
    /// Low-frequency (>=30s) HTTP snapshot polling, used ONLY while the
    /// realtime connection is down for multiple backoff cycles — see
    /// `RealtimeClient.Notice.fellBackToPolling`/`.recovered`. This replaces
    /// what used to be an always-on 3s poll loop: normal sync now comes from
    /// `delta`s over the WebSocket.
    private var fallbackPollTask: Task<Void, Never>?
    /// Whether the fallback poll is *supposed* to be running (independent of
    /// whether `fallbackPollTask` is currently non-nil) — lets `ensurePolling()`
    /// revive it defensively if something silently killed the task while
    /// the realtime connection was still down.
    private var fallbackActive = false
    var lastError: String?

    // Keychain-backed (survives reinstalls); UserDefaults is a legacy
    // fallback for configs saved by earlier builds.
    var serverURL: String = KeychainStore.get("serverURL")
        ?? UserDefaults.standard.string(forKey: "serverURL") ?? ""
    var token: String = KeychainStore.get("token")
        ?? UserDefaults.standard.string(forKey: "token") ?? ""
    /// This phone's device record id in the space (from join), so
    /// disconnecting can revoke itself instead of leaving an orphan row.
    var deviceID: String = KeychainStore.get("deviceID") ?? ""

    var client: APIClient? {
        guard let url = URL(string: serverURL), !serverURL.isEmpty else { return nil }
        return APIClient(baseURL: url, token: token.isEmpty ? nil : token)
    }

    func saveSettings() {
        KeychainStore.set(serverURL, for: "serverURL")
        KeychainStore.set(token, for: "token")
        KeychainStore.set(deviceID, for: "deviceID")
        UserDefaults.standard.set(serverURL, forKey: "serverURL")
        UserDefaults.standard.set(token, forKey: "token")
        SharedConfig.save(server: serverURL, token: token)
        WidgetCenter.shared.reloadAllTimelines()
        let client = self.client
        Task { await PushRegistrar.shared.configure(client: client) }
        // Credentials changed under an existing connection (e.g. re-paired)
        // — reconnect with the new ones rather than keep talking under the
        // old identity.
        if realtimeClient != nil {
            stopRealtime()
            enterForeground()
        }
    }

    func start() async {
        resetLegacyCredentialsIfNeeded()
        needsOnboarding = serverURL.isEmpty || token.isEmpty
        // After a reinstall the keychain still has credentials but the App
        // Group container (widgets) and UserDefaults were wiped — re-seed.
        if !serverURL.isEmpty, SharedConfig.load() == nil {
            saveSettings()
        }
        await PushRegistrar.shared.configure(client: client)
        await PushRegistrar.shared.startObserving()
        // Seed the UI with one HTTP snapshot immediately; the realtime
        // connection (started below) then takes over as the live source.
        await refresh()
        enterForeground()
    }

    // MARK: - Realtime sync (foreground WebSocket + HTTP fallback)

    /// Call when the app becomes foreground-active. Idempotent.
    func enterForeground() {
        guard !needsOnboarding else { return }
        startRealtime()
    }

    /// Call when the app leaves the foreground. The interest lease simply
    /// lapses server-side (SPEC.md §7) rather than being released with an
    /// explicit `unsubscribe` — closing the connection is enough.
    func leaveBackground() {
        stopRealtime()
        stopFallbackPolling()
    }

    private func startRealtime() {
        guard let restClient = client, let url = restClient.realtimeURL, !deviceID.isEmpty else { return }
        if let existing = realtimeClient {
            // Idempotent on the actor side even if already running — this
            // doubles as the revival path if something silently killed the
            // connection's internal loop without going through `stop()`.
            Task { await existing.start() }
            return
        }
        let configuration = RealtimeClient.Configuration(
            url: url, token: token.isEmpty ? nil : token, deviceID: deviceID)
        let rt = RealtimeClient(configuration: configuration)
        realtimeClient = rt
        realtimeObservers = [
            Task { [weak self] in
                for await phase in rt.phases {
                    self?.connectionPhase = phase
                }
            },
            Task { [weak self] in
                for await state in rt.states {
                    self?.applyRealtimeState(state)
                }
            },
            Task { [weak self] in
                for await notice in rt.notices {
                    self?.handleRealtimeNotice(notice)
                }
            },
        ]
        Task { await rt.start() }
    }

    private func stopRealtime() {
        guard let rt = realtimeClient else { return }
        realtimeClient = nil
        for observer in realtimeObservers { observer.cancel() }
        realtimeObservers = []
        connectionPhase = .idle
        Task { await rt.stop() }
    }

    private func applyRealtimeState(_ state: SpaceState) {
        tasks = state.uiTasks
        let sortedMetrics = state.uiMetrics
        // Widgets refresh on a slow OS budget; while the app is foreground
        // we piggyback any change onto them so the two surfaces never
        // disagree (same rule `refresh()` applies for the HTTP path).
        if sortedMetrics != metrics {
            WidgetCenter.shared.reloadAllTimelines()
        }
        metrics = sortedMetrics
        events = state.uiEvents
        automations = state.uiAutomations
        lastError = nil
        lastSyncAt = .now
        adoptOrphanedTasks()
    }

    private func handleRealtimeNotice(_ notice: RealtimeClient.Notice) {
        switch notice {
        case .superseded:
            supersededNotice = true
        case .serverError(let code, let message):
            lastError = "\(code.rawValue): \(message)"
        case .revisionGap:
            break // transparent to the user; the client re-resumes on its own
        case .fellBackToPolling:
            startFallbackPolling()
        case .recovered:
            stopFallbackPolling()
        }
    }

    private func startFallbackPolling() {
        fallbackActive = true
        guard fallbackPollTask == nil else { return }
        fallbackPollTask = Task { [weak self] in
            while !Task.isCancelled {
                await self?.refresh()
                try? await Task.sleep(for: .seconds(30))
            }
        }
    }

    private func stopFallbackPolling() {
        fallbackActive = false
        fallbackPollTask?.cancel()
        fallbackPollTask = nil
    }

    /// v2 intentionally breaks the old space/auth model. Keychain values
    /// survive an app uninstall, so clear them once instead of silently
    /// retrying a v1 token against the v2 API forever.
    private func resetLegacyCredentialsIfNeeded() {
        let defaults = UserDefaults.standard
        guard defaults.integer(forKey: Self.credentialSchemaVersionKey)
                < Self.credentialSchemaVersion else { return }

        serverURL = ""
        token = ""
        deviceID = ""
        KeychainStore.set("", for: "serverURL")
        KeychainStore.set("", for: "token")
        KeychainStore.set("", for: "deviceID")
        defaults.removeObject(forKey: "serverURL")
        defaults.removeObject(forKey: "token")
        SharedConfig.save(server: "", token: "")
        defaults.set(Self.credentialSchemaVersion, forKey: Self.credentialSchemaVersionKey)
    }

    /// Ask only after the user has connected a computer and understands why
    /// Sitrep needs notifications. Live Activities work independently.
    func enableNotifications() async {
        let granted = try? await UNUserNotificationCenter.current()
            .requestAuthorization(options: [.alert, .sound, .badge])
        if granted == true {
            UIApplication.shared.registerForRemoteNotifications()
        }
    }

    /// Called on every `scenePhase == .active` transition (see
    /// `MainTabView`). Both the realtime connection and the HTTP fallback
    /// poll are owned, observable, and restartable — whatever kills them
    /// (suspension edge cases, cancellation), the next foreground pass
    /// revives them. This is the same self-healing shape the old always-on
    /// 3s poll loop used; it now applies to `RealtimeClient.start()`
    /// (idempotent even if already running) and to the fallback poll task.
    func ensurePolling() {
        enterForeground()
        if fallbackActive { startFallbackPolling() }
    }

    /// Local fallback for remote push-to-start: if the server has running
    /// tasks with no Live Activity on this device (start push throttled by
    /// the OS budget, dropped, or app reinstalled), create them locally —
    /// Activity.request has NO push-to-start budget. The activity's update
    /// token is then uploaded by PushRegistrar as usual, so server pushes
    /// take over from there.
    private func adoptOrphanedTasks() {
        guard ActivityAuthorizationInfo().areActivitiesEnabled else { return }
        let existing = Set(Activity<TaskActivityAttributes>.activities.map(\.attributes.sourceId))
        for task in tasks where task.status == .running && !existing.contains(task.sourceID) {
            _ = try? Activity.request(
                attributes: TaskActivityAttributes(sourceId: task.sourceID, title: task.title),
                content: .init(
                    state: .init(percent: task.percent, step: task.step, status: "running"),
                    staleDate: nil
                ),
                pushType: .token
            )
        }
    }

    func updateMetric(id: String, icon: String? = nil, tint: String? = nil,
                      template: String? = nil, level: String? = nil,
                      alertAbove: String? = nil, alertBelow: String? = nil) async {
        guard let client else { return }
        do {
            try await client.updateMetric(
                id: id, icon: icon, tint: tint, template: template, level: level,
                alertAbove: alertAbove, alertBelow: alertBelow
            )
            await refresh()
            WidgetCenter.shared.reloadAllTimelines()
        } catch {
            lastError = "style update failed: \(error.localizedDescription)"
        }
    }

    /// Revoke this phone's device record, clear credentials, return to
    /// onboarding. Other devices in the space are untouched.
    func disconnect() async {
        stopRealtime()
        stopFallbackPolling()
        if !deviceID.isEmpty {
            try? await client?.revokeDevice(id: deviceID)
        }
        serverURL = ""
        token = ""
        deviceID = ""
        KeychainStore.set("", for: "serverURL")
        KeychainStore.set("", for: "token")
        KeychainStore.set("", for: "deviceID")
        UserDefaults.standard.removeObject(forKey: "serverURL")
        UserDefaults.standard.removeObject(forKey: "token")
        SharedConfig.save(server: "", token: "")
        tasks = []
        metrics = []
        events = []
        automations = []
        needsOnboarding = true
    }

    func send(_ command: TaskCommand, to task: TaskState) async {
        guard let client else { return }
        do {
            try await client.sendCommand(command, to: task.sourceID)
        } catch {
            lastError = "command failed: \(error.localizedDescription)"
        }
    }

    func refresh() async {
        guard let client else { return }
        do {
            let snapshot = try await client.snapshot()
            presence = snapshot.presence
            events = snapshot.messages.map(\.state)
            automations = snapshot.automations.sorted { $0.name < $1.name }
            tasks = snapshot.tasks.sorted { $0.updatedAt > $1.updatedAt }
            let sorted = snapshot.metrics.map(\.state).sorted { $0.key < $1.key }
            // Widgets refresh on a slow OS budget; while the app is foreground
            // we piggyback any change — values AND merged style prefs — onto
            // them so the two surfaces never disagree.
            if sorted != metrics {
                WidgetCenter.shared.reloadAllTimelines()
            }
            metrics = sorted
            lastError = nil
            lastSyncAt = .now
            adoptOrphanedTasks()
        } catch {
            lastError = error.localizedDescription
        }
    }
}
