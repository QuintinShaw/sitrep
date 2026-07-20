import ActivityKit
import Foundation
import SitrepKit

/// Uploads Live Activity push tokens to the server:
/// - the device's push-to-start token (lets the server CREATE activities)
/// - each activity's update token (lets the server UPDATE/END it)
///
/// Tokens rotate (~8h); the async sequences below emit on every rotation and
/// we re-upload. The server keeps only the latest per device/source.
actor PushRegistrar {
    static let shared = PushRegistrar()

    private var client: APIClient?
    private var started = false

    private var lastPushToStartToken: String?

    func configure(client: APIClient?) {
        let hadClient = self.client != nil
        self.client = client
        // If the token arrived before credentials were saved, re-upload now.
        if !hadClient, client != nil, let token = lastPushToStartToken {
            Task { await self.uploadPushTokens(pushToStartToken: token) }
        }
    }

    func startObserving() {
        guard !started else { return }
        started = true
        // The updates stream does NOT replay the current token on a fresh
        // subscription (e.g. after reinstall) — read it explicitly first.
        if let current = Activity<TaskActivityAttributes>.pushToStartToken {
            lastPushToStartToken = current.hexString
            Task { await self.uploadPushTokens(pushToStartToken: current.hexString) }
        }
        Task { await self.observePushToStartToken() }
        Task { await self.observeActivities() }
        // activityUpdates only emits NEW activities; activities that already
        // exist (created while the app was suspended, or before a relaunch)
        // must be picked up explicitly.
        for activity in Activity<TaskActivityAttributes>.activities {
            Task { await self.observeActivityTokens(activity) }
        }
    }

    /// Loads credentials straight from UserDefaults — used by the background
    /// launch path (App.init), where the UI/AppModel never comes up.
    func bootstrapFromDefaults() {
        if client == nil,
           let server = UserDefaults.standard.string(forKey: "serverURL"),
           let url = URL(string: server), !server.isEmpty {
            let token = UserDefaults.standard.string(forKey: "token")
            client = APIClient(baseURL: url, token: (token?.isEmpty ?? true) ? nil : token)
        }
        startObserving()
    }

    private func observePushToStartToken() async {
        for await tokenData in Activity<TaskActivityAttributes>.pushToStartTokenUpdates {
            lastPushToStartToken = tokenData.hexString
            await uploadPushTokens(pushToStartToken: tokenData.hexString)
        }
    }

    /// Regular APNs device token, used for message notifications.
    func uploadAlertToken(_ hex: String) async {
        await uploadPushTokens(alertToken: hex)
    }

    /// `PUT /v1/devices/self/push-tokens` (v1-architecture.md §2.3): device-
    /// level tokens, no `device_id` field — the target is always the
    /// authenticated caller. This REPLACES the old `POST /v2/devices
    /// {device_id, push_to_start_token?, alert_token?}` shape; the locally
    /// generated `Self.deviceID` UUID is no longer sent (it was never this
    /// space's real paired device id in the first place — the server
    /// resolves identity from the bearer token now, closing exactly the
    /// hole §2.3 describes).
    private func uploadPushTokens(pushToStartToken: String? = nil, alertToken: String? = nil) async {
        guard let client else { return }
        for attempt in 0..<3 {
            if attempt > 0 {
                try? await Task.sleep(for: .seconds(Double(attempt) * 2))
            }
            if (try? await client.registerPushTokens(
                pushToStartToken: pushToStartToken, alertToken: alertToken)) != nil { return }
        }
    }

    private func observeActivities() async {
        for await activity in Activity<TaskActivityAttributes>.activityUpdates {
            Task { await self.observeActivityTokens(activity) }
        }
    }

    /// `PUT /v1/tasks/:id/live-activity-token` (v1-architecture.md §2.3):
    /// path carries the task id, body is `{token}` — replaces the old
    /// `POST /v2/activities {source_id, token}` body form.
    private func observeActivityTokens(_ activity: Activity<TaskActivityAttributes>) async {
        let taskID = activity.attributes.sourceId
        for await tokenData in activity.pushTokenUpdates {
            await uploadLiveActivityToken(taskID: taskID, token: tokenData.hexString)
        }
    }

    private func uploadLiveActivityToken(taskID: String, token: String) async {
        guard let client else { return }
        for attempt in 0..<3 {
            if attempt > 0 {
                try? await Task.sleep(for: .seconds(Double(attempt) * 2))
            }
            if (try? await client.registerLiveActivityToken(taskID: taskID, token: token)) != nil { return }
        }
    }
}

extension Data {
    var hexString: String {
        map { String(format: "%02x", $0) }.joined()
    }
}
