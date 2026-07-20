import SitrepKit
import SwiftUI

/// The app's information architecture: three glanceable surfaces, in order
/// of immediacy. Management lives behind the gear, not in the flow.
extension Notification.Name {
    static let sitrepOpenEvents = Notification.Name("sitrepOpenEvents")
}

struct MainTabView: View {
    @Bindable var model: AppModel
    @State private var selection = 0
    @Environment(\.scenePhase) private var scenePhase

    var body: some View {
        configuredTabs
            .safeAreaInset(edge: .top, spacing: 0) {
                VStack(spacing: 0) {
                    // Honesty first: a monitoring tool must SAY when it can't see.
                    if let err = model.lastError {
                        ErrorBanner(text: err)
                    } else if let status = SyncStatusStrip.label(for: model.connectionPhase) {
                        SyncStatusStrip(color: status.color, text: status.text)
                    }
                    if model.supersededNotice {
                        SupersededBanner(isPresented: $model.supersededNotice)
                    }
                }
            }
    }

    @ViewBuilder private var tabs: some View {
        let content = TabView(selection: $selection) {
            NowView(model: model)
                .tabItem { Label("现在", systemImage: "dot.radiowaves.left.and.right") }
                .tag(0)
            DashboardView(model: model)
                .tabItem { Label("指标", systemImage: "chart.xyaxis.line") }
                .tag(1)
            HistoryView(model: model)
                .tabItem { Label("消息", systemImage: "tray.full") }
                .tag(2)
        }
        if #available(iOS 26.0, *) {
            content.tabBarMinimizeBehavior(.onScrollDown)
        } else {
            content
        }
    }

    private var configuredTabs: some View {
        tabs
        .fullScreenCover(isPresented: $model.needsOnboarding) {
            JoinView(model: model)
        }
        // sitrep://task/<id> from the Dynamic Island / lock screen card;
        // notifications route to the events timeline.
        .onOpenURL { url in
            guard url.scheme == "sitrep" else { return }
            switch url.host {
            case "task":
                selection = 0
                model.deepLinkTask = url.lastPathComponent
            case "events":
                selection = 2
            case "join":
                model.pendingJoinPayload = url.absoluteString
                model.needsOnboarding = true
            default:
                break
            }
        }
        .onReceive(NotificationCenter.default.publisher(for: .sitrepOpenEvents)) { _ in
            selection = 2
        }
        // Coming back to foreground: refresh immediately, (re)connect the
        // realtime channel, and revive the HTTP fallback poll if anything
        // killed it while suspended. While the WebSocket is live, refresh()
        // only updates REST-only state (presence) — the reliable collections
        // stay owned by deltas. Leaving the foreground closes the realtime
        // connection — the interest lease simply lapses server-side rather
        // than being explicitly released.
        .onChange(of: scenePhase) { _, phase in
            switch phase {
            case .active:
                model.ensurePolling()
                Task { await model.refresh() }
            case .background:
                model.leaveBackground()
            default:
                break
            }
        }
    }
}

// MARK: - shared chrome

struct ErrorBanner: View {
    let text: String

    var body: some View {
        Label("连接不上服务器 · \(text)", systemImage: "wifi.exclamationmark")
            .font(.caption.weight(.medium))
            .lineLimit(1)
            .frame(maxWidth: .infinity)
            .padding(.vertical, 7)
            .background(.orange.opacity(0.92))
            .foregroundStyle(.white)
            .transition(.move(edge: .top).combined(with: .opacity))
    }
}

/// A subtle, transient strip about THIS PHONE's realtime connection to the
/// server — distinct from `PresencePill`/`PresenceRow`, which report
/// whether the Mac/Agent is reporting in. Reuses the same dot-plus-caption
/// language rather than a new visual system; shows nothing once the
/// connection is `.live` (the common case), so it stays out of the way.
struct SyncStatusStrip: View {
    let color: Color
    let text: String

    /// nil means "healthy, show nothing" — only `.live` and `.idle` (not yet
    /// started) count as nothing-to-say; every other phase is transient and
    /// worth a one-line, low-emphasis mention.
    static func label(for phase: RealtimeClient.Phase) -> (color: Color, text: String)? {
        switch phase {
        case .idle, .live: nil
        case .connecting, .handshaking: (.gray, "正在连接实时同步…")
        case .subscribed: (.gray, "正在同步…")
        case .failed: (.orange, "实时同步中断，已切换到低频刷新")
        }
    }

    var body: some View {
        HStack(spacing: 6) {
            Circle().fill(color).frame(width: 6, height: 6)
            Text(text).font(.caption2.weight(.medium)).foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity)
        .padding(.vertical, 4)
        .background(.background.secondary)
        .transition(.move(edge: .top).combined(with: .opacity))
        .animation(.default, value: text)
    }
}

/// One-shot, non-disruptive notice for SPEC.md §9.4 `superseded`: this
/// device's realtime connection was replaced by another connection using
/// the same credential — most often the same phone's own connection racing
/// itself across a fast background/foreground cycle, but possibly a sign
/// the credential is in use elsewhere. Tap to dismiss; not repeated.
struct SupersededBanner: View {
    @Binding var isPresented: Bool

    var body: some View {
        Button {
            withAnimation { isPresented = false }
        } label: {
            Label("这台设备的实时连接被另一个连接取代，可能是该凭据在别处也在使用", systemImage: "info.circle")
                .font(.caption.weight(.medium))
                .lineLimit(2)
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(.horizontal, 12)
                .padding(.vertical, 7)
        }
        .buttonStyle(.plain)
        .background(.blue.opacity(0.12))
        .foregroundStyle(.blue)
        .transition(.move(edge: .top).combined(with: .opacity))
        .task {
            try? await Task.sleep(for: .seconds(6))
            withAnimation { isPresented = false }
        }
    }
}

/// "Is my Mac alive" — the first question a monitoring tool must answer.
/// Green: agent heartbeat or ingest within a minute. Orange: minutes quiet.
/// Red: nothing for 10+ minutes.
struct PresencePill: View {
    let presence: PresenceInfo?

    private var lastHeard: Date? {
        [presence?.agentLastSeen, presence?.ingestLastSeen].compactMap(\.self).max()
    }

    private var state: (Color, String) {
        guard let lastHeard else { return (.gray, "电脑未连接") }
        let age = Date.now.timeIntervalSince(lastHeard)
        if age < 60 { return (.green, "电脑在线") }
        if age < 600 {
            return (.orange, "电脑 \(Int(age / 60)) 分钟前在线")
        }
        return (.red, "电脑已离线")
    }

    var body: some View {
        let (color, text) = state
        HStack(spacing: 6) {
            Circle()
                .fill(color)
                .frame(width: 8, height: 8)
                .overlay {
                    if color == .green {
                        Circle().stroke(color.opacity(0.4), lineWidth: 3)
                    }
                }
            Text(text)
                .font(.caption.weight(.medium))
                .foregroundStyle(.secondary)
            if let lastHeard, color != .green {
                Text(lastHeard, style: .relative)
                    .font(.caption2)
                    .foregroundStyle(.tertiary)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 6)
        .background(.background.secondary, in: Capsule())
    }
}

struct SettingsGear: ToolbarContent {
    @Binding var showSettings: Bool

    var body: some ToolbarContent {
        ToolbarItem(placement: .topBarTrailing) {
            Button {
                showSettings = true
            } label: {
                Image(systemName: "gearshape")
            }
        }
    }
}

/// Card container: the visual unit of the whole app.
struct Card<Content: View>: View {
    @ViewBuilder var content: Content

    var body: some View {
        content
            .padding(16)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(.background.secondary, in: RoundedRectangle(cornerRadius: 20))
    }
}
