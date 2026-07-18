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
                // Honesty first: a monitoring tool must SAY when it can't see.
                if let err = model.lastError {
                    ErrorBanner(text: err)
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
        // Coming back to foreground: refresh immediately and revive the
        // poll loop if anything killed it while suspended.
        .onChange(of: scenePhase) { _, phase in
            if phase == .active {
                model.ensurePolling()
                Task { await model.refresh() }
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
