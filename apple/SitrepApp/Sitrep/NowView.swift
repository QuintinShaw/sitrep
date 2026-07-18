import SitrepKit
import SwiftUI

/// The immediate surface: bounded work that is running or recently finished.
struct NowView: View {
    @Bindable var model: AppModel
    @State private var showSettings = false
    @State private var showAutomations = false
    @State private var confirmStop: TaskState?

    private var running: [TaskState] { model.tasks.filter { $0.status == .running } }
    private var finished: [TaskState] { model.tasks.filter { $0.status != .running } }

    var body: some View {
        NavigationStack {
            List {
                Section {
                    PresenceRow(presence: model.presence, lastSyncAt: model.lastSyncAt)
                }

                if running.isEmpty && finished.isEmpty {
                    ContentUnavailableView {
                        Label("一切安静", systemImage: "moon.stars")
                    } description: {
                        Text("电脑上的 Agent 开始任务后，进度会出现在这里和灵动岛。")
                    }
                }

                if !running.isEmpty {
                    Section("正在进行") {
                        ForEach(running) { task in
                            NavigationLink(value: task.sourceID) {
                                RunningTaskRow(task: task)
                            }
                            .swipeActions(edge: .leading, allowsFullSwipe: true) {
                                Button {
                                    Task { await model.send(isPaused(task) ? .resume : .pause, to: task) }
                                } label: {
                                    Label(isPaused(task) ? "继续" : "暂停", systemImage: isPaused(task) ? "play.fill" : "pause.fill")
                                }
                                .tint(.blue)
                            }
                            .swipeActions(edge: .trailing, allowsFullSwipe: false) {
                                Button(role: .destructive) { confirmStop = task } label: {
                                    Label("停止", systemImage: "stop.fill")
                                }
                            }
                            .contextMenu { taskMenu(task) }
                        }
                    }
                }

                if !finished.isEmpty {
                    Section("最近") {
                        ForEach(finished.prefix(8)) { task in
                            NavigationLink(value: task.sourceID) {
                                FinishedRow(task: task)
                            }
                        }
                    }
                }
            }
            .listStyle(.insetGrouped)
            .navigationTitle("现在")
            .navigationDestination(for: String.self) { sourceID in
                TaskDetailView(model: model, sourceID: sourceID)
            }
            .navigationDestination(
                isPresented: Binding(
                    get: { model.deepLinkTask != nil },
                    set: { if !$0 { model.deepLinkTask = nil } }
                )
            ) {
                TaskDetailView(model: model, sourceID: model.deepLinkTask ?? "")
            }
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button { showAutomations = true } label: {
                        Label("自动化", systemImage: "bolt.badge.clock")
                    }
                }
                SettingsGear(showSettings: $showSettings)
            }
            .sheet(isPresented: $showSettings) { SettingsView(model: model) }
            .sheet(isPresented: $showAutomations) { AutomationLibraryView(model: model) }
            .refreshable { await model.refresh() }
            .confirmationDialog(
                "停止“\(confirmStop?.title ?? "")”？",
                isPresented: Binding(get: { confirmStop != nil }, set: { if !$0 { confirmStop = nil } }),
                titleVisibility: .visible
            ) {
                Button("停止任务", role: .destructive) {
                    if let task = confirmStop { Task { await model.send(.stop, to: task) } }
                    confirmStop = nil
                }
            } message: {
                Text("电脑上的进程将被终止。")
            }
        }
    }

    private func isPaused(_ task: TaskState) -> Bool { task.step?.hasPrefix("⏸") == true }

    @ViewBuilder private func taskMenu(_ task: TaskState) -> some View {
        Button {
            Task { await model.send(isPaused(task) ? .resume : .pause, to: task) }
        } label: {
            Label(isPaused(task) ? "继续" : "暂停", systemImage: isPaused(task) ? "play.fill" : "pause.fill")
        }
        Button(role: .destructive) { confirmStop = task } label: {
            Label("停止", systemImage: "stop.fill")
        }
    }

}

private struct PresenceRow: View {
    let presence: PresenceInfo?
    let lastSyncAt: Date?

    private var lastHeard: Date? {
        [presence?.agentLastSeen, presence?.ingestLastSeen].compactMap(\.self).max()
    }

    private var state: (Color, String, String) {
        guard let lastHeard else { return (.gray, "电脑未连接", "等待电脑首次上报") }
        let age = Date.now.timeIntervalSince(lastHeard)
        if age < 60 { return (.green, "电脑在线", "状态已同步") }
        if age < 600 { return (.orange, "连接不稳定", "\(Int(age / 60)) 分钟前在线") }
        return (.red, "电脑已离线", lastHeard.formatted(.relative(presentation: .named)))
    }

    var body: some View {
        let (color, title, detail) = state
        LabeledContent {
            Text(detail).foregroundStyle(.secondary)
        } label: {
            Label {
                Text(title)
            } icon: {
                Circle().fill(color).frame(width: 9, height: 9)
            }
        }
        .font(.subheadline)
        .accessibilityElement(children: .combine)
    }
}

private struct RunningTaskRow: View {
    let task: TaskState
    private var tint: Color { Presentation.statusTint(task.status.rawValue, hint: task.tint) }

    var body: some View {
        HStack(spacing: 14) {
            ZStack {
                Circle().stroke(tint.opacity(0.18), lineWidth: 5)
                Circle()
                    .trim(from: 0, to: Double(task.percent ?? 0) / 100)
                    .stroke(tint, style: StrokeStyle(lineWidth: 5, lineCap: .round))
                    .rotationEffect(.degrees(-90))
                if let percent = task.percent {
                    Text("\(percent)").font(.caption.monospacedDigit().bold())
                } else {
                    Presentation.icon(task.icon, status: task.status.rawValue).foregroundStyle(tint)
                }
            }
            .frame(width: 48, height: 48)

            VStack(alignment: .leading, spacing: 3) {
                Text(task.title).font(.headline).lineLimit(1)
                if let step = task.step {
                    Text(step).font(.subheadline).foregroundStyle(.secondary).lineLimit(2)
                }
            }
        }
        .padding(.vertical, 4)
        .accessibilityElement(children: .combine)
        .accessibilityValue(task.percent.map { "完成 \($0)%" } ?? "正在运行")
    }
}

struct FinishedRow: View {
    let task: TaskState

    var body: some View {
        HStack(spacing: 12) {
            Image(systemName: task.status == .done ? "checkmark.circle.fill" : "xmark.circle.fill")
                .foregroundStyle(task.status == .done ? .green : .red)
            VStack(alignment: .leading, spacing: 2) {
                Text(task.title).lineLimit(1)
                if let step = task.step {
                    Text(step).font(.caption).foregroundStyle(.secondary).lineLimit(1)
                }
            }
            Spacer()
            Text(task.updatedAt, style: .relative).font(.caption).foregroundStyle(.tertiary)
        }
        .padding(.vertical, 2)
        .accessibilityElement(children: .combine)
    }
}
