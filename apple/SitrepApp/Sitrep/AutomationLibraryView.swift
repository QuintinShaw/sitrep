import SitrepKit
import SwiftUI

/// Secondary management surface for computer-side scheduled executors.
/// The phone controls cadence and lifecycle; the installed Agent owns execution.
struct AutomationLibraryView: View {
    @Bindable var model: AppModel
    @Environment(\.dismiss) private var dismiss
    @State private var confirmDelete: AutomationInfo?
    @State private var runningID: String?
    @Environment(\.accessibilityReduceMotion) private var reduceMotion

    var body: some View {
        NavigationStack {
            List {
                if model.automations.isEmpty {
                    ContentUnavailableView {
                        Label("还没有自动化", systemImage: "bolt.badge.clock")
                    } description: {
                        Text("在电脑上让 Agent 创建定时监控，它会自动出现在这里。")
                    }
                } else {
                    Section {
                        ForEach(model.automations) { automation in
                            automationRow(automation)
                                .swipeActions(edge: .leading, allowsFullSwipe: true) {
                                    Button { run(automation) } label: {
                                        Label("运行", systemImage: "play.fill")
                                    }
                                    .tint(.blue)
                                }
                                .swipeActions(edge: .trailing, allowsFullSwipe: false) {
                                    Button(role: .destructive) { confirmDelete = automation } label: {
                                        Label("删除", systemImage: "trash")
                                    }
                                }
                        }
                    } footer: {
                        Text("自动化由这台电脑上的脚本或已登录 Agent 执行；Sitrep 只负责调度、状态和提醒。")
                    }
                }
            }
            .navigationTitle("自动化")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("完成") { dismiss() }
                }
            }
            .refreshable { await model.refresh() }
            .confirmationDialog(
                "删除“\(confirmDelete?.name ?? "")”？",
                isPresented: Binding(get: { confirmDelete != nil }, set: { if !$0 { confirmDelete = nil } }),
                titleVisibility: .visible
            ) {
                Button("删除自动化", role: .destructive) {
                    guard let automation = confirmDelete else { return }
                    Task {
                        try? await model.client?.deleteAutomation(id: automation.id)
                        await model.refresh()
                    }
                    confirmDelete = nil
                }
            }
        }
        .presentationDetents([.medium, .large])
    }

    private func automationRow(_ automation: AutomationInfo) -> some View {
        HStack(spacing: 12) {
            Image(systemName: automation.enabled ? "bolt.circle.fill" : "pause.circle")
                .font(.title2)
                .foregroundStyle(automation.enabled ? .blue : .secondary)
                .symbolEffect(.pulse, isActive: runningID == automation.id && !reduceMotion)
            VStack(alignment: .leading, spacing: 3) {
                Text(automation.name).font(.headline)
                Text(subtitle(automation)).font(.caption).foregroundStyle(.secondary)
            }
            Spacer()
            Menu {
                Button { run(automation) } label: { Label("立即运行", systemImage: "play.fill") }
                Menu("运行间隔") {
                    ForEach(intervalChoices, id: \.1) { label, seconds in
                        Button(label) { patch(automation, everyS: seconds) }
                    }
                }
                Button(automation.enabled ? "暂停" : "恢复") { patch(automation, enabled: !automation.enabled) }
                Divider()
                Button("删除", role: .destructive) { confirmDelete = automation }
            } label: {
                Image(systemName: "ellipsis")
                    .frame(width: 32, height: 32)
                    .contentShape(Rectangle())
            }
            .accessibilityLabel("管理 \(automation.name)")
        }
        .padding(.vertical, 4)
    }

    private func subtitle(_ automation: AutomationInfo) -> String {
        let interval = intervalChoices.first { $0.1 == automation.everyS }?.0 ?? "\(automation.everyS) 秒"
        guard let last = automation.lastRun else { return automation.enabled ? "每 \(interval)" : "已暂停" }
        let relative = RelativeDateTimeFormatter().localizedString(for: last, relativeTo: .now)
        return automation.enabled ? "每 \(interval) · \(relative)运行" : "已暂停 · \(relative)运行"
    }

    private func run(_ automation: AutomationInfo) {
        runningID = automation.id
        Task {
            try? await model.client?.patchAutomation(id: automation.id, runNow: true)
            try? await Task.sleep(for: .milliseconds(700))
            await model.refresh()
            runningID = nil
        }
    }

    private func patch(_ automation: AutomationInfo, everyS: Int? = nil, enabled: Bool? = nil) {
        Task {
            try? await model.client?.patchAutomation(id: automation.id, everyS: everyS, enabled: enabled)
            await model.refresh()
        }
    }
}
