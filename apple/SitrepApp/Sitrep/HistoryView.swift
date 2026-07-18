import SitrepKit
import SwiftUI

struct HistoryView: View {
    @Bindable var model: AppModel
    @State private var searchText = ""
    @State private var showSettings = false
    @State private var showAutomations = false
    @State private var confirmClear = false

    struct EventGroup: Identifiable, Hashable {
        let name: String
        let entries: [EventLogEntry]
        var id: String { name }

        static func == (lhs: Self, rhs: Self) -> Bool { lhs.id == rhs.id }
        func hash(into hasher: inout Hasher) { hasher.combine(id) }
    }

    private func groupName(_ source: String?) -> String {
        guard let source else { return "其他" }
        if let automation = model.automations.first(where: { "a\($0.id)" == source }) { return automation.name }
        if let task = model.tasks.first(where: { $0.sourceID == source }) { return task.title }
        return source
    }

    private var filtered: [EventLogEntry] {
        guard !searchText.isEmpty else { return model.events }
        return model.events.filter {
            $0.text.localizedCaseInsensitiveContains(searchText)
                || groupName($0.source).localizedCaseInsensitiveContains(searchText)
        }
    }

    private var groups: [EventGroup] {
        Dictionary(grouping: filtered) { groupName($0.source) }
            .map { EventGroup(name: $0.key, entries: $0.value.sorted { $0.ts > $1.ts }) }
            .sorted { ($0.entries.first?.ts ?? .distantPast) > ($1.entries.first?.ts ?? .distantPast) }
    }

    var body: some View {
        NavigationStack {
            List {
                if model.events.isEmpty {
                    ContentUnavailableView {
                        Label("还没有消息", systemImage: "tray")
                    } description: {
                        Text("任务完成、条件达成和 Agent 提醒会保存在这里。")
                    }
                } else if groups.isEmpty {
                    ContentUnavailableView.search(text: searchText)
                } else {
                    ForEach(groups) { group in
                        NavigationLink(value: group) {
                            MessageGroupRow(group: group)
                        }
                    }
                }
            }
            .navigationTitle("消息")
            .navigationDestination(for: EventGroup.self) { group in
                MessageGroupView(model: model, group: group)
            }
            .searchable(text: $searchText, prompt: "搜索消息或来源")
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button { showAutomations = true } label: {
                        Label("自动化", systemImage: "bolt.badge.clock")
                    }
                }
                ToolbarItemGroup(placement: .topBarTrailing) {
                    if !model.events.isEmpty {
                        Button { confirmClear = true } label: {
                            Label("清空消息", systemImage: "trash")
                        }
                    }
                    Button { showSettings = true } label: {
                        Label("设置", systemImage: "gearshape")
                    }
                }
            }
            .sheet(isPresented: $showSettings) { SettingsView(model: model) }
            .sheet(isPresented: $showAutomations) { AutomationLibraryView(model: model) }
            .refreshable { await model.refresh() }
            .confirmationDialog("清空全部消息？", isPresented: $confirmClear, titleVisibility: .visible) {
                Button("清空", role: .destructive) {
                    Task {
                        try? await model.client?.clearEvents()
                        await model.refresh()
                    }
                }
            } message: {
                Text("所有设备上的消息都会被清空，此操作无法撤销。")
            }
        }
    }
}

private struct MessageGroupRow: View {
    let group: HistoryView.EventGroup
    private var latest: EventLogEntry? { group.entries.first }

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: icon)
                .font(.headline)
                .foregroundStyle(tint)
                .frame(width: 34, height: 34)
                .background(tint.opacity(0.12), in: Circle())
            VStack(alignment: .leading, spacing: 4) {
                HStack {
                    Text(group.name).font(.headline).lineLimit(1)
                    Spacer()
                    if let latest {
                        Text(latest.ts, style: .relative).font(.caption).foregroundStyle(.secondary)
                    }
                }
                if let latest {
                    Text(latest.text).font(.subheadline).foregroundStyle(.secondary).lineLimit(2)
                }
            }
            if group.entries.count > 1 {
                Text("\(group.entries.count)")
                    .font(.caption2.monospacedDigit().bold())
                    .foregroundStyle(.secondary)
                    .padding(.horizontal, 7).padding(.vertical, 3)
                    .background(.fill.tertiary, in: Capsule())
            }
        }
        .padding(.vertical, 5)
        .accessibilityElement(children: .combine)
    }

    private var tint: Color {
        switch latest?.level { case "error": .red; case "warn": .orange; default: .blue }
    }
    private var icon: String {
        switch latest?.level { case "error": "exclamationmark.triangle.fill"; case "warn": "bell.badge.fill"; default: "bell.fill" }
    }
}

private struct MessageGroupView: View {
    @Bindable var model: AppModel
    let group: HistoryView.EventGroup
    @State private var entries: [EventLogEntry]

    init(model: AppModel, group: HistoryView.EventGroup) {
        self.model = model
        self.group = group
        _entries = State(initialValue: group.entries)
    }

    var body: some View {
        List {
            ForEach(entries) { event in
                VStack(alignment: .leading, spacing: 6) {
                    Text(event.text)
                    Text(event.ts.formatted(date: .abbreviated, time: .shortened))
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .padding(.vertical, 4)
                .swipeActions {
                    if let id = event.serverID {
                        Button("删除", role: .destructive) { delete([id]) }
                    }
                }
                .contextMenu {
                    Button { UIPasteboard.general.string = event.text } label: {
                        Label("复制", systemImage: "doc.on.doc")
                    }
                    if let id = event.serverID {
                        Button(role: .destructive) { delete([id]) } label: {
                            Label("删除", systemImage: "trash")
                        }
                    }
                }
            }
        }
        .navigationTitle(group.name)
        .navigationBarTitleDisplayMode(.inline)
    }

    private func delete(_ ids: [String]) {
        entries.removeAll { $0.serverID.map(ids.contains) == true }
        Task {
            try? await model.client?.deleteEvents(ids: ids)
            await model.refresh()
        }
    }
}
