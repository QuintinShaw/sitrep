import SwiftUI
import SitrepKit

struct StatusPanel: View {
    var model: StatusModel
    @State private var showInvite = false

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            if !model.configured {
                ContentUnavailableView(
                    "Not configured",
                    systemImage: "gear.badge.questionmark",
                    description: Text("Set SITREP_SERVER / SITREP_TOKEN or create ~/.config/sitrep/config.json")
                )
            } else {
                if model.tasks.isEmpty && model.metrics.isEmpty {
                    Text("No tasks yet — run one with `sitrep run -- <cmd>`")
                        .foregroundStyle(.secondary)
                        .padding(.vertical, 8)
                }
                ForEach(model.tasks) { task in
                    TaskRow(task: task)
                }
                if !model.metrics.isEmpty {
                    Divider()
                    ForEach(model.metrics) { metric in
                        MetricRow(model: model, metric: metric)
                    }
                }
                if !model.automations.isEmpty {
                    Divider()
                    Text("自动化").font(.caption2).foregroundStyle(.tertiary)
                    ForEach(model.automations) { automation in
                        AutomationRow(model: model, automation: automation)
                    }
                }
                if let err = model.lastError {
                    Divider()
                    Label(err, systemImage: "wifi.exclamationmark")
                        .font(.caption)
                        .foregroundStyle(.orange)
                }
            }
            Divider()
            HStack {
                if let client = model.client {
                    Button("添加设备") { showInvite = true }
                        .controlSize(.small)
                        .popover(isPresented: $showInvite) {
                            InviteQRView(client: client, server: model.serverURL)
                        }
                }
                Spacer()
                LaunchAtLoginToggle()
                Button("退出") { NSApp.terminate(nil) }
                    .controlSize(.small)
            }
        }
        .padding(12)
        .frame(width: 300)
    }
}

struct TaskRow: View {
    let task: TaskState

    private var tint: Color {
        Presentation.statusTint(task.status.rawValue, hint: task.tint)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 3) {
            HStack {
                Presentation.icon(task.icon, status: task.status.rawValue)
                    .foregroundStyle(tint)
                Text(task.title).font(.callout).bold().lineLimit(1)
                Spacer()
                if task.template == "timer", task.status == .running, let started = task.startedAt {
                    Text(started, style: .timer)
                        .font(.caption.monospacedDigit())
                        .foregroundStyle(tint)
                } else if let percent = task.percent {
                    Text("\(percent)%")
                        .font(.caption.monospacedDigit())
                        .foregroundStyle(.secondary)
                }
            }
            if task.status == .running && task.template != "timer" && task.template != "plain" {
                ProgressView(value: Double(task.percent ?? 0), total: 100)
                    .controlSize(.small)
                    .tint(tint)
            }
            if let step = task.step {
                Text(step).font(.caption).foregroundStyle(.secondary).lineLimit(1)
            }
        }
        .padding(.vertical, 2)
    }
}

struct AutomationRow: View {
    var model: StatusModel
    let automation: AutomationInfo

    private static let intervals: [(String, Int)] = [
        ("1 分钟", 60), ("5 分钟", 300), ("15 分钟", 900),
        ("30 分钟", 1800), ("1 小时", 3600), ("12 小时", 43200),
    ]

    var body: some View {
        HStack {
            Image(systemName: automation.enabled ? "clock" : "pause.circle")
                .font(.caption)
                .foregroundStyle(automation.enabled ? Color.blue : .secondary)
            Text(automation.name).font(.caption).lineLimit(1)
            Spacer()
            Menu(intervalLabel) {
                ForEach(Self.intervals, id: \.1) { label, seconds in
                    Button(label) { Task { await model.setAutomation(automation, everyS: seconds) } }
                }
                Divider()
                Button(automation.enabled ? "暂停" : "恢复") {
                    Task { await model.setAutomation(automation, enabled: !automation.enabled) }
                }
                Button("删除", role: .destructive) {
                    Task { await model.deleteAutomation(automation) }
                }
            }
            .menuStyle(.borderlessButton)
            .fixedSize()
            .font(.caption)
        }
    }

    private var intervalLabel: String {
        Self.intervals.first { $0.1 == automation.everyS }?.0
            ?? "\(automation.everyS)s"
    }
}

struct MetricRow: View {
    var model: StatusModel
    let metric: MetricState

    private static let tints = ["blue", "purple", "green", "orange", "red", "teal"]
    private static let templates: [(String, String)] = [
        ("数字", "number"), ("仪表盘", "gauge"), ("进度条", "bar"), ("走势图", "spark"),
    ]

    var body: some View {
        HStack {
            if metric.icon != nil {
                Presentation.icon(metric.icon, status: "running")
                    .font(.caption)
                    .foregroundStyle(Presentation.tint(metric.tint))
            }
            Text(metric.label ?? metric.key).font(.caption)
            Spacer()
            Text(metric.value)
                .font(.caption.monospacedDigit())
                .bold()
        }
        .contentShape(Rectangle())
        .contextMenu {
            Menu("颜色") {
                ForEach(Self.tints, id: \.self) { name in
                    Button(name) { Task { await model.setStyle(metric: metric, tint: name) } }
                }
            }
            Menu("小组件样式") {
                ForEach(Self.templates, id: \.1) { label, template in
                    Button(label) { Task { await model.setStyle(metric: metric, template: template) } }
                }
            }
        }
    }
}
