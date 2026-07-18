import SitrepKit
import SwiftUI

/// Metric cards at full size — the in-app twin of the home-screen widgets.
/// Long-press a card to restyle it (cascades to every surface).
struct DashboardView: View {
    @Bindable var model: AppModel
    @State private var showSettings = false
    @State private var showAutomations = false
    @State private var showAddGuide = false

    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    private var columns: [GridItem] {
        dynamicTypeSize.isAccessibilitySize
            ? [GridItem(.flexible())]
            : [GridItem(.adaptive(minimum: 164), spacing: 12)]
    }

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(alignment: .leading, spacing: 18) {
                    if model.metrics.isEmpty {
                        emptyState
                    } else {
                        LazyVGrid(columns: columns, spacing: 12) {
                            ForEach(model.metrics) { metric in
                                NavigationLink(value: metric.key) {
                                    MetricCard(metric: metric)
                                }
                                .buttonStyle(.plain)
                                .contextMenu { styleMenu(metric) }
                            }
                        }
                    }
                }
                .padding(.horizontal)
                .animation(.spring(duration: 0.4), value: model.metrics)
            }
            .navigationTitle("指标")
            .navigationDestination(for: String.self) { key in
                MetricDetailView(model: model, key: key)
            }
            .toolbar {
                ToolbarItemGroup(placement: .topBarLeading) {
                    Button { showAddGuide = true } label: {
                        Label("添加显示", systemImage: "plus")
                    }
                    Button { showAutomations = true } label: {
                        Label("自动化", systemImage: "bolt.badge.clock")
                    }
                }
                SettingsGear(showSettings: $showSettings)
            }
            .sheet(isPresented: $showSettings) { SettingsView(model: model) }
            .sheet(isPresented: $showAutomations) { AutomationLibraryView(model: model) }
            .sheet(isPresented: $showAddGuide) { AddGuideSheet() }
            .refreshable { await model.refresh() }
        }
    }

    private var emptyState: some View {
        Card {
            VStack(alignment: .leading, spacing: 12) {
                Label("还没有指标", systemImage: "gauge.with.dots.needle.0percent")
                    .font(.headline)
                Text("让 Agent 添加一个监控——比如：")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                Text("“把我仓库的 star 数放到锁屏上”")
                    .font(.callout.italic())
                    .padding(10)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(.background.tertiary, in: RoundedRectangle(cornerRadius: 10))
            }
        }
        .padding(.top, 24)
    }

    @ViewBuilder private func styleMenu(_ metric: MetricState) -> some View {
        Menu("颜色") {
            ForEach(["blue", "purple", "green", "orange", "red", "teal"], id: \.self) { name in
                Button(name) { Task { await model.updateMetric(id: metric.key, tint: name) } }
            }
        }
        Menu("样式") {
            Button("数字") { Task { await model.updateMetric(id: metric.key, template: "number") } }
            Button("仪表盘") { Task { await model.updateMetric(id: metric.key, template: "gauge") } }
            Button("走势图") { Task { await model.updateMetric(id: metric.key, template: "spark") } }
        }
    }

}

/// The phone never defines what runs — it hands the user words to say to
/// the Agent on their computer. Each row copies a ready-made prompt.
struct AddGuideSheet: View {
    @Environment(\.dismiss) private var dismiss
    @State private var copied: String?

    private let recipes: [(String, String, String)] = [
        ("chart.line.uptrend.xyaxis", "盯一个数字",
         "把 <某个数字，如我仓库的 star 数> 显示到我手机的桌面小组件上，每 15 分钟更新"),
        ("bell.badge", "越线提醒我",
         "帮我盯着 <某个数字>，低于 <数值> 就提醒我，提醒线做成我手机上可以调的参数"),
        ("sparkles", "盯一件事的发生",
         "当 <某件事发生，如某软件发新版> 时通知我手机"),
    ]

    var body: some View {
        NavigationStack {
            List {
                Section {
                    ForEach(recipes, id: \.1) { icon, title, prompt in
                        Button {
                            UIPasteboard.general.string = prompt
                            withAnimation { copied = title }
                        } label: {
                            HStack(spacing: 12) {
                                Image(systemName: icon)
                                    .font(.title3)
                                    .foregroundStyle(.blue)
                                    .frame(width: 28)
                                VStack(alignment: .leading, spacing: 3) {
                                    Text(title)
                                        .font(.callout.weight(.medium))
                                        .foregroundStyle(.primary)
                                    Text("“\(prompt)”")
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                }
                                Spacer()
                                Image(systemName: copied == title ? "checkmark.circle.fill" : "doc.on.doc")
                                    .foregroundStyle(copied == title ? AnyShapeStyle(.green) : AnyShapeStyle(.tertiary))
                            }
                        }
                    }
                } header: {
                    Text("在电脑上对你的 Agent 说")
                } footer: {
                    Text("复制后发给电脑上的 Claude（或任何装了 sitrep skill 的 Agent），它会写好脚本并接入这里。手机只负责看和调，不定义任务。")
                }
            }
            .navigationTitle("添加显示")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("完成") { dismiss() }
                }
            }
        }
        .presentationDetents([.medium, .large])
    }
}

let intervalChoices: [(String, Int)] = [
    ("1 分钟", 60), ("5 分钟", 300), ("15 分钟", 900),
    ("30 分钟", 1800), ("1 小时", 3600), ("12 小时", 43200),
]

/// One metric, Health-style: every card shares the same geometry — header,
/// tinted value, then a fixed-height trend strip. The template only changes
/// what fills the strip, never the card's shape, so the grid stays even.
struct MetricCard: View {
    let metric: MetricState
    @Environment(\.accessibilityReduceMotion) private var reduceMotion

    private var tint: Color { Presentation.tint(metric.tint) }

    var body: some View {
        Card {
            VStack(alignment: .leading, spacing: 6) {
                HStack(spacing: 5) {
                    Presentation.icon(metric.icon, status: "running")
                        .font(.caption)
                        .foregroundStyle(tint)
                    Text(metric.label ?? metric.key)
                        .font(.caption.weight(.medium))
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                    Spacer(minLength: 0)
                    Image(systemName: "chevron.right")
                        .font(.caption2.weight(.semibold))
                        .foregroundStyle(.quaternary)
                }
                HStack(alignment: .lastTextBaseline, spacing: 4) {
                    Text(Presentation.formatValue(metric.value))
                        .font(.system(.title, design: .rounded).weight(.semibold))
                        .monospacedDigit()
                        .foregroundStyle(tint)
                        .minimumScaleFactor(0.5)
                        .lineLimit(1)
                        .contentTransition(.numericText())
                        .animation(reduceMotion ? nil : .spring(duration: 0.4), value: metric.value)
                    Spacer(minLength: 0)
                    if let delta {
                        Label(delta.text, systemImage: delta.symbol)
                            .font(.caption2.monospacedDigit().weight(.semibold))
                            .foregroundStyle(.secondary)
                            .labelStyle(.titleAndIcon)
                    }
                }
                trendStrip
                    .frame(height: 30)
            }
        }
    }

    /// Change across the visible history window — Health's "since last" cue.
    private var delta: (text: String, symbol: String)? {
        guard let h = metric.history, h.count >= 2, let first = h.first, let last = h.last,
              first != last else { return nil }
        let diff = last - first
        let text = abs(diff).formatted(.number.precision(.fractionLength(0...1)))
        return (text, diff > 0 ? "arrow.up.right" : "arrow.down.right")
    }

    @ViewBuilder private var trendStrip: some View {
        if metric.template == "gauge" || metric.template == "bar", let g = metric.gauge {
            VStack(alignment: .leading, spacing: 3) {
                Gauge(value: g.value, in: g.range) { EmptyView() }
                    .gaugeStyle(.accessoryLinearCapacity)
                    .tint(tint)
                HStack {
                    Text(g.range.lowerBound.formatted(.number.precision(.fractionLength(0))))
                    Spacer()
                    Text(g.range.upperBound.formatted(.number.precision(.fractionLength(0))))
                }
                .font(.caption2.monospacedDigit())
                .foregroundStyle(.tertiary)
            }
            .frame(maxHeight: .infinity, alignment: .center)
        } else if let h = metric.history, h.count >= 2 {
            SparklineView(metric: metric)
        } else {
            Text(metric.updatedAt, format: .relative(presentation: .named))
                .font(.caption2)
                .foregroundStyle(.tertiary)
                .frame(maxHeight: .infinity, alignment: .bottom)
        }
    }
}
