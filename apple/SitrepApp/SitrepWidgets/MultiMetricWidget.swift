import AppIntents
import SitrepKit
import SwiftUI
import WidgetKit

// The complication-board widget: the user picks WHICH metrics appear in this
// instance, so two widgets side by side can show entirely different boards.
// Small holds 2, medium 4, large 6 — a grid of uniform mini-cells.

struct MultiMetricIntent: WidgetConfigurationIntent {
    static let title: LocalizedStringResource = "选择指标组合"
    static let description = IntentDescription("勾选要在这个组件里显示的指标")

    @Parameter(title: "指标")
    var metrics: [MetricChoice]?
}

struct MultiMetricEntry: TimelineEntry {
    let date: Date
    let metrics: [MetricState]
    let configured: Bool
}

struct MultiMetricProvider: AppIntentTimelineProvider {
    func placeholder(in context: Context) -> MultiMetricEntry {
        MultiMetricEntry(
            date: .now,
            metrics: [
                MetricState(key: "aapl", value: "212.4", label: "AAPL", updatedAt: .now),
                MetricState(key: "gh", value: "1343", label: "GitHub ★", updatedAt: .now),
            ],
            configured: true
        )
    }

    func snapshot(for configuration: MultiMetricIntent, in context: Context) async -> MultiMetricEntry {
        placeholder(in: context)
    }

    func timeline(for configuration: MultiMetricIntent, in context: Context) async -> Timeline<MultiMetricEntry> {
        var entry = MultiMetricEntry(date: .now, metrics: [], configured: false)
        if let cfg = SharedConfig.load() {
            let all = (try? await APIClient(baseURL: cfg.server, token: cfg.token).snapshot().metrics.map(\.state)) ?? []
            // Chosen metrics in the chosen order; unconfigured → first few.
            let chosen: [MetricState]
            if let picks = configuration.metrics, !picks.isEmpty {
                chosen = picks.compactMap { pick in all.first { $0.key == pick.id } }
            } else {
                chosen = Array(all.sorted { $0.key < $1.key })
            }
            entry = MultiMetricEntry(date: .now, metrics: chosen, configured: true)
        }
        return Timeline(entries: [entry], policy: .after(.now.addingTimeInterval(15 * 60)))
    }
}

struct MultiMetricWidget: Widget {
    var body: some WidgetConfiguration {
        AppIntentConfiguration(
            kind: "SitrepMultiMetric",
            intent: MultiMetricIntent.self,
            provider: MultiMetricProvider()
        ) { entry in
            MultiMetricView(entry: entry)
                .containerBackground(.fill.tertiary, for: .widget)
        }
        .configurationDisplayName("指标组合")
        .description("自选多个指标拼成一块仪表盘，长按「编辑小组件」勾选。")
        .supportedFamilies([.systemSmall, .systemMedium, .systemLarge])
    }
}

struct MultiMetricView: View {
    @Environment(\.widgetFamily) private var family
    let entry: MultiMetricEntry

    private var capacity: Int {
        switch family {
        case .systemSmall: 2
        case .systemMedium: 4
        default: 6
        }
    }

    private var visible: [MetricState] { Array(entry.metrics.prefix(capacity)) }

    var body: some View {
        if !entry.configured {
            Text("打开 Sitrep 完成连接")
                .font(.caption)
                .foregroundStyle(.secondary)
        } else if visible.isEmpty {
            Text("长按编辑，勾选指标")
                .font(.caption)
                .foregroundStyle(.secondary)
        } else if family == .systemSmall {
            VStack(alignment: .leading, spacing: 8) {
                ForEach(visible) { MetricCell(metric: $0, showSpark: false) }
                Spacer(minLength: 0)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        } else {
            let columns = [GridItem(.flexible(), spacing: 12), GridItem(.flexible(), spacing: 12)]
            LazyVGrid(columns: columns, alignment: .leading, spacing: family == .systemLarge ? 14 : 10) {
                ForEach(visible) { MetricCell(metric: $0, showSpark: family == .systemLarge) }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
    }
}

/// One uniform mini-cell: identity line, tinted value, optional sparkline.
private struct MetricCell: View {
    let metric: MetricState
    let showSpark: Bool

    var body: some View {
        let tint = Presentation.tint(metric.tint)
        VStack(alignment: .leading, spacing: 2) {
            HStack(spacing: 3) {
                Presentation.icon(metric.icon, status: "running")
                    .font(.caption2)
                    .foregroundStyle(tint)
                Text(metric.label ?? metric.key)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Text(Presentation.formatValue(metric.value))
                .font(.system(.title3, design: .rounded).weight(.semibold))
                .monospacedDigit()
                .foregroundStyle(tint)
                .minimumScaleFactor(0.5)
                .lineLimit(1)
            if showSpark, (metric.history?.count ?? 0) >= 2 {
                SparklineView(metric: metric)
                    .frame(height: 22)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
