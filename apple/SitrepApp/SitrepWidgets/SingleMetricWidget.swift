import AppIntents
import SitrepKit
import SwiftUI
import WidgetKit

// User-configurable widget: long-press → Edit to pick WHICH metric and HOW
// to draw it. The computer's hints are the default ("auto"); the person
// holding the phone has the final say — and can place any number of
// instances, each configured differently.

enum WidgetStyle: String, AppEnum {
    case auto, number, gauge, bar, spark

    static let typeDisplayRepresentation: TypeDisplayRepresentation = "样式"
    static let caseDisplayRepresentations: [WidgetStyle: DisplayRepresentation] = [
        .auto: "自动（跟随电脑建议）",
        .number: "数字",
        .gauge: "仪表盘",
        .bar: "进度条",
        .spark: "走势图",
    ]
}

struct MetricChoice: AppEntity {
    var id: String
    var label: String

    static let typeDisplayRepresentation: TypeDisplayRepresentation = "Metric"
    static let defaultQuery = MetricChoiceQuery()
    var displayRepresentation: DisplayRepresentation {
        DisplayRepresentation(title: "\(label)")
    }
}

struct MetricChoiceQuery: EntityQuery {
    func entities(for identifiers: [String]) async throws -> [MetricChoice] {
        try await suggestedEntities().filter { identifiers.contains($0.id) }
    }

    func suggestedEntities() async throws -> [MetricChoice] {
        guard let cfg = SharedConfig.load() else { return [] }
        let metrics = (try? await APIClient(baseURL: cfg.server, token: cfg.token).snapshot().metrics.map(\.state)) ?? []
        return metrics.map { MetricChoice(id: $0.key, label: $0.label ?? $0.key) }
    }

    func defaultResult() async -> MetricChoice? {
        try? await suggestedEntities().first
    }
}

struct SingleMetricIntent: WidgetConfigurationIntent {
    static let title: LocalizedStringResource = "选择指标"
    static let description = IntentDescription("挑一个指标和显示样式")

    @Parameter(title: "指标")
    var metric: MetricChoice?

    @Parameter(title: "样式", default: .auto)
    var style: WidgetStyle
}

struct SingleMetricEntry: TimelineEntry {
    let date: Date
    let metric: MetricState?
    let style: WidgetStyle
}

struct SingleMetricProvider: AppIntentTimelineProvider {
    func placeholder(in context: Context) -> SingleMetricEntry {
        SingleMetricEntry(
            date: .now,
            metric: MetricState(key: "aapl", value: "343", label: "AAPL", updatedAt: .now),
            style: .gauge
        )
    }

    func snapshot(for configuration: SingleMetricIntent, in context: Context) async -> SingleMetricEntry {
        placeholder(in: context)
    }

    func timeline(for configuration: SingleMetricIntent, in context: Context) async -> Timeline<SingleMetricEntry> {
        var metric: MetricState?
        if let cfg = SharedConfig.load() {
            let all = (try? await APIClient(baseURL: cfg.server, token: cfg.token).snapshot().metrics.map(\.state)) ?? []
            metric = all.first { $0.key == configuration.metric?.id } ?? all.first
        }
        let entry = SingleMetricEntry(date: .now, metric: metric, style: configuration.style)
        return Timeline(entries: [entry], policy: .after(.now.addingTimeInterval(15 * 60)))
    }
}

struct SingleMetricWidget: Widget {
    var body: some WidgetConfiguration {
        AppIntentConfiguration(
            kind: "SitrepSingleMetric",
            intent: SingleMetricIntent.self,
            provider: SingleMetricProvider()
        ) { entry in
            SingleMetricView(entry: entry)
                .containerBackground(.fill.tertiary, for: .widget)
        }
        .configurationDisplayName("单个指标")
        .description("一个指标一个组件，长按「编辑小组件」选择内容与样式。")
        .supportedFamilies([
            .systemSmall, .systemMedium, .systemLarge,
            .accessoryCircular, .accessoryRectangular, .accessoryInline,
        ])
    }
}

struct SingleMetricView: View {
    @Environment(\.widgetFamily) private var family
    let entry: SingleMetricEntry

    private var effectiveStyle: WidgetStyle {
        if entry.style != .auto { return entry.style }
        switch entry.metric?.template {
        case "gauge": return .gauge
        case "bar": return .bar
        case "spark": return .spark
        default: return .number
        }
    }

    var body: some View {
        if let m = entry.metric {
            switch family {
            case .accessoryInline:
                Text("\(m.label ?? m.key) \(Presentation.formatValue(m.value))")
            case .accessoryCircular:
                MetricGaugeView(metric: m)
            case .systemMedium:
                mediumBody(m)
            case .systemLarge:
                largeBody(m)
            default:
                content(m)
            }
        } else {
            Text("打开 Sitrep 完成连接")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    /// Medium: identity + value on the left, trend filling the right half.
    private func mediumBody(_ m: MetricState) -> some View {
        let tint = Presentation.tint(m.tint)
        return HStack(spacing: 14) {
            VStack(alignment: .leading, spacing: 4) {
                HStack(spacing: 4) {
                    Presentation.icon(m.icon, status: "running")
                        .font(.caption).foregroundStyle(tint)
                    Text(m.label ?? m.key)
                        .font(.caption).foregroundStyle(.secondary).lineLimit(1)
                }
                Spacer(minLength: 0)
                Text(Presentation.formatValue(m.value))
                    .font(.system(.largeTitle, design: .rounded).weight(.semibold))
                    .monospacedDigit()
                    .foregroundStyle(tint)
                    .minimumScaleFactor(0.4)
                    .lineLimit(1)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            trendPane(m, tint: tint)
                .frame(maxWidth: .infinity)
        }
    }

    /// Large: a full dashboard tile — value, big trend, then min/avg/max.
    private func largeBody(_ m: MetricState) -> some View {
        let tint = Presentation.tint(m.tint)
        let history = m.history ?? []
        return VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 5) {
                Presentation.icon(m.icon, status: "running")
                    .font(.callout).foregroundStyle(tint)
                Text(m.label ?? m.key)
                    .font(.callout.weight(.medium)).foregroundStyle(.secondary).lineLimit(1)
                Spacer()
            }
            HStack(alignment: .lastTextBaseline, spacing: 6) {
                Text(Presentation.formatValue(m.value))
                    .font(.system(size: 42, weight: .semibold, design: .rounded))
                    .monospacedDigit()
                    .foregroundStyle(tint)
                    .minimumScaleFactor(0.4)
                    .lineLimit(1)
            }
            trendPane(m, tint: tint)
                .frame(maxHeight: .infinity)
            if history.count >= 2, let lo = history.min(), let hi = history.max() {
                let avg = history.reduce(0, +) / Double(history.count)
                HStack {
                    largeStat("最低", lo)
                    largeStat("平均", avg)
                    largeStat("最高", hi)
                }
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .leading)
    }

    private func largeStat(_ label: String, _ value: Double) -> some View {
        VStack(spacing: 1) {
            Text(label).font(.caption2).foregroundStyle(.tertiary)
            Text(value.formatted(.number.precision(.fractionLength(0...2))))
                .font(.footnote.monospacedDigit().weight(.semibold))
        }
        .frame(maxWidth: .infinity)
    }

    /// The trend visual for the roomy families, honoring the style choice:
    /// gauge → circular gauge, bar → linear capacity, otherwise sparkline
    /// (falling back to a gauge, then to nothing, when there's no history).
    @ViewBuilder private func trendPane(_ m: MetricState, tint: Color) -> some View {
        switch effectiveStyle {
        case .gauge:
            MetricGaugeView(metric: m)
                .scaleEffect(1.3)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        case .bar:
            VStack(spacing: 4) {
                Spacer(minLength: 0)
                if let g = m.gauge {
                    Gauge(value: g.value, in: g.range) { EmptyView() }
                        .gaugeStyle(.accessoryLinearCapacity)
                        .tint(tint)
                }
                Spacer(minLength: 0)
            }
        default:
            if (m.history?.count ?? 0) >= 2 {
                SparklineView(metric: m)
            } else if m.gauge != nil {
                MetricGaugeView(metric: m)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                Color.clear
            }
        }
    }

    @ViewBuilder private func content(_ m: MetricState) -> some View {
        let tint = Presentation.tint(m.tint)
        switch effectiveStyle {
        case .gauge:
            VStack(spacing: 4) {
                MetricGaugeView(metric: m)
                    .scaleEffect(family == .systemSmall ? 1.25 : 1)
                Text(m.label ?? m.key)
                    .font(.caption2).foregroundStyle(.secondary).lineLimit(1)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
        case .bar:
            VStack(alignment: .leading, spacing: 6) {
                HStack(spacing: 4) {
                    Presentation.icon(m.icon, status: "running")
                        .font(.caption).foregroundStyle(tint)
                    Text(m.label ?? m.key)
                        .font(.caption).foregroundStyle(.secondary).lineLimit(1)
                }
                Text(Presentation.formatValue(m.value))
                    .font(.title2.monospacedDigit().weight(.semibold))
                    .minimumScaleFactor(0.5)
                if let g = m.gauge {
                    Gauge(value: g.value, in: g.range) { EmptyView() }
                        .gaugeStyle(.accessoryLinearCapacity)
                        .tint(tint)
                }
                if let target = m.target {
                    Text("目标 \(target)")
                        .font(.caption2).foregroundStyle(.tertiary)
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .leading)
        case .spark:
            VStack(alignment: .leading, spacing: 4) {
                HStack(spacing: 4) {
                    Presentation.icon(m.icon, status: "running")
                        .font(.caption).foregroundStyle(tint)
                    Text(m.label ?? m.key)
                        .font(.caption).foregroundStyle(.secondary).lineLimit(1)
                    Spacer()
                    Text(Presentation.formatValue(m.value))
                        .font(.callout.monospacedDigit().weight(.semibold))
                }
                SparklineView(metric: m)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .leading)
        case .number, .auto:
            VStack(alignment: .leading, spacing: 2) {
                HStack(spacing: 4) {
                    Presentation.icon(m.icon, status: "running")
                        .font(.caption).foregroundStyle(tint)
                    Text(m.label ?? m.key)
                        .font(.caption).foregroundStyle(.secondary).lineLimit(1)
                }
                Spacer(minLength: 0)
                Text(Presentation.formatValue(m.value))
                    .font(.system(.largeTitle, design: .rounded).weight(.semibold))
                    .monospacedDigit()
                    .minimumScaleFactor(0.4)
                    .lineLimit(1)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .leading)
        }
    }
}
