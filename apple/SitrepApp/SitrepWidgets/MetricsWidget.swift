import SitrepKit
import SwiftUI
import WidgetKit

struct MetricsEntry: TimelineEntry {
    let date: Date
    let metrics: [MetricState]
    let configured: Bool
}

struct MetricsProvider: TimelineProvider {
    func placeholder(in context: Context) -> MetricsEntry {
        MetricsEntry(
            date: .now,
            metrics: [MetricState(key: "gh_stars", value: "1284", label: "GitHub ★", updatedAt: .now)],
            configured: true
        )
    }

    func getSnapshot(in context: Context, completion: @escaping (MetricsEntry) -> Void) {
        completion(placeholder(in: context))
    }

    func getTimeline(in context: Context, completion: @escaping (Timeline<MetricsEntry>) -> Void) {
        // WidgetKit invokes completion exactly once; safe across the Task hop.
        nonisolated(unsafe) let completion = completion
        Task {
            var entry = MetricsEntry(date: .now, metrics: [], configured: false)
            if let cfg = SharedConfig.load() {
                let client = APIClient(baseURL: cfg.server, token: cfg.token)
                let metrics = (try? await client.snapshot().metrics.map(\.state)) ?? []
                entry = MetricsEntry(
                    date: .now,
                    metrics: metrics.sorted { $0.key < $1.key },
                    configured: true
                )
            }
            // WidgetKit budgets refreshes; ~15 min is sustainable all day.
            completion(Timeline(entries: [entry], policy: .after(.now.addingTimeInterval(15 * 60))))
        }
    }
}

struct MetricsWidget: Widget {
    var body: some WidgetConfiguration {
        StaticConfiguration(kind: "SitrepMetrics", provider: MetricsProvider()) { entry in
            MetricsWidgetView(entry: entry)
                .containerBackground(.fill.tertiary, for: .widget)
        }
        .configurationDisplayName("自动汇总")
        .description("自动显示最新的几个指标，无需配置。")
        .supportedFamilies([.systemSmall, .systemMedium, .accessoryCircular, .accessoryRectangular])
    }
}

/// Lock-screen circular gauge: renders the first gauge-template metric (or
/// the first metric at all) as a system Gauge — the "speedometer" form.
struct MetricGaugeView: View {
    let metric: MetricState?

    var body: some View {
        if let metric, let gauge = metric.gauge {
            Gauge(value: gauge.value, in: gauge.range) {
                Presentation.icon(metric.icon, status: "running")
            } currentValueLabel: {
                Text(Presentation.formatValue(metric.value))
                    .font(.system(.body, design: .rounded).weight(.semibold))
                    .minimumScaleFactor(0.5)
            }
            .gaugeStyle(.accessoryCircular)
            .tint(Presentation.tint(metric.tint))
        } else if let metric {
            VStack(spacing: 1) {
                Presentation.icon(metric.icon, status: "running").font(.caption)
                Text(Presentation.formatValue(metric.value))
                    .font(.system(.body, design: .rounded).weight(.semibold))
                    .minimumScaleFactor(0.4)
            }
        } else {
            Image(systemName: "dot.radiowaves.left.and.right")
        }
    }
}

struct MetricsWidgetView: View {
    @Environment(\.widgetFamily) private var family
    let entry: MetricsEntry

    private var visible: [MetricState] {
        Array(entry.metrics.prefix(family == .systemSmall ? 2 : 4))
    }

    /// Prefer the metric explicitly styled as a gauge for accessory slots.
    private var featured: MetricState? {
        entry.metrics.first { $0.template == "gauge" } ?? entry.metrics.first
    }

    var body: some View {
        if family == .accessoryCircular {
            MetricGaugeView(metric: featured)
        } else if family == .accessoryRectangular {
            accessoryRectangular
        } else {
            homeScreenBody
        }
    }

    @ViewBuilder private var accessoryRectangular: some View {
        if let m = featured {
            HStack(spacing: 6) {
                MetricGaugeView(metric: m)
                VStack(alignment: .leading, spacing: 0) {
                    Text(m.label ?? m.key).font(.caption2)
                    Text(Presentation.formatValue(m.value)).font(.caption.monospacedDigit().weight(.semibold))
                }
            }
        } else {
            Text("Sitrep").font(.caption)
        }
    }

    /// Home-screen small: a gauge-styled metric takes over the whole widget.
    @ViewBuilder private var smallFeaturedGauge: some View {
        if let m = featured, m.template == "gauge", m.gauge != nil {
            VStack(spacing: 4) {
                MetricGaugeView(metric: m)
                    .scaleEffect(1.25)
                Text(m.label ?? m.key)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else {
            listBody
        }
    }

    @ViewBuilder private var homeScreenBody: some View {
        if family == .systemSmall {
            smallFeaturedGauge
        } else {
            listBody
        }
    }

    @ViewBuilder private var listBody: some View {
        VStack(alignment: .leading, spacing: 6) {
            if !entry.configured {
                Text("Open Sitrep to configure")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else if visible.isEmpty {
                Text("No metrics yet")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            } else {
                ForEach(visible) { metric in
                    VStack(alignment: .leading, spacing: 1) {
                        HStack(spacing: 3) {
                            if metric.icon != nil {
                                Presentation.icon(metric.icon, status: "running")
                                    .font(.caption2)
                                    .foregroundStyle(Presentation.tint(metric.tint))
                            }
                            Text(metric.label ?? metric.key)
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                        }
                        Text(Presentation.formatValue(metric.value))
                            .font(family == .systemSmall ? .title3 : .headline)
                            .fontWeight(.semibold)
                            .monospacedDigit()
                            .lineLimit(1)
                            .minimumScaleFactor(0.6)
                    }
                }
            }
            Spacer(minLength: 0)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
