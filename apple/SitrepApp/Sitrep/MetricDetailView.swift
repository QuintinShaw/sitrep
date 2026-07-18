import Charts
import SitrepKit
import SwiftUI

/// One metric as a full console: a real time-axis chart with ranges, the
/// numbers that matter, and every control in the open — style, color,
/// notification level and owning automation. Nothing behind a
/// long-press.
struct MetricDetailView: View {
    var model: AppModel
    let key: String

    @State private var range: SeriesRange = .day
    @State private var series: [SeriesPoint] = []
    @State private var seriesLoaded = false
    @State private var editingThreshold: String?
    @State private var thresholdDraft = ""

    private var metric: MetricState? {
        model.metrics.first { $0.key == key }
    }

    /// The automation that owns this metric, linked by stable id.
    private var automation: AutomationInfo? {
        guard let source = metric?.source else { return nil }
        return model.automations.first { "a\($0.id)" == source }
    }

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                if let m = metric {
                    header(m)
                    Picker("范围", selection: $range) {
                        ForEach(SeriesRange.allCases, id: \.self) { r in
                            Text(r.label).tag(r)
                        }
                    }
                    .pickerStyle(.segmented)
                    chartCard(m)
                    if series.count >= 2 {
                        stats(series.map(\.v))
                    }
                    controlsCard(m)
                    if let automation {
                        scheduleCard(automation)
                    }
                }
            }
            .padding(.horizontal)
        }
        .navigationTitle(metric?.label ?? key)
        .navigationBarTitleDisplayMode(.inline)
        .task(id: range) { await loadSeries() }
        // New data while the screen is open → refetch the window.
        .onChange(of: metric?.value) { _, _ in
            Task { await loadSeries() }
        }
        .alert(
            editingThreshold == "above" ? "高于此值时提醒" : "低于此值时提醒",
            isPresented: Binding(
                get: { editingThreshold != nil },
                set: { if !$0 { editingThreshold = nil } }
            )
        ) {
            TextField("阈值", text: $thresholdDraft)
                .keyboardType(.numbersAndPunctuation)
            Button("保存") {
                guard Double(thresholdDraft) != nil, let direction = editingThreshold else { return }
                Task {
                    if direction == "above" {
                        await model.updateMetric(id: key, alertAbove: thresholdDraft)
                    } else {
                        await model.updateMetric(id: key, alertBelow: thresholdDraft)
                    }
                }
                editingThreshold = nil
            }
            Button("取消", role: .cancel) { editingThreshold = nil }
        } message: {
            Text("提醒规则属于这个指标，保存后立即用于下一次数值更新。")
        }
    }

    private func loadSeries() async {
        guard let client = model.client else { return }
        if let fresh = try? await client.series(key: key, range: range) {
            withAnimation(.spring(duration: 0.35)) { series = fresh }
        }
        seriesLoaded = true
    }

    private func header(_ m: MetricState) -> some View {
        let tint = Presentation.tint(m.tint)
        return Card {
            VStack(alignment: .leading, spacing: 6) {
                HStack(spacing: 6) {
                    Presentation.icon(m.icon, status: "running")
                        .font(.callout)
                        .foregroundStyle(tint)
                    Text(m.label ?? m.key)
                        .font(.callout.weight(.medium))
                        .foregroundStyle(.secondary)
                    Spacer()
                    Text("更新于 \(m.updatedAt, format: .relative(presentation: .named))")
                        .font(.caption2)
                        .foregroundStyle(.tertiary)
                }
                HStack(alignment: .lastTextBaseline, spacing: 6) {
                    Text(Presentation.formatValue(m.value))
                        .font(.system(size: 44, weight: .semibold, design: .rounded))
                        .monospacedDigit()
                        .foregroundStyle(tint)
                        .contentTransition(.numericText())
                        .minimumScaleFactor(0.5)
                        .lineLimit(1)
                    if let delta = rangeDelta {
                        Spacer()
                        Label(delta.text, systemImage: delta.symbol)
                            .font(.footnote.monospacedDigit().weight(.semibold))
                            .foregroundStyle(delta.up ? Color.green : .red)
                    }
                }
            }
        }
    }

    /// Change across the selected window.
    private var rangeDelta: (text: String, symbol: String, up: Bool)? {
        guard let first = series.first?.v, let last = series.last?.v, first != last, first != 0 else { return nil }
        let pct = (last - first) / abs(first) * 100
        let text = "\(abs(pct).formatted(.number.precision(.fractionLength(1))))%"
        return (text, pct > 0 ? "arrow.up.right" : "arrow.down.right", pct > 0)
    }

    @ViewBuilder private func chartCard(_ m: MetricState) -> some View {
        let tint = Presentation.tint(m.tint)
        Card {
            if series.count >= 2 {
                Chart {
                    ForEach(series) { p in
                        LineMark(x: .value("时间", p.t), y: .value("值", p.v))
                            .interpolationMethod(.catmullRom)
                            .foregroundStyle(tint)
                        AreaMark(x: .value("时间", p.t), y: .value("值", p.v))
                            .interpolationMethod(.catmullRom)
                            .foregroundStyle(
                                LinearGradient(
                                    colors: [tint.opacity(0.25), tint.opacity(0.02)],
                                    startPoint: .top, endPoint: .bottom
                                )
                            )
                    }
                    if let target = m.target.flatMap(Double.init) {
                        RuleMark(y: .value("目标", target))
                            .lineStyle(StrokeStyle(lineWidth: 1, dash: [4, 4]))
                            .foregroundStyle(.secondary)
                            .annotation(position: .topTrailing) {
                                Text("目标 \(m.target ?? "")")
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }
                    }
                    // Alert lines: where the phone will buzz, drawn on the trend.
                    ForEach(
                        [m.alertAbove.flatMap(Double.init).map { ("提醒线", $0) },
                         m.alertBelow.flatMap(Double.init).map { ("提醒线 ", $0) }]
                            .compactMap(\.self), id: \.0
                    ) { label, y in
                        RuleMark(y: .value(label, y))
                            .lineStyle(StrokeStyle(lineWidth: 1, dash: [2, 3]))
                            .foregroundStyle(.red.opacity(0.7))
                            .annotation(position: .bottomTrailing) {
                                Text("\(label)\(y.formatted(.number.precision(.fractionLength(0...2))))")
                                    .font(.caption2)
                                    .foregroundStyle(.red)
                            }
                    }
                }
                .chartYScale(domain: .automatic(includesZero: false))
                .frame(height: 220)
            } else {
                VStack(spacing: 6) {
                    Image(systemName: "chart.xyaxis.line")
                        .font(.title2)
                        .foregroundStyle(.tertiary)
                    Text(seriesLoaded ? "这个时间范围内还没有数据" : "加载中…")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, minHeight: 160)
            }
        }
    }

    private func stats(_ values: [Double]) -> some View {
        let lo = values.min() ?? 0
        let hi = values.max() ?? 0
        let avg = values.reduce(0, +) / Double(values.count)
        return Card {
            HStack {
                stat("最低", lo)
                Divider().frame(height: 28)
                stat("平均", avg)
                Divider().frame(height: 28)
                stat("最高", hi)
                Divider().frame(height: 28)
                VStack(spacing: 2) {
                    Text("样本").font(.caption2).foregroundStyle(.secondary)
                    Text("\(values.count)")
                        .font(.callout.monospacedDigit().weight(.semibold))
                }
                .frame(maxWidth: .infinity)
            }
        }
    }

    private func stat(_ label: String, _ value: Double) -> some View {
        VStack(spacing: 2) {
            Text(label).font(.caption2).foregroundStyle(.secondary)
            Text(value.formatted(.number.precision(.fractionLength(0...2)).grouping(.automatic)))
                .font(.callout.monospacedDigit().weight(.semibold))
        }
        .frame(maxWidth: .infinity)
    }

    /// Style, color, and notification level in the open — long-press menus
    /// are where features go to be forgotten.
    private func controlsCard(_ m: MetricState) -> some View {
        Card {
            VStack(alignment: .leading, spacing: 14) {
                HStack {
                    Text("样式")
                        .font(.callout)
                    Spacer()
                    Picker("样式", selection: Binding(
                        get: { m.template ?? "number" },
                        set: { t in Task { await model.updateMetric(id: m.key, template: t) } }
                    )) {
                        Text("数字").tag("number")
                        Text("仪表").tag("gauge")
                        Text("进度").tag("bar")
                        Text("走势").tag("spark")
                    }
                    .pickerStyle(.segmented)
                    .frame(maxWidth: 220)
                }
                Divider()
                HStack(spacing: 10) {
                    Text("颜色")
                        .font(.callout)
                    Spacer()
                    ForEach(["blue", "purple", "green", "orange", "red", "teal"], id: \.self) { name in
                        Button {
                            Task { await model.updateMetric(id: m.key, tint: name) }
                        } label: {
                            Circle()
                                .fill(Presentation.tint(name))
                                .frame(width: 26, height: 26)
                                .overlay {
                                    if (m.tint ?? "blue") == name {
                                        Image(systemName: "checkmark")
                                            .font(.caption2.weight(.bold))
                                            .foregroundStyle(.white)
                                    }
                                }
                        }
                        .buttonStyle(.plain)
                    }
                }
                if m.alertAbove != nil || m.alertBelow != nil {
                    Divider()
                    HStack {
                        VStack(alignment: .leading, spacing: 2) {
                            Text("越线提醒")
                                .font(.callout)
                            Text(alertLineText(m))
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                        }
                        Spacer()
                        Button("编辑") {
                            if let below = m.alertBelow {
                                editingThreshold = "below"
                                thresholdDraft = below
                            } else if let above = m.alertAbove {
                                editingThreshold = "above"
                                thresholdDraft = above
                            }
                        }
                        .buttonStyle(.borderless)
                        Menu {
                            Button("时效性（穿透专注模式）") {
                                Task { await model.updateMetric(id: m.key, level: "warn") }
                            }
                            Button("普通") {
                                Task { await model.updateMetric(id: m.key, level: "info") }
                            }
                            Button("静音") {
                                Task { await model.updateMetric(id: m.key, level: "off") }
                            }
                        } label: {
                            HStack(spacing: 4) {
                                Text("级别")
                                Image(systemName: "chevron.up.chevron.down")
                                    .font(.caption2)
                            }
                            .font(.callout)
                        }
                    }
                }
            }
        }
    }

    private func alertLineText(_ m: MetricState) -> String {
        [m.alertAbove.map { "高于 \(Presentation.formatValue($0))" },
         m.alertBelow.map { "低于 \(Presentation.formatValue($0))" }]
            .compactMap(\.self)
            .joined(separator: " · ") + " 时通知"
    }

    private func scheduleCard(_ w: AutomationInfo) -> some View {
        Card {
            HStack {
                Image(systemName: w.enabled ? "clock" : "pause.circle")
                    .foregroundStyle(w.enabled ? Color.blue : .secondary)
                VStack(alignment: .leading, spacing: 2) {
                    Text("每\(intervalChoices.first { $0.1 == w.everyS }?.0 ?? "\(w.everyS)s")更新")
                        .font(.callout.weight(.medium))
                    if let last = w.lastRun {
                        Text("上次 \(RelativeDateTimeFormatter().localizedString(for: last, relativeTo: .now))")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                }
                Spacer()
                Button {
                    UIImpactFeedbackGenerator(style: .medium).impactOccurred()
                    Task { try? await model.client?.patchAutomation(id: w.id, runNow: true); await model.refresh() }
                } label: {
                    Label("立即更新", systemImage: "bolt.fill")
                        .font(.caption.weight(.semibold))
                }
                .buttonStyle(.bordered)
                .buttonBorderShape(.capsule)
                Menu {
                    Menu("间隔") {
                        ForEach(intervalChoices, id: \.1) { label, seconds in
                            Button(label) {
                                Task { try? await model.client?.patchAutomation(id: w.id, everyS: seconds); await model.refresh() }
                            }
                        }
                    }
                    Button(w.enabled ? "暂停" : "恢复") {
                        Task { try? await model.client?.patchAutomation(id: w.id, enabled: !w.enabled); await model.refresh() }
                    }
                } label: {
                    Image(systemName: "ellipsis.circle")
                        .font(.title3)
                }
            }
        }
    }
}
