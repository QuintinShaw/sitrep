import ActivityKit
import SitrepKit
import SwiftUI
import WidgetKit

struct TaskLiveActivity: Widget {
    var body: some WidgetConfiguration {
        ActivityConfiguration(for: TaskActivityAttributes.self) { context in
            LockScreenView(context: context)
                .widgetURL(URL(string: "sitrep://task/\(context.attributes.sourceId)"))
        } dynamicIsland: { context in
            let tint = Presentation.statusTint(context.state.status, hint: context.attributes.tint)
            return DynamicIsland {
                DynamicIslandExpandedRegion(.leading) {
                    Presentation.icon(context.attributes.icon, status: context.state.status)
                        .font(.title2)
                        .foregroundStyle(tint)
                        .padding(.leading, 4)
                }
                DynamicIslandExpandedRegion(.trailing) {
                    TrailingStat(context: context, tint: tint)
                        .padding(.trailing, 4)
                }
                DynamicIslandExpandedRegion(.center) {
                    Text(context.attributes.title)
                        .font(.callout).bold()
                        .lineLimit(1)
                }
                DynamicIslandExpandedRegion(.bottom) {
                    VStack(alignment: .leading, spacing: 4) {
                        if template(context) == "progress" {
                            ProgressBar(percent: context.state.percent, status: context.state.status, tint: tint)
                        }
                        if let step = context.state.step {
                            Text(step)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .lineLimit(1)
                        }
                    }
                    .padding(.horizontal, 4)
                    .widgetURL(URL(string: "sitrep://task/\(context.attributes.sourceId)"))
                }
            } compactLeading: {
                Presentation.icon(context.attributes.icon, status: context.state.status)
                    .foregroundStyle(tint)
            } compactTrailing: {
                CompactStat(context: context, tint: tint)
            } minimal: {
                Presentation.icon(context.attributes.icon, status: context.state.status)
                    .foregroundStyle(tint)
            }
        }
    }
}

private func template(_ context: ActivityViewContext<TaskActivityAttributes>) -> String {
    switch context.attributes.template {
    case "timer", "plain": context.attributes.template!
    default: "progress"
    }
}

private func startDate(_ context: ActivityViewContext<TaskActivityAttributes>) -> Date {
    Date(timeIntervalSince1970: context.attributes.startedAtEpoch ?? Date().timeIntervalSince1970)
}

/// Expanded trailing region: percent, live elapsed timer, or nothing (plain).
private struct TrailingStat: View {
    let context: ActivityViewContext<TaskActivityAttributes>
    let tint: Color

    var body: some View {
        switch template(context) {
        case "timer" where context.state.status == "running":
            // System live timer: ticks every second on-device, zero pushes.
            Text(startDate(context), style: .timer)
                .font(.title3.monospacedDigit()).bold()
                .foregroundStyle(tint)
                .frame(maxWidth: 64)
        default:
            if let percent = context.state.percent {
                Text("\(percent)%")
                    .font(.title3.monospacedDigit()).bold()
                    .foregroundStyle(tint)
            }
        }
    }
}

private struct CompactStat: View {
    let context: ActivityViewContext<TaskActivityAttributes>
    let tint: Color

    var body: some View {
        switch template(context) {
        case "timer" where context.state.status == "running":
            Text(startDate(context), style: .timer)
                .font(.caption2.monospacedDigit())
                .foregroundStyle(tint)
                .frame(maxWidth: 44)
        case "plain":
            EmptyView()
        default:
            if let percent = context.state.percent {
                Text("\(percent)%")
                    .font(.caption2.monospacedDigit())
                    .foregroundStyle(tint)
            }
        }
    }
}

struct LockScreenView: View {
    let context: ActivityViewContext<TaskActivityAttributes>

    var body: some View {
        let tint = Presentation.statusTint(context.state.status, hint: context.attributes.tint)
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Presentation.icon(context.attributes.icon, status: context.state.status)
                    .foregroundStyle(tint)
                Text(context.attributes.title).font(.callout).bold().lineLimit(1)
                Spacer()
                switch template(context) {
                case "timer" where context.state.status == "running":
                    Text(startDate(context), style: .timer)
                        .font(.callout.monospacedDigit()).bold()
                        .foregroundStyle(tint)
                        .frame(maxWidth: 64)
                default:
                    if let percent = context.state.percent {
                        Text("\(percent)%").font(.callout.monospacedDigit()).bold()
                            .foregroundStyle(tint)
                    }
                }
            }
            if template(context) == "progress" {
                ProgressBar(percent: context.state.percent, status: context.state.status, tint: tint)
            }
            if let step = context.state.step {
                Text(step).font(.caption).foregroundStyle(.secondary).lineLimit(1)
            }
        }
        .padding(14)
        .activityBackgroundTint(Color.black.opacity(0.6))
        .activitySystemActionForegroundColor(.white)
    }
}

struct ProgressBar: View {
    let percent: Int?
    let status: String
    var tint: Color = .blue

    var body: some View {
        ProgressView(value: Double(percent ?? (status == "running" ? 0 : 100)), total: 100)
            .tint(tint)
    }
}
