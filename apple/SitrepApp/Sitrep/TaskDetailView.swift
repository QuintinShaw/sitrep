import SitrepKit
import SwiftUI

/// Full view of one task: hero progress, metadata, live output tail.
struct TaskDetailView: View {
    var model: AppModel
    let sourceID: String
    @State private var log: [String] = []
    @State private var confirmStop = false

    private var task: TaskState? {
        model.tasks.first { $0.sourceID == sourceID }
    }

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                if let task {
                    hero(task)
                    meta(task)
                }
                logSection
            }
            .padding(.horizontal)
        }
        .navigationTitle(task?.title ?? "任务")
        .navigationBarTitleDisplayMode(.inline)
        .task {
            while !Task.isCancelled {
                log = (try? await model.client?.taskLog(id: sourceID)) ?? log
                try? await Task.sleep(for: .seconds(2))
            }
        }
        .toolbar {
            if let task, task.status == .running {
                ToolbarItem(placement: .topBarTrailing) {
                    Button(role: .destructive) {
                        confirmStop = true
                    } label: {
                        Image(systemName: "stop.circle")
                    }
                }
            }
        }
        .confirmationDialog("停止这个任务？", isPresented: $confirmStop, titleVisibility: .visible) {
            Button("停止任务", role: .destructive) {
                if let task {
                    Task { await model.send(.stop, to: task) }
                }
            }
        }
    }

    private func hero(_ task: TaskState) -> some View {
        let tint = Presentation.statusTint(task.status.rawValue, hint: task.tint)
        return Card {
            HStack(spacing: 20) {
                ZStack {
                    Circle().stroke(tint.opacity(0.15), lineWidth: 10)
                    Circle()
                        .trim(from: 0, to: task.status == .running ? Double(task.percent ?? 0) / 100 : 1)
                        .stroke(tint, style: StrokeStyle(lineWidth: 10, lineCap: .round))
                        .rotationEffect(.degrees(-90))
                        .animation(.spring(duration: 0.6), value: task.percent)
                    VStack(spacing: 2) {
                        Presentation.icon(task.icon, status: task.status.rawValue)
                            .font(.title3)
                            .foregroundStyle(tint)
                        if task.status == .running, task.template == "timer", let started = task.startedAt {
                            Text(started, style: .timer)
                                .font(.callout.monospacedDigit().weight(.semibold))
                        } else if let percent = task.percent {
                            Text("\(percent)%")
                                .font(.title3.monospacedDigit().bold())
                                .contentTransition(.numericText())
                        }
                    }
                }
                .frame(width: 110, height: 110)
                VStack(alignment: .leading, spacing: 6) {
                    Text(statusLabel(task.status))
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(tint)
                    if let step = task.step {
                        Text(step)
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }
                }
                Spacer(minLength: 0)
            }
        }
    }

    private func meta(_ task: TaskState) -> some View {
        Card {
            VStack(spacing: 10) {
                if let started = task.startedAt {
                    metaRow("开始时间", value: started.formatted(date: .abbreviated, time: .shortened))
                    metaRow("运行时长", value: durationText(from: started, to: task.status == .running ? .now : task.updatedAt))
                }
                metaRow("最近更新", value: task.updatedAt.formatted(date: .omitted, time: .standard))
            }
        }
    }

    private func metaRow(_ label: String, value: String) -> some View {
        HStack {
            Text(label).font(.subheadline).foregroundStyle(.secondary)
            Spacer()
            Text(value).font(.subheadline.monospacedDigit())
        }
    }

    private var logSection: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("输出").font(.title3.bold())
            Card {
                if log.isEmpty {
                    Text("暂无输出")
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                } else {
                    ScrollViewReader { proxy in
                        ScrollView {
                            VStack(alignment: .leading, spacing: 3) {
                                ForEach(Array(log.enumerated()), id: \.offset) { i, line in
                                    Text(line)
                                        .font(.caption.monospaced())
                                        .foregroundStyle(.secondary)
                                        .frame(maxWidth: .infinity, alignment: .leading)
                                        .id(i)
                                }
                            }
                        }
                        .frame(maxHeight: 320)
                        .onChange(of: log.count) {
                            withAnimation { proxy.scrollTo(log.count - 1, anchor: .bottom) }
                        }
                    }
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private func statusLabel(_ s: TaskStatus) -> String {
        switch s {
        case .running: "运行中"
        case .done: "已完成"
        case .failed: "已失败"
        }
    }

    private func durationText(from: Date, to: Date) -> String {
        let seconds = Int(to.timeIntervalSince(from))
        if seconds < 60 { return "\(seconds) 秒" }
        if seconds < 3600 { return "\(seconds / 60) 分 \(seconds % 60) 秒" }
        return "\(seconds / 3600) 小时 \(seconds % 3600 / 60) 分"
    }
}
