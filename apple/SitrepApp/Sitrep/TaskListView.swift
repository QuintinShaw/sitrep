import SitrepKit
import SwiftUI
import UIKit
import UserNotifications

struct SettingsView: View {
    @Bindable var model: AppModel
    @Environment(\.dismiss) private var dismiss
    @State private var devices: [DeviceInfo] = []
    @State private var confirmDisconnect = false

    var body: some View {
        NavigationStack {
            Form {
                Section("设备") {
                    ForEach(devices) { device in
                        HStack {
                            Image(systemName: platformIcon(device))
                            Text(device.name)
                            Spacer()
                            Text(roleLabel(device.role))
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        .swipeActions {
                            Button("移除", role: .destructive) {
                                Task {
                                    try? await model.client?.revokeDevice(id: device.id)
                                    devices.removeAll { $0.id == device.id }
                                }
                            }
                        }
                    }
                    if devices.isEmpty {
                        Text("加载中…").foregroundStyle(.secondary)
                    }
                }
                NotificationSettingsSection(model: model)
                Section {
                    Button("断开并重新连接", role: .destructive) {
                        confirmDisconnect = true
                    }
                } footer: {
                    Text("清除本机凭据，回到扫码连接界面。其他设备不受影响。")
                }
            }
            .navigationTitle("设置")
            .task {
                devices = (try? await model.client?.devices()) ?? []
            }
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("完成") { dismiss() }
                }
            }
            .confirmationDialog("断开这台 iPhone？", isPresented: $confirmDisconnect, titleVisibility: .visible) {
                Button("断开", role: .destructive) {
                    Task {
                        await model.disconnect()
                        dismiss()
                    }
                }
            }
        }
    }

    private func platformIcon(_ device: DeviceInfo) -> String {
        switch device.platform ?? (device.role == "source" ? "macos" : "ios") {
        case "ios": "iphone"
        case "android": "candybarphone"
        case "linux": "server.rack"
        default: "desktopcomputer"
        }
    }

    private func roleLabel(_ role: String) -> String {
        switch role {
        case "owner": "所有者"
        case "source": "电脑"
        default: "查看"
        }
    }
}

private struct NotificationSettingsSection: View {
    @Bindable var model: AppModel
    @State private var status: UNAuthorizationStatus = .notDetermined

    var body: some View {
        Section {
            LabeledContent("提醒权限") {
                Text(statusLabel).foregroundStyle(.secondary)
            }
            Button(status == .denied ? "前往系统设置" : "启用通知") {
                Task {
                    if status == .denied,
                       let url = URL(string: UIApplication.openSettingsURLString) {
                        await UIApplication.shared.open(url)
                    } else {
                        await model.enableNotifications()
                        await loadStatus()
                    }
                }
            }
            .disabled(status == .authorized || status == .provisional)
        } header: {
            Text("通知")
        } footer: {
            Text("任务完成和条件触发通过通知送达；灵动岛不依赖此权限。")
        }
        .task { await loadStatus() }
    }

    private var statusLabel: String {
        switch status {
        case .authorized, .provisional, .ephemeral: "已启用"
        case .denied: "已关闭"
        default: "未设置"
        }
    }

    private func loadStatus() async {
        status = await UNUserNotificationCenter.current().notificationSettings().authorizationStatus
    }
}
