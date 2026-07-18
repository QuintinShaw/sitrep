import SitrepKit
import SwiftUI

/// The official cloud. Users never see the word "server" — self-hosters
/// find the override in 高级 settings (Tailscale-style).
let officialServer = URL(string: "https://sitrep.quintinshaw.com")!

/// First-launch: silently mint an anonymous space, persist credentials to
/// the shared config, and show the pairing code.
@MainActor
enum Onboarding {
    static func ensureSpace() async -> ClientConfig? {
        if let cfg = ClientConfig.loadFromFile(), cfg.space != nil || cfg.token != nil {
            return cfg
        }
        do {
            let name = Host.current().localizedName ?? "Mac"
            let creds = try await APIClient.createSpace(server: officialServer, platform: "macos", name: name)
            let cfg = ClientConfig(
                server: officialServer.absoluteString,
                token: creds.ownerToken,
                space: creds.spaceID
            )
            try cfg.save()
            return cfg
        } catch {
            return nil
        }
    }
}

/// Gift-card-style connect code: a black frame, one string, scan or copy.
struct InviteQRView: View {
    let client: APIClient
    let server: String
    @State private var invite: APIClient.Invite?
    @State private var error: String?
    @State private var copied = false

    var body: some View {
        VStack(spacing: 12) {
            if let invite {
                let display = invite.code
                Text("在 iPhone 上打开 Sitrep，\n用相机对准这个代码")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)

                // Apple-gift-card look: white card, thin black outline,
                // large black monospaced characters. Deliberately single-
                // theme — it reads as a physical card in light or dark.
                Text(display)
                    .font(.system(size: 26, weight: .medium, design: .monospaced))
                    .kerning(0.5)
                    .lineLimit(1)
                    .minimumScaleFactor(0.5)
                    .foregroundStyle(.black)
                    .padding(.vertical, 14)
                    .padding(.horizontal, 12)
                    .frame(maxWidth: .infinity)
                    .background(RoundedRectangle(cornerRadius: 6).fill(.white))
                    .overlay(
                        RoundedRectangle(cornerRadius: 6)
                            .strokeBorder(.black, lineWidth: 2.5)
                    )
                    .textSelection(.enabled)

                Text("10 分钟内有效 · 用一次即失效")
                    .font(.caption2)
                    .foregroundStyle(.tertiary)

                HStack {
                    Button(copied ? "已复制 ✓" : "复制代码") {
                        NSPasteboard.general.clearContents()
                        NSPasteboard.general.setString(display, forType: .string)
                        copied = true
                        Task {
                            try? await Task.sleep(for: .seconds(2))
                            copied = false
                        }
                    }
                    Button("重新生成") { Task { await mint() } }
                }
                .controlSize(.small)
            } else if let error {
                Text(error).foregroundStyle(.red).font(.caption)
                Button("重试") { Task { await mint() } }
            } else {
                ProgressView().task { await mint() }
            }
        }
        .padding(16)
        .frame(width: 320)
    }

    private func mint() async {
        do {
            invite = try await client.createInvite(role: "viewer")
            error = nil
        } catch {
            self.error = "生成连接码失败：\(error.localizedDescription)"
        }
    }
}
