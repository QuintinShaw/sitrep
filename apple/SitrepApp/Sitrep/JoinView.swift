import SitrepKit
import SwiftUI
import UIKit
import VisionKit

/// First-run pairing: scan the connect code shown on the Mac, paste it,
/// or enter it manually.
/// Lets the SwiftUI layer re-arm the scanner after a failed join.
@MainActor
final class ScannerControl {
    weak var coordinator: CodeScanner.Coordinator?

    func rejectAndResume(_ code: String) {
        coordinator?.reject(code)
    }
}

struct JoinView: View {
    @Bindable var model: AppModel
    @State private var manualCode = ""
    @State private var error: String?
    @State private var joining = false
    @State private var scannerControl = ScannerControl()
    @FocusState private var codeFieldFocused: Bool

    var body: some View {
        ScrollView {
            VStack(spacing: 0) {
                header
                    .padding(.top, 28)

                scanner
                    .padding(.top, 30)

                manualEntry
                    .padding(.top, 20)
            }
            .padding(.horizontal, 20)
            .padding(.bottom, 32)
        }
        .scrollDismissesKeyboard(.interactively)
        .background(Color(.systemGroupedBackground).ignoresSafeArea())
        .interactiveDismissDisabled()
        .task(id: model.pendingJoinPayload) {
            guard let payload = model.pendingJoinPayload else { return }
            model.pendingJoinPayload = nil
            await join(payload: payload)
        }
        // A full-screen cover is the frontmost URL recipient. Handle pairing
        // here as well as in MainTabView so cold-start and already-open links
        // both reach the join flow.
        .onOpenURL { url in
            Task { await join(payload: url.absoluteString) }
        }
    }

    private var header: some View {
        VStack(spacing: 14) {
            Image(systemName: "macbook.and.iphone")
                .font(.largeTitle)
                .symbolRenderingMode(.hierarchical)
                .foregroundStyle(.blue)
                .padding(18)
                .background(.blue.opacity(0.12), in: RoundedRectangle(cornerRadius: 24))

            VStack(spacing: 8) {
                Text("连接 Mac")
                    .font(.largeTitle.bold())

                Text("在 Mac 菜单栏打开 Sitrep，选择「添加设备」，\n然后扫描它显示的连接码。")
                    .font(.body)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .frame(maxWidth: .infinity)
    }

    @ViewBuilder private var scanner: some View {
        if DataScannerViewController.isSupported {
            ZStack {
                CodeScanner(control: scannerControl) { text in
                    guard !joining else { return }
                    Task { await join(payload: text) }
                }

                LinearGradient(
                    colors: [.black.opacity(0.24), .clear, .black.opacity(0.32)],
                    startPoint: .top,
                    endPoint: .bottom
                )

                RoundedRectangle(cornerRadius: 14)
                    .stroke(
                        .white.opacity(0.9),
                        style: StrokeStyle(lineWidth: 2, dash: [10, 7])
                    )
                    .frame(maxWidth: 270)
                    .aspectRatio(4.6, contentMode: .fit)
                    .shadow(color: .black.opacity(0.18), radius: 8, y: 3)

                VStack {
                    Spacer()
                    Label("将连接码放入框内", systemImage: "viewfinder")
                        .font(.subheadline.weight(.medium))
                        .foregroundStyle(.white)
                        .padding(.horizontal, 14)
                        .padding(.vertical, 8)
                        .background(.ultraThinMaterial, in: Capsule())
                        .padding(.bottom, 14)
                }

                if joining {
                    Rectangle()
                        .fill(.regularMaterial)
                    ProgressView("正在连接…")
                        .font(.headline)
                }
            }
            .aspectRatio(1.45, contentMode: .fit)
            .clipShape(RoundedRectangle(cornerRadius: 28))
            .overlay {
                RoundedRectangle(cornerRadius: 28)
                    .stroke(Color(.separator).opacity(0.45), lineWidth: 0.5)
            }
            .accessibilityLabel("连接码扫描器")
        } else {
            ContentUnavailableView(
                "无法使用相机扫描",
                systemImage: "camera.fill",
                description: Text("请在下方粘贴或输入连接码")
            )
            .frame(maxWidth: .infinity)
            .aspectRatio(1.45, contentMode: .fit)
            .background(Color(.secondarySystemGroupedBackground), in: RoundedRectangle(cornerRadius: 28))
        }
    }

    private var manualEntry: some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack {
                Label("手动输入", systemImage: "keyboard")
                    .font(.headline)

                Spacer()

                Button("粘贴", systemImage: "doc.on.clipboard") {
                    pasteCode()
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
                .disabled(joining)
            }

            TextField("连接码或邀请链接", text: $manualCode)
                .font(.body.monospaced())
                .textInputAutocapitalization(.characters)
                .autocorrectionDisabled()
                .focused($codeFieldFocused)
                .submitLabel(.go)
                .onSubmit { Task { await join(payload: manualCode) } }
                .padding(.horizontal, 14)
                .frame(minHeight: 52)
                .background(Color(.tertiarySystemGroupedBackground), in: RoundedRectangle(cornerRadius: 14))
                .overlay {
                    RoundedRectangle(cornerRadius: 14)
                        .stroke(Color(.separator).opacity(0.35), lineWidth: 0.5)
                }
                .disabled(joining)

            if let error {
                Label(error, systemImage: "exclamationmark.circle.fill")
                    .font(.footnote)
                    .foregroundStyle(.red)
                    .fixedSize(horizontal: false, vertical: true)
                    .transition(.move(edge: .top).combined(with: .opacity))
            }

            Button {
                Task { await join(payload: manualCode) }
            } label: {
                if joining {
                    ProgressView()
                        .frame(maxWidth: .infinity)
                } else {
                    Label("连接", systemImage: "link")
                        .frame(maxWidth: .infinity)
                }
            }
            .buttonStyle(.borderedProminent)
            .controlSize(.large)
            .disabled(!canSubmit || joining)
        }
        .padding(16)
        .background(Color(.secondarySystemGroupedBackground), in: RoundedRectangle(cornerRadius: 24))
        .animation(.snappy, value: error)
    }

    private var canSubmit: Bool {
        let payload = manualCode.trimmingCharacters(in: .whitespacesAndNewlines)
        if ConnectCode.parse(payload) != nil { return true }
        guard let components = URLComponents(string: payload) else { return false }
        return components.scheme == "sitrep" && components.host == "join"
    }

    private func pasteCode() {
        guard let text = UIPasteboard.general.string,
              !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            error = "剪贴板里没有连接码"
            return
        }
        manualCode = text.trimmingCharacters(in: .whitespacesAndNewlines)
        error = nil
        codeFieldFocused = true
    }

    /// Accepts a connect code (official server implicit) or a full
    /// sitrep://join link (self-host path).
    private func join(payload: String) async {
        guard !joining else { return }
        let trimmed = payload.trimmingCharacters(in: .whitespacesAndNewlines)
        if let code = ConnectCode.parse(trimmed) {
            await performJoin(
                server: URL(string: "https://sitrep.quintinshaw.com")!,
                space: nil, code: code
            )
            return
        }
        if let comps = URLComponents(string: trimmed),
           comps.scheme == "sitrep", comps.host == "join",
           let server = comps.queryItems?.first(where: { $0.name == "server" })?.value,
           let space = comps.queryItems?.first(where: { $0.name == "space" })?.value,
           let code = comps.queryItems?.first(where: { $0.name == "code" })?.value,
           let serverURL = URL(string: server) {
            await performJoin(server: serverURL, space: space, code: code)
            return
        }
        error = "无法识别的连接码"
    }

    private func performJoin(server: URL, space: String?, code: String) async {
        joining = true
        error = nil
        defer { joining = false }
        do {
            let joined = try await APIClient.join(
                server: server, space: space, code: code,
                name: UIDevice.current.name, platform: "ios"
            )
            model.serverURL = server.absoluteString
            model.token = joined.token
            model.deviceID = joined.deviceID
            model.saveSettings()
            model.needsOnboarding = false
            await model.enableNotifications()
        } catch {
            self.error = "连接失败：代码无效或已过期，请重新生成后再扫"
            // Re-arm the scanner, ignoring this dead code so it doesn't
            // re-trigger while still on screen.
            scannerControl.rejectAndResume(code)
        }
    }
}

/// Live text scanner filtered to the connect-code pattern — random text in
/// the camera view never triggers a join attempt.
struct CodeScanner: UIViewControllerRepresentable {
    var control: ScannerControl?
    var onCode: (String) -> Void

    func makeUIViewController(context: Context) -> DataScannerViewController {
        let vc = DataScannerViewController(
            // Latin-only OCR: skipping the CJK text model is the single
            // biggest speed win; the code alphabet is pure A-Z/2-9.
            recognizedDataTypes: [.text(languages: ["en-US"])],
            qualityLevel: .balanced,
            recognizesMultipleItems: true,
            isHighlightingEnabled: true
        )
        vc.delegate = context.coordinator
        context.coordinator.scanner = vc
        control?.coordinator = context.coordinator
        try? vc.startScanning()
        return vc
    }

    func updateUIViewController(_ vc: DataScannerViewController, context: Context) {}

    func makeCoordinator() -> Coordinator { Coordinator(onCode: onCode) }

    final class Coordinator: NSObject, DataScannerViewControllerDelegate {
        let onCode: (String) -> Void
        weak var scanner: DataScannerViewController?
        private var fired = false
        private var rejected: Set<String> = []
        init(onCode: @escaping (String) -> Void) { self.onCode = onCode }

        /// A join with this code failed: don't fire on it again, resume.
        func reject(_ code: String) {
            rejected.insert(code)
            fired = false
            try? scanner?.startScanning()
        }

        // OCR refines transcripts over time (didUpdate) and may split one
        // code across adjacent text items — so on every change, scan each
        // transcript AND the concatenation of all of them, whitespace
        // stripped.
        private func scan(_ allItems: [RecognizedItem]) {
            guard !fired else { return }
            var transcripts: [String] = []
            for item in allItems {
                if case .text(let text) = item {
                    transcripts.append(text.transcript.filter { !$0.isWhitespace })
                }
            }
            for candidate in transcripts + [transcripts.joined()] {
                if let match = candidate.firstMatch(of: ConnectCode.scanPattern),
                   !rejected.contains(String(match.output)) {
                    fired = true
                    scanner?.stopScanning() // one shot; also stops the camera work
                    onCode(String(match.output))
                    return
                }
            }
        }

        func dataScanner(
            _ dataScanner: DataScannerViewController,
            didAdd addedItems: [RecognizedItem],
            allItems: [RecognizedItem]
        ) {
            scan(allItems)
        }

        func dataScanner(
            _ dataScanner: DataScannerViewController,
            didUpdate updatedItems: [RecognizedItem],
            allItems: [RecognizedItem]
        ) {
            scan(allItems)
        }
    }
}
