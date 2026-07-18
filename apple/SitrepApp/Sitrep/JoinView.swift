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
    private enum Destination: String, Identifiable {
        case scanner
        case manual

        var id: String { rawValue }
    }

    @Bindable var model: AppModel
    @State private var manualCode = ""
    @State private var error: String?
    @State private var joining = false
    @State private var scannerControl = ScannerControl()
    @State private var destination: Destination?

    var body: some View {
        VStack(spacing: 0) {
            Spacer(minLength: 32)

            header

            Spacer()

            VStack(spacing: 12) {
                Button {
                    present(.scanner)
                } label: {
                    Label("扫描连接码", systemImage: "camera.viewfinder")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)

                Button {
                    present(.manual)
                } label: {
                    Label("手动输入连接码", systemImage: "keyboard")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .controlSize(.large)
            }
            .buttonBorderShape(.roundedRectangle(radius: 18))
            .frame(maxWidth: .infinity)
            .padding(.bottom, 12)
        }
        .padding(.horizontal, 24)
        .padding(.bottom, 20)
        .background(Color(.systemGroupedBackground).ignoresSafeArea())
        .interactiveDismissDisabled()
        .sheet(item: $destination) { destination in
            destinationView(destination)
        }
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
        VStack(spacing: 18) {
            Image(systemName: "macbook.and.iphone")
                .font(.system(size: 46))
                .symbolRenderingMode(.hierarchical)
                .foregroundStyle(.blue)
                .padding(24)
                .background(.blue.opacity(0.12), in: RoundedRectangle(cornerRadius: 28))

            VStack(spacing: 10) {
                Text("连接 Mac")
                    .font(.largeTitle.bold())

                Text("在 Mac 菜单栏打开 Sitrep 并选择「添加设备」，再选择一种连接方式。")
                    .font(.body)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                    .fixedSize(horizontal: false, vertical: true)
                    .frame(maxWidth: 340)
            }
        }
        .frame(maxWidth: .infinity)
    }

    @ViewBuilder
    private func destinationView(_ destination: Destination) -> some View {
        switch destination {
        case .scanner:
            ScannerJoinView(
                joining: joining,
                error: error,
                scannerControl: scannerControl,
                onCode: { code in beginJoin(payload: code) }
            )
        case .manual:
            ManualJoinView(
                code: $manualCode,
                joining: joining,
                error: error,
                canSubmit: canSubmit,
                onPaste: pasteCode,
                onSubmit: { beginJoin(payload: manualCode) }
            )
        }
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
    }

    private func present(_ destination: Destination) {
        error = nil
        self.destination = destination
    }

    /// Accepts a connect code (official server implicit) or a full
    /// sitrep://join link (self-host path).
    private func join(payload: String) async {
        guard !joining else { return }
        joining = true
        error = nil
        await resolveJoin(payload: payload)
    }

    /// Switch the UI to its progress state in the same main-actor turn as
    /// the scanner callback, before URL parsing or any network work begins.
    private func beginJoin(payload: String) {
        guard !joining else { return }
        joining = true
        error = nil
        Task { await resolveJoin(payload: payload) }
    }

    private func resolveJoin(payload: String) async {
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
        joining = false
        error = "无法识别的连接码"
    }

    private func performJoin(server: URL, space: String?, code: String) async {
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

private struct ScannerJoinView: View {
    @Environment(\.dismiss) private var dismiss

    let joining: Bool
    let error: String?
    let scannerControl: ScannerControl
    let onCode: (String) -> Void

    var body: some View {
        NavigationStack {
            Group {
                if DataScannerViewController.isSupported {
                    ZStack {
                        CodeScanner(control: scannerControl) { code in
                            guard !joining else { return }
                            onCode(code)
                        }

                        LinearGradient(
                            colors: [.black.opacity(0.3), .clear, .black.opacity(0.42)],
                            startPoint: .top,
                            endPoint: .bottom
                        )
                        .allowsHitTesting(false)

                        RoundedRectangle(cornerRadius: 16)
                            .stroke(
                                .white.opacity(0.92),
                                style: StrokeStyle(lineWidth: 2, dash: [10, 7])
                            )
                            .frame(maxWidth: 300)
                            .aspectRatio(4.6, contentMode: .fit)
                            .shadow(color: .black.opacity(0.22), radius: 8, y: 3)
                            .allowsHitTesting(false)

                        VStack(spacing: 12) {
                            Spacer()

                            if let error {
                                Label(error, systemImage: "exclamationmark.circle.fill")
                                    .font(.footnote)
                                    .foregroundStyle(.red)
                                    .multilineTextAlignment(.center)
                                    .padding(.horizontal, 14)
                                    .padding(.vertical, 10)
                                    .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 14))
                                    .transition(.move(edge: .bottom).combined(with: .opacity))
                            }

                            Label("将 Mac 上的连接码放入框内", systemImage: "viewfinder")
                                .font(.subheadline.weight(.medium))
                                .foregroundStyle(.white)
                                .padding(.horizontal, 14)
                                .padding(.vertical, 9)
                                .background(.ultraThinMaterial, in: Capsule())
                        }
                        .padding()
                        .allowsHitTesting(false)

                        if joining {
                            Rectangle()
                                .fill(.regularMaterial)
                            ProgressView("正在连接…")
                                .font(.headline)
                        }
                    }
                    .accessibilityLabel("连接码扫描器")
                    .animation(.snappy, value: error)
                } else {
                    ContentUnavailableView(
                        "无法使用相机扫描",
                        systemImage: "camera.fill",
                        description: Text("请返回并选择手动输入")
                    )
                }
            }
            .background(Color(.systemBackground).ignoresSafeArea())
            .navigationTitle("扫描连接码")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("关闭", systemImage: "xmark") { dismiss() }
                }
            }
        }
        .interactiveDismissDisabled(joining)
    }
}

private struct ManualJoinView: View {
    @Environment(\.dismiss) private var dismiss
    @Binding var code: String
    @FocusState private var codeFieldFocused: Bool

    let joining: Bool
    let error: String?
    let canSubmit: Bool
    let onPaste: () -> Void
    let onSubmit: () -> Void

    var body: some View {
        NavigationStack {
            Form {
                Section {
                    TextField("连接码或邀请链接", text: $code)
                        .font(.body.monospaced())
                        .textInputAutocapitalization(.characters)
                        .autocorrectionDisabled()
                        .focused($codeFieldFocused)
                        .submitLabel(.go)
                        .onSubmit(onSubmit)
                        .disabled(joining)

                    Button("从剪贴板粘贴", systemImage: "doc.on.clipboard") {
                        onPaste()
                        codeFieldFocused = true
                    }
                    .disabled(joining)
                } header: {
                    Text("连接码")
                } footer: {
                    Text("连接码由 Mac 上的 Sitrep 生成，仅用于将这台设备加入你的空间。")
                }

                if let error {
                    Section {
                        Label(error, systemImage: "exclamationmark.circle.fill")
                            .font(.footnote)
                            .foregroundStyle(.red)
                    }
                }
            }
            .navigationTitle("手动输入")
            .navigationBarTitleDisplayMode(.inline)
            .safeAreaInset(edge: .bottom) {
                Button(action: onSubmit) {
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
                .buttonBorderShape(.roundedRectangle(radius: 18))
                .disabled(!canSubmit || joining)
                .padding()
                .background(.bar)
            }
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("关闭", systemImage: "xmark") { dismiss() }
                }
            }
        }
        .interactiveDismissDisabled(joining)
        .onAppear { codeFieldFocused = true }
        .animation(.snappy, value: error)
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
            qualityLevel: .fast,
            recognizesMultipleItems: true,
            isHighFrameRateTrackingEnabled: true,
            isPinchToZoomEnabled: true,
            isGuidanceEnabled: false,
            isHighlightingEnabled: false
        )
        vc.delegate = context.coordinator
        context.coordinator.scanner = vc
        control?.coordinator = context.coordinator
        try? vc.startScanning()
        Task { @MainActor [weak vc] in
            // SwiftUI lays out the hosted controller after creation. Yield
            // once so the ROI is calculated from the final camera bounds.
            await Task.yield()
            guard let vc else { return }
            Self.updateRegionOfInterest(vc)
        }
        return vc
    }

    func updateUIViewController(_ vc: DataScannerViewController, context: Context) {
        Self.updateRegionOfInterest(vc)
    }

    @MainActor
    private static func updateRegionOfInterest(_ vc: DataScannerViewController) {
        let bounds = vc.view.bounds
        guard bounds.width > 0, bounds.height > 0 else { return }

        // Match the visible reticle while leaving enough vertical tolerance
        // for OCR bounding-box drift. VisionKit ignores unrelated page text.
        let width = min(max(bounds.width - 40, 0), 340)
        let height = min(max(bounds.height * 0.18, 100), 150)
        let region = CGRect(
            x: bounds.midX - width / 2,
            y: bounds.midY - height / 2,
            width: width,
            height: height
        )
        if vc.regionOfInterest != region {
            vc.regionOfInterest = region
        }
    }

    static func dismantleUIViewController(_ vc: DataScannerViewController, coordinator: Coordinator) {
        vc.stopScanning()
    }

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
                    onCode(String(match.output))
                    scanner?.stopScanning() // one shot; also stops the camera work
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
