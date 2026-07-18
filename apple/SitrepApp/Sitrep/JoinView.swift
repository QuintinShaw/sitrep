import SitrepKit
import SwiftUI
import UIKit
import VisionKit

/// First-run onboarding, gift-card style: point the camera at the connect
/// code shown on the Mac (live text recognition), paste it, or type it.
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

    var body: some View {
        VStack(spacing: 16) {
            Spacer()
            Image(systemName: "dot.radiowaves.left.and.right")
                .font(.system(size: 44))
                .foregroundStyle(.blue)
            Text("连接你的电脑").font(.title2.bold())
            Text("在 Mac 的 Sitrep 菜单里点「添加设备」，\n把相机对准它显示的代码")
                .font(.subheadline)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)

            if DataScannerViewController.isSupported {
                ZStack {
                    CodeScanner(control: scannerControl) { text in
                        guard !joining else { return }
                        Task { await join(payload: text) }
                    }
                    // The instant a code matches, the camera stops and this
                    // takes over — unmistakable "got it" feedback.
                    if joining {
                        Rectangle().fill(.thinMaterial)
                        ProgressView("正在连接…")
                    }
                }
                .frame(height: 240)
                .clipShape(RoundedRectangle(cornerRadius: 16))
                .padding(.horizontal)
            }

            HStack {
                TextField("输入连接码", text: $manualCode)
                    .font(.body.monospaced())
                    .textInputAutocapitalization(.characters)
                    .autocorrectionDisabled()
                    .textFieldStyle(.roundedBorder)
                    .onSubmit { Task { await join(payload: manualCode) } }
                Button {
                    Task {
                        if let text = UIPasteboard.general.string {
                            await join(payload: text)
                        } else {
                            error = "剪贴板是空的"
                        }
                    }
                } label: {
                    Image(systemName: "doc.on.clipboard")
                }
                .buttonStyle(.bordered)
                Button("连接") { Task { await join(payload: manualCode) } }
                    .buttonStyle(.borderedProminent)
                    .disabled(ConnectCode.parse(manualCode) == nil)
            }
            .padding(.horizontal)

            if let error {
                Text(error).font(.caption).foregroundStyle(.red)
            }
            Spacer()
        }
        .padding()
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

    /// Accepts a connect code (official server implicit) or a full
    /// sitrep://join link (self-host path).
    private func join(payload: String) async {
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
