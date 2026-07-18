import Foundation

/// Keeps `sitrep agent` (the Go scheduler) alive for as long as the menu bar
/// app runs. The binary is looked up in the bundle first, then dev paths.
final class AgentSupervisor: @unchecked Sendable {
    static let shared = AgentSupervisor()
    private var process: Process?
    private var stopped = false

    private var binary: String? {
        let candidates = [
            Bundle.main.path(forResource: "sitrep", ofType: nil),
            "/usr/local/bin/sitrep",
            "/opt/homebrew/bin/sitrep",
            "/tmp/sitrep",
        ]
        return candidates.compactMap { $0 }.first { FileManager.default.isExecutableFile(atPath: $0) }
    }

    func start() {
        guard process == nil, let bin = binary else { return }
        launch(bin: bin, backoff: 2)
    }

    private func launch(bin: String, backoff: TimeInterval) {
        let p = Process()
        p.executableURL = URL(fileURLWithPath: bin)
        p.arguments = ["agent"]
        p.standardOutput = FileHandle.nullDevice
        p.standardError = FileHandle.nullDevice
        p.terminationHandler = { [weak self] _ in
            guard let self, !self.stopped else { return }
            DispatchQueue.global().asyncAfter(deadline: .now() + backoff) {
                self.launch(bin: bin, backoff: min(backoff * 2, 60))
            }
        }
        do {
            try p.run()
            process = p
        } catch {
            // binary missing/quarantined: retry later
            DispatchQueue.global().asyncAfter(deadline: .now() + 30) {
                self.launch(bin: bin, backoff: backoff)
            }
        }
    }

    func stop() {
        stopped = true
        process?.terminate()
    }
}
