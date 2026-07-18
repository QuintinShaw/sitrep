import ServiceManagement
import SwiftUI

/// Launch-at-login via SMAppService — only functional when running from a
/// real .app bundle (scripts/build-menubar-app.sh); hidden in dev `swift run`.
struct LaunchAtLoginToggle: View {
    @State private var enabled = SMAppService.mainApp.status == .enabled

    private var isBundled: Bool {
        Bundle.main.bundleIdentifier != nil && Bundle.main.bundlePath.hasSuffix(".app")
    }

    var body: some View {
        if isBundled {
            Toggle("开机自启", isOn: $enabled)
                .toggleStyle(.checkbox)
                .controlSize(.small)
                .onChange(of: enabled) { _, on in
                    do {
                        if on {
                            try SMAppService.mainApp.register()
                        } else {
                            try SMAppService.mainApp.unregister()
                        }
                    } catch {
                        enabled = SMAppService.mainApp.status == .enabled
                    }
                }
        }
    }
}
