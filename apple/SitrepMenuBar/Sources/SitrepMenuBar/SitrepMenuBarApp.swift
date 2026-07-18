import SwiftUI
import SitrepKit

@main
struct SitrepMenuBarApp: App {
    @State private var model = StatusModel()

    init() {
        // No dock icon, no main window — menu bar only.
        NSApplication.shared.setActivationPolicy(.accessory)
    }

    var body: some Scene {
        MenuBarExtra {
            StatusPanel(model: model)
        } label: {
            MenuBarLabel(model: model)
        }
        .menuBarExtraStyle(.window)
    }
}

struct MenuBarLabel: View {
    var model: StatusModel

    var body: some View {
        // The menu bar renders this as a template image + text.
        if model.hasFailure {
            Image(systemName: "exclamationmark.triangle.fill")
        } else if model.runningCount > 0 {
            HStack(spacing: 2) {
                Image(systemName: "dot.radiowaves.left.and.right")
                Text("\(model.runningCount)")
            }
        } else {
            Image(systemName: "checkmark.circle")
        }
    }
}
