import SwiftUI
#if canImport(UIKit)
import UIKit
#elseif canImport(AppKit)
import AppKit
#endif

// Shared rendering vocabulary for presentation hints — used by the Live
// Activity, widgets, the iOS app, and the macOS menu bar so every surface
// agrees. Unknown values always fall back; hints can never break rendering.
public enum Presentation {
    /// "67340.4" → "67,340.4"; non-numeric values pass through untouched.
    /// The detail discipline that separates a dashboard from a printf.
    public static func formatValue(_ raw: String) -> String {
        guard let d = Double(raw), d.isFinite else { return raw }
        let decimals = raw.split(separator: ".").dropFirst().first?.count ?? 0
        return d.formatted(.number.precision(.fractionLength(decimals)).grouping(.automatic))
    }

    public static func tint(_ name: String?) -> Color {
        switch name {
        case "purple": .purple
        case "green": .green
        case "orange": .orange
        case "red": .red
        case "pink": .pink
        case "teal": .teal
        case "indigo": .indigo
        case "gray": .gray
        case let hex? where hex.hasPrefix("#") && hex.count == 7:
            hexColor(hex) ?? .blue
        default: .blue
        }
    }

    private static func hexColor(_ hex: String) -> Color? {
        var value: UInt64 = 0
        guard Scanner(string: String(hex.dropFirst())).scanHexInt64(&value) else { return nil }
        return Color(
            red: Double((value >> 16) & 0xFF) / 255,
            green: Double((value >> 8) & 0xFF) / 255,
            blue: Double(value & 0xFF) / 255
        )
    }

    private static func symbolExists(_ name: String) -> Bool {
        #if canImport(UIKit)
        UIImage(systemName: name) != nil
        #elseif canImport(AppKit)
        NSImage(systemSymbolName: name, accessibilityDescription: nil) != nil
        #else
        false
        #endif
    }

    /// SF Symbol name → symbol; anything else (e.g. an emoji) → text; nil →
    /// status-appropriate default.
    @ViewBuilder
    public static func icon(_ name: String?, status: String) -> some View {
        if let name, symbolExists(name) {
            Image(systemName: name)
        } else if let name, !name.isEmpty, name.rangeOfCharacter(from: .alphanumerics) == nil {
            Text(name) // emoji or other glyph
        } else {
            switch status {
            case "done": Image(systemName: "checkmark.circle.fill")
            case "failed": Image(systemName: "xmark.circle.fill")
            default: Image(systemName: "dot.radiowaves.left.and.right")
            }
        }
    }

    /// Lifecycle overrides decoration: finished states always read green/red.
    public static func statusTint(_ status: String, hint: String?) -> Color {
        switch status {
        case "done": .green
        case "failed": .red
        default: tint(hint)
        }
    }
}
