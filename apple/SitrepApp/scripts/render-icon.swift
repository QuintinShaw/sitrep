// Renders the app icon: Apple-native treatment — a deep blue-to-indigo
// gradient with the radio-waves glyph in white, SF-Symbol weight.
// Usage: swift render-icon.swift <output.png>
import AppKit

let size: CGFloat = 1024
let out = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "AppIcon-1024.png"

let image = NSImage(size: NSSize(width: size, height: size))
image.lockFocus()

// Background: subtle vertical gradient, iOS-native hue (indigo → blue).
let gradient = NSGradient(colors: [
    NSColor(calibratedRed: 0.16, green: 0.20, blue: 0.55, alpha: 1),
    NSColor(calibratedRed: 0.05, green: 0.42, blue: 0.95, alpha: 1),
])!
gradient.draw(in: NSRect(x: 0, y: 0, width: size, height: size), angle: 90)

// Glyph: dot.radiowaves.left.and.right, white, centered, generous margins.
let config = NSImage.SymbolConfiguration(pointSize: size * 0.42, weight: .medium)
if let symbol = NSImage(systemSymbolName: "dot.radiowaves.left.and.right", accessibilityDescription: nil)?
    .withSymbolConfiguration(config) {
    let tinted = NSImage(size: symbol.size)
    tinted.lockFocus()
    NSColor.white.set()
    let rect = NSRect(origin: .zero, size: symbol.size)
    symbol.draw(in: rect)
    rect.fill(using: .sourceAtop)
    tinted.unlockFocus()

    let glyphSize = NSSize(width: symbol.size.width * (size * 0.62 / symbol.size.width),
                           height: symbol.size.height * (size * 0.62 / symbol.size.width))
    tinted.draw(in: NSRect(
        x: (size - glyphSize.width) / 2,
        y: (size - glyphSize.height) / 2,
        width: glyphSize.width,
        height: glyphSize.height
    ))
}

image.unlockFocus()

guard let tiff = image.tiffRepresentation,
      let rep = NSBitmapImageRep(data: tiff),
      let png = rep.representation(using: .png, properties: [:]) else {
    fatalError("render failed")
}
try! png.write(to: URL(fileURLWithPath: out))
print("wrote \(out)")
