// swift-tools-version:6.0
import PackageDescription

// macOS menu bar app as a plain SwiftPM executable — `swift run` for dev.
// Ships later as a proper .app bundle; the SwiftUI code moves unchanged.
let package = Package(
    name: "SitrepMenuBar",
    platforms: [.macOS(.v14)],
    dependencies: [
        .package(path: "../SitrepKit")
    ],
    targets: [
        .executableTarget(
            name: "SitrepMenuBar",
            dependencies: ["SitrepKit"]
        )
    ]
)
