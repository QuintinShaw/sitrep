// swift-tools-version:6.0
import PackageDescription

// SitrepKit is the Swift model + client layer shared by the iOS app, the
// Widget/Live Activity extensions, and the macOS menu bar app.
let package = Package(
    name: "SitrepKit",
    platforms: [.iOS(.v17), .macOS(.v14)],
    products: [
        .library(name: "SitrepKit", targets: ["SitrepKit"])
    ],
    targets: [
        .target(name: "SitrepKit"),
        .testTarget(name: "SitrepKitTests", dependencies: ["SitrepKit"]),
    ]
)
