import ActivityKit
import Foundation

// Compiled into BOTH the app and the widget extension — the two must agree
// on this type exactly, and the server's APNs payload references it by name
// via "attributes-type": "TaskActivityAttributes".
struct TaskActivityAttributes: ActivityAttributes {
    struct ContentState: Codable, Hashable {
        var percent: Int?
        var step: String?
        var status: String // "running" | "done" | "failed"
    }

    var sourceId: String
    var title: String
    // Presentation hints (docs/design/presentation.md); all optional so old
    // payloads and old apps stay mutually compatible.
    var icon: String?
    var tint: String?
    var template: String? // "progress" (default) | "timer" | "plain"
    var startedAtEpoch: Double?
}
