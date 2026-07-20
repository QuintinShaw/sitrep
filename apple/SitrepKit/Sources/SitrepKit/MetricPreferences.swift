import Foundation

// v1 drops the old `PATCH /v2/metrics/:id` route with no successor
// (v1-architecture.md §2.3, §13.2): a per-viewer metric display override
// (style/tint/alert lines) has no cross-viewer convergence channel in v1 —
// "v1 keeps display preferences client-local (each device stores its own,
// no server round-trip)." This file is that client-local store, replacing
// the old server round-trip entirely; nothing here ever touches the network.

/// One device's display override for one metric. All fields are optional —
/// only fields the user has actually touched are set, so a metric the
/// device never customized has an all-nil (`isEmpty`) override.
public struct MetricDisplayOverride: Codable, Sendable, Equatable {
    public var icon: String?
    public var tint: String?
    public var template: String?
    public var alertAbove: String?
    public var alertBelow: String?

    public init(icon: String? = nil, tint: String? = nil, template: String? = nil,
                alertAbove: String? = nil, alertBelow: String? = nil) {
        self.icon = icon
        self.tint = tint
        self.template = template
        self.alertAbove = alertAbove
        self.alertBelow = alertBelow
    }

    public var isEmpty: Bool {
        icon == nil && tint == nil && template == nil && alertAbove == nil && alertBelow == nil
    }
}

/// Persists overrides in the shared App Group container so the main app,
/// its widgets, and Live Activities all agree on the same per-metric style —
/// the same durability domain `SharedConfig` already uses for credentials.
/// Falls back to `UserDefaults.standard` when the App Group is unavailable
/// (e.g. the macOS menu bar target, which has no App Group entitlement and
/// keeps its own independent local prefs — "each device stores its own" is
/// true per-target here, not just per-physical-device).
public enum MetricPreferencesStore {
    private static let suiteName = "group.dev.sitrep.app"
    private static let keyPrefix = "sitrep.metricPrefs."

    private static var defaults: UserDefaults {
        UserDefaults(suiteName: suiteName) ?? .standard
    }

    public static func get(_ metricID: String) -> MetricDisplayOverride {
        guard let data = defaults.data(forKey: keyPrefix + metricID),
              let value = try? JSONDecoder().decode(MetricDisplayOverride.self, from: data)
        else { return MetricDisplayOverride() }
        return value
    }

    /// Merges any non-nil parameter onto the stored override (a nil
    /// parameter leaves that field's existing stored value untouched) —
    /// mirrors the old PATCH route's partial-update semantics, now applied
    /// locally instead of server-side.
    @discardableResult
    public static func update(
        _ metricID: String, icon: String? = nil, tint: String? = nil, template: String? = nil,
        alertAbove: String? = nil, alertBelow: String? = nil
    ) -> MetricDisplayOverride {
        var current = get(metricID)
        if let icon { current.icon = icon }
        if let tint { current.tint = tint }
        if let template { current.template = template }
        if let alertAbove { current.alertAbove = alertAbove }
        if let alertBelow { current.alertBelow = alertBelow }
        if let data = try? JSONEncoder().encode(current) {
            defaults.set(data, forKey: keyPrefix + metricID)
        }
        return current
    }

    /// Applies this device's stored override on top of a server-reported
    /// metric — override wins field-by-field; a metric the device has never
    /// customized passes through unchanged.
    public static func apply(to metric: MetricState) -> MetricState {
        let override = get(metric.key)
        guard !override.isEmpty else { return metric }
        var m = metric
        if let icon = override.icon { m.icon = icon }
        if let tint = override.tint { m.tint = tint }
        if let template = override.template { m.template = template }
        if let alertAbove = override.alertAbove { m.alertAbove = alertAbove }
        if let alertBelow = override.alertBelow { m.alertBelow = alertBelow }
        return m
    }
}
