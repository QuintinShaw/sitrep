import XCTest
@testable import SitrepKit

/// v1-architecture.md §2.3/§13.2: `PATCH /v2/metrics/:id` has no v1
/// successor — display preferences are entirely client-local now. These
/// tests exercise the store directly (no network involved by construction).
final class MetricPreferencesStoreTests: XCTestCase {
    /// Unique per test run so parallel/repeated runs never collide on the
    /// same UserDefaults key.
    private func freshMetricID() -> String { "test.metric.\(UUID().uuidString)" }

    func testUntouchedMetricHasEmptyOverride() {
        let id = freshMetricID()
        XCTAssertTrue(MetricPreferencesStore.get(id).isEmpty)
    }

    func testUpdateIsPartialAndPersists() {
        let id = freshMetricID()
        MetricPreferencesStore.update(id, tint: "blue")
        MetricPreferencesStore.update(id, template: "gauge")
        let stored = MetricPreferencesStore.get(id)
        XCTAssertEqual(stored.tint, "blue", "a later update must not clobber an earlier field")
        XCTAssertEqual(stored.template, "gauge")
        XCTAssertNil(stored.icon)
    }

    func testApplyLeavesUncustomizedMetricUnchanged() {
        let id = freshMetricID()
        let metric = MetricState(key: id, value: "42", updatedAt: .now)
        let applied = MetricPreferencesStore.apply(to: metric)
        XCTAssertEqual(applied, metric)
    }

    func testApplyOverlaysStoredFieldsOntoServerMetric() {
        let id = freshMetricID()
        MetricPreferencesStore.update(id, tint: "purple", alertAbove: "90")
        var metric = MetricState(key: id, value: "42", updatedAt: .now)
        metric.tint = "blue" // the server's own hint
        metric.alertBelow = "10" // server-reported threshold, untouched by the override
        let applied = MetricPreferencesStore.apply(to: metric)
        XCTAssertEqual(applied.tint, "purple", "the local override wins over the server hint")
        XCTAssertEqual(applied.alertAbove, "90")
        XCTAssertEqual(applied.alertBelow, "10", "fields the device never customized pass through")
    }
}
