import XCTest
@testable import SitrepKit

/// Pure decision-logic tests for the P0 capability gate: whether a
/// RealtimeClient connection may be opened is decided solely from the most
/// recent REST refresh's `realtime_enabled`. No live socket involved — this
/// exercises `RealtimeCapabilityGate` directly.
final class RealtimeCapabilityGateTests: XCTestCase {
    func testColdStartDefaultsToNotCapable() {
        // Before any refresh has ever completed, the gate must already read
        // as "off" — this is what makes a cold start before first refresh
        // safe without any extra bookkeeping at call sites.
        let gate = RealtimeCapabilityGate()
        XCTAssertFalse(gate.isCapable)
    }

    func testFirstRefreshTrueConnects() {
        var gate = RealtimeCapabilityGate()
        let action = gate.apply(refreshedCapability: true)
        XCTAssertEqual(action, .connect)
        XCTAssertTrue(gate.isCapable)
    }

    func testRefreshFalseFromColdStartStaysOff() {
        var gate = RealtimeCapabilityGate()
        let action = gate.apply(refreshedCapability: false)
        XCTAssertEqual(action, .none, "already off, no teardown action needed")
        XCTAssertFalse(gate.isCapable)
    }

    func testAbsentFieldNeverConnects() {
        // Server response with no `realtime_enabled` key decodes to `false`
        // upstream (see ModelsTests); feeding that in must never connect.
        var gate = RealtimeCapabilityGate()
        XCTAssertEqual(gate.apply(refreshedCapability: false), .none)
        XCTAssertFalse(gate.isCapable)
    }

    func testTrueThenFalseTearsDownOnce() {
        var gate = RealtimeCapabilityGate()
        XCTAssertEqual(gate.apply(refreshedCapability: true), .connect)
        XCTAssertEqual(gate.apply(refreshedCapability: false), .disconnect)
        XCTAssertFalse(gate.isCapable)
    }

    func testRepeatingSameValueIsANoOp() {
        var gate = RealtimeCapabilityGate()
        XCTAssertEqual(gate.apply(refreshedCapability: true), .connect)
        // Steady-state refreshes that keep reporting true must not re-fire
        // a connect action every 30s poll / pull-to-refresh.
        XCTAssertEqual(gate.apply(refreshedCapability: true), .none)
        XCTAssertEqual(gate.apply(refreshedCapability: true), .none)
        XCTAssertTrue(gate.isCapable)
    }

    func testFalseThenTrueReconnectsAfterRollback() {
        var gate = RealtimeCapabilityGate()
        XCTAssertEqual(gate.apply(refreshedCapability: true), .connect)
        XCTAssertEqual(gate.apply(refreshedCapability: false), .disconnect)
        // "no reconnect attempts until a refresh says true again" — the very
        // next refresh reporting true is exactly that refresh.
        XCTAssertEqual(gate.apply(refreshedCapability: true), .connect)
        XCTAssertTrue(gate.isCapable)
    }

    func testResetReturnsToColdStartState() {
        var gate = RealtimeCapabilityGate()
        _ = gate.apply(refreshedCapability: true)
        XCTAssertTrue(gate.isCapable)
        gate.reset()
        XCTAssertFalse(gate.isCapable)
        // Re-pairing to a new server must prove capability again — a stale
        // `true` must not survive the reset.
        XCTAssertEqual(gate.apply(refreshedCapability: false), .none)
        XCTAssertFalse(gate.isCapable)
    }
}
