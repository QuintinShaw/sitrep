import XCTest
@testable import SitrepKit

/// Generation-scoped connection-resource ownership: a stale run-loop
/// generation unwinding after a stop()/start() cycle on the SAME
/// RealtimeClient instance must not tear down or replace the newer
/// generation's socket/lease/watchdog resources. No real network is used:
/// the guard itself is pure logic, and the behavioral test connects to a
/// local port that refuses immediately.
final class RealtimeGenerationTests: XCTestCase {
    // MARK: - Pure guard matrix

    func testTeardownPolicyMatrix() {
        // stop() — unconditional teardown regardless of current generation.
        XCTAssertTrue(ConnectionTeardownPolicy.shouldTearDownShared(requestedBy: nil, current: 1))
        XCTAssertTrue(ConnectionTeardownPolicy.shouldTearDownShared(requestedBy: nil, current: 7))

        // The owning generation tears down its own shared resources.
        XCTAssertTrue(ConnectionTeardownPolicy.shouldTearDownShared(requestedBy: 3, current: 3))

        // A stale generation (superseded by stop()+start()) must NOT touch
        // the newer generation's shared resources.
        XCTAssertFalse(ConnectionTeardownPolicy.shouldTearDownShared(requestedBy: 1, current: 2))
        XCTAssertFalse(ConnectionTeardownPolicy.shouldTearDownShared(requestedBy: 2, current: 5))
    }

    // MARK: - stop()/start() on the same instance

    /// The public-contract regression: after stop(), the SAME instance must
    /// be restartable — the stale generation's deferred teardown (still
    /// unwinding from its suspended connect attempt) must not kill the new
    /// generation's run loop or connection attempt. Uses a closed local
    /// port, so each connect attempt fails fast without real network.
    func testStopThenStartOnSameInstanceRestarts() async throws {
        let configuration = RealtimeClient.Configuration(
            url: URL(string: "ws://127.0.0.1:1/v3/realtime")!, token: nil, deviceID: "iphone-test-01")
        let client = RealtimeClient(configuration: configuration)

        // Generation 1 starts and leaves idle (connecting or already failed).
        await client.start()
        try await waitUntil("first start leaves .idle") {
            await client.currentPhase != .idle
        }

        // Stop, then immediately start again on the same instance — the
        // window in which generation 1's connectOnce is still unwinding.
        await client.stop()
        await client.start()

        // Generation 2 must be alive and progressing: it leaves .idle and,
        // because the port refuses, reaches .failed — proving the new run
        // loop owns the cycle and was not torn down by generation 1's
        // deferred cleanup.
        try await waitUntil("second start leaves .idle") {
            await client.currentPhase != .idle
        }
        try await waitUntil("second generation reaches .failed on a refused port") {
            if case .failed = await client.currentPhase { return true }
            return false
        }

        await client.stop()
        let final = await client.currentPhase
        XCTAssertEqual(final, .idle)
    }

    private func waitUntil(
        _ what: String, timeout: Double = 5,
        _ condition: @escaping () async -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if await condition() { return }
            try await Task.sleep(for: .milliseconds(25))
        }
        XCTFail("timed out waiting for: \(what)")
    }
}
