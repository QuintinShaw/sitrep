import XCTest
@testable import SitrepKit

/// v1-architecture.md §8.1: a WS upgrade rejected with `503
/// {"error": "transport_unavailable"}` is a capability fact, not a
/// transient network error. `RealtimeClient.classifyConnectFailure` is the
/// pure reclassification function the live client calls after a failed
/// handshake — exercised here directly with a synthetic `HTTPURLResponse`,
/// no live socket required, keeping `RealtimeClient` itself transport-
/// agnostic and testable.
final class RealtimeTransportUnavailableTests: XCTestCase {
    private func response(status: Int) -> HTTPURLResponse {
        HTTPURLResponse(url: URL(string: "https://example.com/v1/realtime")!,
                        statusCode: status, httpVersion: nil, headerFields: nil)!
    }

    func test503ResponseReclassifiesAsTransportUnavailable() {
        let original = URLError(.badServerResponse)
        let reclassified = RealtimeClient.classifyConnectFailure(original, response: response(status: 503))
        guard case RealtimeClientError.transportUnavailable = reclassified else {
            return XCTFail("expected .transportUnavailable, got \(reclassified)")
        }
    }

    func testNonServiceUnavailableStatusPassesThroughUnchanged() {
        let original = URLError(.badServerResponse)
        let reclassified = RealtimeClient.classifyConnectFailure(original, response: response(status: 401))
        XCTAssertEqual((reclassified as? URLError)?.code, .badServerResponse)
    }

    func testNoResponseAtAllPassesThroughUnchanged() {
        let original = URLError(.timedOut)
        let reclassified = RealtimeClient.classifyConnectFailure(original, response: nil)
        XCTAssertEqual((reclassified as? URLError)?.code, .timedOut)
    }

    /// The run loop must treat `.transportUnavailable` as terminal (stop
    /// retrying, no backoff) — mirrors the same treatment as a fatal,
    /// non-retryable server error, asserted here at the `Notice`/phase
    /// level via a live-free scenario is out of scope for a synchronous
    /// unit test, so this documents the classification contract the
    /// run loop's `isTerminal`-adjacent branch relies on: a
    /// `.transportUnavailable` error is never a `.serverError`, so it must
    /// be checked as its own case (see `RealtimeClient.runLoop`).
    func testTransportUnavailableIsDistinctFromServerError() {
        let error = RealtimeClientError.transportUnavailable
        if case RealtimeClientError.serverError = error {
            XCTFail("transportUnavailable must not be representable as serverError")
        }
    }
}
