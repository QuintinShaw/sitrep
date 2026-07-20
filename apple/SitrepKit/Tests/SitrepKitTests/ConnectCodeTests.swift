import XCTest
@testable import SitrepKit

/// v1-architecture.md §10.5 (P0-6): the connect code self-routes by
/// encoding `space_id` directly, so `POST /v1/join` needs zero KV lookup.
/// These cover the decode contract in isolation — no filesystem, no
/// network — since a mismatch between this decode and the daemon's Go
/// encode would silently break real pairing.
final class ConnectCodeTests: XCTestCase {
    /// Hand-constructed per the exact layout: 'X' + 10-char space_id +
    /// 9-char secret + 'Z'. `space_id` = "abcdefghjk" (10 chars, alphabet
    /// digits 2-9 + a-z minus i/l/o), `secret` = "23456789a" (9 chars).
    private let knownCode = "XABCDEFGHJK23456789AZ"

    func testDecodeKnownCodeExtractsSpaceIDAndSecret() throws {
        let decoded = try ConnectCode.decode(knownCode)
        XCTAssertEqual(decoded.spaceID, "abcdefghjk")
        XCTAssertEqual(decoded.secret, "23456789a")
        XCTAssertEqual(decoded.code, knownCode)
    }

    func testDecodeIsCaseInsensitiveOnInput() throws {
        let decoded = try ConnectCode.decode(knownCode.lowercased())
        XCTAssertEqual(decoded.spaceID, "abcdefghjk")
        XCTAssertEqual(decoded.secret, "23456789a")
        // Normalized `code` is always the uppercase display form.
        XCTAssertEqual(decoded.code, knownCode)
    }

    func testDecodeStripsWhitespace() throws {
        let spaced = "X ABCD EFGHJK 23456789A Z"
        let decoded = try ConnectCode.decode(spaced)
        XCTAssertEqual(decoded.code, knownCode)
    }

    // MARK: - Round trip (encode-by-hand -> decode)

    func testRoundTripForMultipleSpaceIDsAndSecrets() throws {
        let cases: [(spaceID: String, secret: String)] = [
            ("2345678923", "456789234"),
            ("zzzzzzzzzz", "999999999"),
            ("xzxzxzxzxz", "zxzxzxzxz"), // anchors X/Z legally appear inside
        ]
        for c in cases {
            let code = "X" + c.spaceID.uppercased() + c.secret.uppercased() + "Z"
            let decoded = try ConnectCode.decode(code)
            XCTAssertEqual(decoded.spaceID, c.spaceID)
            XCTAssertEqual(decoded.secret, c.secret)
        }
    }

    // MARK: - Malformed input is rejected client-side, before any network call

    func testWrongLengthIsRejected() {
        XCTAssertThrowsError(try ConnectCode.decode("XABCDEFGHJKZ")) { error in
            guard case ConnectCode.DecodeError.wrongLength = error else {
                return XCTFail("expected .wrongLength, got \(error)")
            }
        }
    }

    func testWrongStartAnchorIsRejected() {
        let bad = "Y" + String(knownCode.dropFirst())
        XCTAssertThrowsError(try ConnectCode.decode(bad)) { error in
            guard case ConnectCode.DecodeError.badAnchors = error else {
                return XCTFail("expected .badAnchors, got \(error)")
            }
        }
    }

    func testWrongEndAnchorIsRejected() {
        let bad = String(knownCode.dropLast()) + "Y"
        XCTAssertThrowsError(try ConnectCode.decode(bad)) { error in
            guard case ConnectCode.DecodeError.badAnchors = error else {
                return XCTFail("expected .badAnchors, got \(error)")
            }
        }
    }

    func testCharacterOutsideAlphabetIsRejected() {
        // '0', '1', 'I', 'L', 'O' are all excluded from the 31-symbol
        // alphabet even though they're valid uppercase letters/digits.
        for badChar in ["0", "1", "I", "L", "O"] {
            var chars = Array(knownCode)
            chars[5] = Character(badChar)
            let bad = String(chars)
            XCTAssertThrowsError(try ConnectCode.decode(bad), "expected rejection for '\(badChar)'") { error in
                guard case ConnectCode.DecodeError.invalidCharacter = error else {
                    return XCTFail("expected .invalidCharacter, got \(error)")
                }
            }
        }
    }

    func testDecodeFailureDoesNotProduceAPartialResult() {
        // Belt-and-suspenders: parse() (used for shape-only checks) must
        // also reject anything decode() rejects.
        XCTAssertNil(ConnectCode.parse("not a code"))
        XCTAssertNil(ConnectCode.parse(""))
    }

    // MARK: - Live-scan regex mirrors the decode contract

    func testScanPatternMatchesKnownCode() {
        XCTAssertNotNil(knownCode.wholeMatch(of: ConnectCode.scanPattern))
    }

    func testScanPatternRejectsOldSixteenCharLength() {
        // The R1 (pre-§10.5) 16-char code must NOT satisfy the new pattern.
        XCTAssertNil("XABCDEFGHJKLMNZ".wholeMatch(of: ConnectCode.scanPattern))
    }
}
