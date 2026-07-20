import Foundation

/// The self-routing connect code (v1-architecture.md §10.5, P0-6): a
/// 21-character code that encodes the target `space_id` directly, so
/// `POST /v1/join` can route to the right SpaceHub with **zero** KV lookup.
/// This replaced the R1 design (`X` + 14 random chars, pure noise, routed
/// server-side via an eventually-consistent `INVITE_DIR` KV lookup) — that
/// KV lookup could spuriously 404 a join attempted immediately after invite
/// creation, before the write had propagated to the replica being read.
/// Embedding `space_id` in the code removes the dependency instead of
/// retrying around it.
public enum ConnectCode {
    /// Fixed total length: `X` + 10-char space_id + 9-char secret + `Z`.
    public static let length = 21

    /// The 31-symbol, confusable-free alphabet shared with the server's
    /// `newSpaceId()` (`server/src/app.ts`): digits `2-9` and letters `A-Z`
    /// excluding `I`, `L`, `O`. Uppercase is the code's display/scan form;
    /// the canonical wire form is lowercase (same character set, one
    /// case-fold, no re-encoding). `X` and `Z` are themselves valid
    /// alphabet symbols and may also appear inside positions [1..19], not
    /// just as the anchors at [0] and [20].
    public static let alphabet = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"

    /// A successfully decoded code: the embedded `space_id` and one-time
    /// `secret`, both lowercased to the canonical wire form, plus the
    /// normalized (uppercased, trimmed) 21-character code string itself —
    /// the exact value to send as `POST /v1/join`'s `code` field.
    public struct Decoded: Equatable, Sendable {
        public var code: String
        public var spaceID: String
        public var secret: String
    }

    public enum DecodeError: Error, LocalizedError, Equatable {
        case wrongLength(Int)
        case badAnchors
        case invalidCharacter(Character)

        public var errorDescription: String? {
            switch self {
            case .wrongLength(let n):
                return "connect code must be \(ConnectCode.length) characters (got \(n))"
            case .badAnchors:
                return "connect code must start with X and end with Z"
            case .invalidCharacter(let c):
                return "connect code contains a character outside the alphabet: '\(c)'"
            }
        }
    }

    /// Decodes a scanned/typed/pasted candidate per the §10.5 byte layout:
    /// `[0]` = literal `X`, `[1..10]` (10 chars) = `space_id`, `[11..19]`
    /// (9 chars) = one-time `secret`, `[20]` = literal `Z`. Whitespace is
    /// stripped and the candidate is uppercased before validation (matching
    /// OCR/typed input); every character — anchors included — must be in
    /// `alphabet`. Throws a specific `DecodeError` for malformed input so
    /// callers can show a clear message and MUST NOT attempt a network call
    /// on failure.
    public static func decode(_ raw: String) throws -> Decoded {
        let s = raw.uppercased().filter { !$0.isWhitespace }
        guard s.count == length else { throw DecodeError.wrongLength(s.count) }
        guard s.first == "X", s.last == "Z" else { throw DecodeError.badAnchors }
        for ch in s where !alphabet.contains(ch) {
            throw DecodeError.invalidCharacter(ch)
        }
        let chars = Array(s)
        let spaceID = String(chars[1...10]).lowercased()
        let secret = String(chars[11...19]).lowercased()
        return Decoded(code: s, spaceID: spaceID, secret: secret)
    }

    /// Shape-only normalize/validate; nil if `raw` isn't a well-formed code.
    /// A thin wrapper over `decode` for call sites that only need a
    /// yes/no + normalized string (e.g. enabling a submit button) and don't
    /// need `space_id`/`secret` themselves.
    public static func parse(_ raw: String) -> String? {
        (try? decode(raw))?.code
    }

    /// Live-scan pattern (applied to whitespace-stripped OCR transcripts):
    /// `X…Z` anchors + exact length + alphabet = instant shape verification,
    /// before ever calling `decode`. (Computed to satisfy Swift 6 strict
    /// concurrency — Regex isn't Sendable.)
    public static var scanPattern: Regex<Substring> {
        /X[2-9A-HJ-KM-NP-Z]{19}Z/
    }
}
