import Foundation

/// The gift-card-style connect code: `X` + 15 random characters, pure noise
/// (server-minted, no structure, nothing to leak). Alphabet excludes
/// 0/1/O/I/L confusables. Joining resolves the space server-side via the
/// invite directory, so the code alone is all a phone needs.
public enum ConnectCode {
    public static let length = 16

    /// Normalize a scanned/typed/pasted candidate; nil if it isn't a code.
    public static func parse(_ raw: String) -> String? {
        let s = raw.uppercased().filter { $0.isLetter || $0.isNumber }
        guard s.count == length, s.first == "X", s.last == "Z",
              s.allSatisfy({ !"01OIL".contains($0) })
        else { return nil }
        return s
    }

    /// Live-scan pattern (applied to whitespace-stripped OCR transcripts):
    /// X…Z anchors + exact length = instant shape verification.
    /// (Computed to satisfy Swift 6 strict concurrency — Regex isn't Sendable.)
    public static var scanPattern: Regex<Substring> {
        /X[A-HJ-KM-NP-Z2-9]{14}Z/
    }
}
