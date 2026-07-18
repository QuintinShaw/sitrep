import SitrepKit
import SwiftUI

/// Minimal trend line: recent history as a smoothed path with a soft area
/// fill and an emphasized endpoint. Pure Path — no chart dependency.
struct SparklineView: View {
    let metric: MetricState

    var body: some View {
        let points = metric.history ?? []
        let tint = Presentation.tint(metric.tint)
        if points.count >= 2, let lo = points.min(), let hi = points.max() {
            GeometryReader { geo in
                let span = hi - lo == 0 ? 1 : hi - lo
                let step = geo.size.width / CGFloat(points.count - 1)
                let ys = points.map { geo.size.height * (1 - CGFloat(($0 - lo) / span) * 0.86 - 0.07) }

                let line = Path { p in
                    p.move(to: CGPoint(x: 0, y: ys[0]))
                    for i in 1..<ys.count {
                        p.addLine(to: CGPoint(x: CGFloat(i) * step, y: ys[i]))
                    }
                }
                line.stroke(tint, style: StrokeStyle(lineWidth: 2, lineCap: .round, lineJoin: .round))

                Path { p in
                    p.move(to: CGPoint(x: 0, y: geo.size.height))
                    p.addLine(to: CGPoint(x: 0, y: ys[0]))
                    for i in 1..<ys.count {
                        p.addLine(to: CGPoint(x: CGFloat(i) * step, y: ys[i]))
                    }
                    p.addLine(to: CGPoint(x: geo.size.width, y: geo.size.height))
                }
                .fill(tint.opacity(0.15))

                Circle()
                    .fill(tint)
                    .frame(width: 5, height: 5)
                    .position(x: geo.size.width, y: ys[ys.count - 1])
            }
        } else {
            Rectangle().fill(.clear)
        }
    }
}
