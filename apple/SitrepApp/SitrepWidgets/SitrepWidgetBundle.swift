import SwiftUI
import WidgetKit

@main
struct SitrepWidgetBundle: WidgetBundle {
    var body: some Widget {
        TaskLiveActivity()
        SingleMetricWidget()
        MultiMetricWidget()
        MetricsWidget()
    }
}
