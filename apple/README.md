# apple/

Swift code for all Apple platforms.

- `SitrepKit/` — shared SwiftPM package: models, API/WebSocket client. Shared
  by every target below.
- `Sitrep.xcworkspace` (to be created in Xcode) with targets:
  - **Sitrep iOS** — SwiftUI app
  - **SitrepWidgets** — WidgetKit extension: Live Activity (tasks) + widgets
    (metrics). Must declare `NSSupportsLiveActivitiesFrequentUpdates`.
  - **Sitrep Menu Bar** — macOS `NSStatusItem` app, thin client over the local
    daemon socket.

Live Activity constraints the implementation must respect (see
docs/competitive-research.md §2.4): 8h active-update ceiling → auto re-issue
via push-to-start; APNs token rotation; server-side throttling of updates.
