#!/bin/sh
# Packages the SwiftPM executable into a proper menu-bar .app bundle
# (needed for launch-at-login and a clean Dock-free identity).
# Usage: scripts/build-menubar-app.sh [output-dir]   (default: ./dist)
set -eu
cd "$(dirname "$0")/.."

OUT="${1:-dist}"
APP="$OUT/Sitrep Menu Bar.app"

case "$APP" in
	/*) APP_ABS="$APP" ;;
	*) APP_ABS="$PWD/$APP" ;;
esac

swift build -c release
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"

cp .build/release/SitrepMenuBar "$APP/Contents/MacOS/SitrepMenuBar"

# Embed the Go CLI/agent so the app is self-contained (the supervisor looks
# in Resources first).
(cd ../../daemon && go build -o "$APP_ABS/Contents/Resources/sitrep" ./cmd/sitrep)

cat > "$APP/Contents/Info.plist" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>CFBundleExecutable</key><string>SitrepMenuBar</string>
	<key>CFBundleIdentifier</key><string>dev.sitrep.menubar</string>
	<key>CFBundleName</key><string>Sitrep Menu Bar</string>
	<key>CFBundleShortVersionString</key><string>0.1.0</string>
	<key>CFBundleVersion</key><string>1</string>
	<key>CFBundlePackageType</key><string>APPL</string>
	<key>LSMinimumSystemVersion</key><string>14.0</string>
	<key>LSUIElement</key><true/>
</dict>
</plist>
EOF

codesign --force --sign - "$APP"
echo "built: $APP"
echo "install: cp -r \"$APP\" /Applications/"
