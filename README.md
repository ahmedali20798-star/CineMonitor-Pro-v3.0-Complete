# CineMonitor Pro 3.0.0

Direct USB PTP Live View and cinema field monitor for Canon EOS cameras (including EOS R6 Mark II) on Android, built with Kotlin, Jetpack Compose, and OpenGL ES 3.0.

## Key Features & Fixes in v3.0.0
- **Zero-Allocation EVF Pipeline**: Reuses USB transfer buffers and a 3-slot `inBitmap` pool (`CanonPtpController` + `CinemaRenderer`) to prevent GC stutter during long shoots.
- **Strict PTP Session & Watchdog**: Validates transaction IDs and response codes (`0x2001`), cleanly closes partial connections on failure, and monitors frame stalls via monotonic clock (`elapsedRealtimeNanos`).
- **Fixed `.cube` 3D LUT Parser**: Corrected `TITLE` quote stripping and whitespace regex escaping (`\s+`), with automatic 3D texture restoration across OpenGL context recreation.
- **On-Demand OpenGL ES 3.0 Rendering**: Uses `RENDERMODE_WHEN_DIRTY` with automatic frame-driven updates and continuous animation support when Zebra stripes are active.
- **Dynamic Aspect Ratio & Anamorphic Desqueeze**: Automatically syncs live sensor aspect ratio with `CinemaGuidesOverlay` and `TouchAfOverlay`.
- **GitHub Actions CI**: Includes `.github/workflows/android-ci.yml` to automatically build and upload the debug APK on every push.
