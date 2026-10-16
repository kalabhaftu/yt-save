# Changelog

## 0.1.1 — Queue cleanup and release workflow fix

- Added safe history removal for finished queue records and bulk clearing for completed records; downloaded files stay in place.
- Restored pending downloads in submission order after restart.
- Fixed the release workflow's Inno Setup check to avoid treating its help command's exit code as an installation failure.

## 0.1.0 — Initial foundation

- Created the native Slint/Rust desktop shell and transparent Yt Save icon.
- Added URL analysis, a SQLite-backed single-worker queue, yt-dlp progress parsing, and a pinned dependency manifest for yt-dlp, Deno, and FFmpeg/FFprobe.
- Added project documentation, installer sources, and CI/release scaffolding.
- Queue controls beyond enqueue, playlist selection, Windows tray/clipboard integration, and installer runtime verification remain in progress.
