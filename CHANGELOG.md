# Changelog

## 0.1.5 — Resolve transitive XML parser advisories

- Updated the Slint Linux accessibility dependency chain to `zbus_xml 5.2.1`, removing vulnerable `quick-xml 0.38.4` from the lockfile; only fixed `quick-xml 0.41.0` remains.
- Resolves [RUSTSEC-2026-0194](https://rustsec.org/advisories/RUSTSEC-2026-0194) and [RUSTSEC-2026-0195](https://rustsec.org/advisories/RUSTSEC-2026-0195) reported by the lockfile-wide dependency audit.

## 0.1.4 — Playlist folders and queued cancellation

- Save playlist entries in a playlist-title subfolder while keeping single-item downloads in the configured output directory.
- Add a queued-only Cancel action. SQLite arbitrates cancellation against worker start, and canceled waiters exit before launching yt-dlp.
- Verified the output-template paths using the bundled yt-dlp with local info-JSON fixtures; actual playlist download verification remains pending.
- Record successful v0.1.3 MSI and Inno Setup install, launch, bundled-tool, and uninstall smoke checks in GitHub Actions.

## 0.1.3 — Dependency preflight and installer smoke tests

- Added asynchronous startup checks that launch yt-dlp, Deno, FFmpeg, and FFprobe and report their version or failure. A failed Deno check warns that JavaScript-based formats may be incomplete; it does not gate other yt-dlp jobs.
- Added an Open action for completed files using the operating system's registered handler, with path validation off the UI thread.
- Added tagged Windows release checks that install, launch, inspect bundled tools, and uninstall both installer formats in isolated runner directories.
- Updated the feature matrix to distinguish Deno CLI/path validation from a real YouTube download test.

## 0.1.2 — Retry failed downloads

- Added a retry action for failed jobs. A conditional SQLite update rejects duplicate/stale retry actions and preserves partial-file progress for yt-dlp continuation.
- Updated the feature matrix and limitations to distinguish working retry and history cleanup from pause, cancellation, and reordering, which remain unfinished.

## 0.1.1 — Queue cleanup and release workflow fix

- Added safe history removal for finished queue records and bulk clearing for completed records; downloaded files stay in place.
- Restored pending downloads in submission order after restart.
- Fixed the release workflow's Inno Setup check to avoid treating its help command's exit code as an installation failure.

## 0.1.0 — Initial foundation

- Created the native Slint/Rust desktop shell and transparent Yt Save icon.
- Added URL analysis, a SQLite-backed single-worker queue, yt-dlp progress parsing, and a pinned dependency manifest for yt-dlp, Deno, and FFmpeg/FFprobe.
- Added project documentation, installer sources, and CI/release scaffolding.
- Queue controls beyond enqueue, playlist selection, Windows tray/clipboard integration, and installer runtime verification remain in progress.
