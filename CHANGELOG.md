# Changelog

## Unreleased

- Add one-time local date/time scheduling for single downloads and selected playlist entries; scheduled queue rows persist across restart and remain outside worker slots until due.
- Add scheduled-job pause, resume, cancel, reorder, queue summaries, and history export fields; the app must be running at the scheduled time and does not configure Windows auto-launch.
- Cover due-time promotion, claim ordering, pause/cancel, schema migration, restart persistence, local-time validation, and scheduled queue presentation. Installed-app timer acceptance remains pending.
- Add single-media subtitle-only downloads with language selection, optional automatic captions, and SRT/VTT sidecars; queued choices survive retries, restart recovery, and history export.
- Show manual and automatic caption languages from Analyze, validate requested languages before queueing, reject playlist subtitle requests clearly, and fail completion if yt-dlp reports no output sidecar.
- Extend the Library filters and relinking picker to include SRT/VTT files; add deterministic queue-worker, parser, command, migration, and library coverage. Live subtitle download and Windows UI acceptance remain pending.
- Add main-queue Pause all, Resume paused, confirmed Cancel all, and Retry failed actions, with real SQLite/process transitions and partial-file preservation.
- Cover bulk cancel, failed/interrupted retry selection, and queue-toolbar action availability with app tests.
- Add a deterministic queue-worker acceptance fixture that inspects metadata, streams progress, creates media, and verifies the completed SQLite record and output file.
- Verify the packaged yt-dlp and explicit Deno runtime can download a public YouTube sample and merge its low-resolution video/audio streams with bundled FFmpeg.
- Add an opt-in Windows acceptance test that runs the app queue worker against packaged yt-dlp, Deno, FFmpeg, and FFprobe, then verifies the downloaded YouTube file and SQLite completion record.
- Add durable playlist-group identity and queue actions to pause or resume all selected entries together, including active jobs while preserving partial files.
- Add SQLite migration and queue-view coverage for playlist group persistence, mixed group states, and filtered queue views.
- Add Select all, Select none, and inclusive index-range selection to playlist previews; entries without valid playlist indexes remain unselectable.
- Add a file-backed Library view for completed downloads, with search, media/missing-file filters, date/title/size sorting, Open and Show actions, and relinking for moved files without copying or deleting media.
- Add regression coverage for library search/filter/sort and missing paths; Windows file-picker and Explorer runtime acceptance remains pending.
- Add configurable automatic retries for transient HTTP, rate-limit, and temporary network failures, with durable attempt/deadline state, restart recovery, and exponential backoff capped at four minutes.
- Add an Off-to-five retry selector; pause, cancel, reorder, and priority controls include retry-waiting rows. Permanent URL, access, and dependency failures remain manual.
- Add regression coverage for retry classification, persisted retry deadlines and counts, due-time promotion, and retry settings. CI and Windows runtime acceptance are pending.
- Add case-insensitive local search across queue and history titles, sources, URLs, statuses, formats, output paths, and error text.
- Add a Windows Save dialog and UTF-8 CSV export for persisted download history, with RFC 4180 quoting for commas, quotes, and line breaks.
- Add a saved per-download fragment concurrency setting (1–8) and pass it to yt-dlp as `--concurrent-fragments` for HLS/DASH transfers.
- Add a persisted parallel-download limit (1–8, default 1), queue-header selector, and dispatcher-managed workers; changing the limit applies to new launches without interrupting active jobs.
- Persist queue rank and High/Normal priority, and add move up/down/top/bottom controls for queued and paused items.
- Replace competing per-item waiters with a single dispatcher that claims the next row in the current persisted order; save selected playlist entries as one atomic batch.
- Add backward-compatible queue schema migration and coverage for rank migration, queue order, priority, reordering, and conditional claims. CI and Windows UI acceptance are pending.

## 0.1.14 — Clipboard popup and playlist confirmation

- Add a default-off, non-activating Windows popup for copied HTTP(S) media links, positioned in the active monitor's work area and suppressed when Windows reports quiet time or full-screen/presentation activity.
- Offer Analyze, Add video, Add audio, More options, and Dismiss actions; the popup expires after 12 seconds and performs no metadata lookup until the user chooses an action.
- Inspect Add to queue requests before saving; playlist URLs open entry selection and queue only items the user selects.
- Keep clipboard payloads in memory only. Popup focus, monitor/DPI placement, Windows notification state, and clipboard/tray behavior still need Windows runtime acceptance.

## 0.1.13 — System tray and orderly shutdown

- Add a Slint system tray menu to reopen or hide the main window, pause all downloads, resume paused downloads, open the download folder, and quit.
- Persist default-off close-to-tray and minimize-to-tray preferences.
- Pause queued items and request safe pauses from active jobs before quitting; retain the window and tray if active jobs do not stop cleanly.
- Add tray preference and status-summary coverage. Windows tray interaction, native minimize behavior, and installer lifecycle acceptance remain pending.

## 0.1.12 — Opt-in clipboard URL review

- Add a default-off Windows clipboard listener that parses one HTTP(S) URL locally and ignores other clipboard text.
- Persist the explicit monitoring choice; do not store clipboard contents or initiate metadata lookup/download until the user chooses an action.
- Show copied links in the main window with Use and Dismiss actions, then reuse the existing Analyze and Add to queue controls.
- Add parser, event-deduplication, and SQLite preference coverage. Floating popup, tray behavior, and Windows notification support remain pending.

## 0.1.11 — Persistent New download presets

- Restore the Video/Audio only mode, video quality, audio format, and audio bitrate controls from SQLite `app_settings` at startup.
- Save these form defaults only after a new single item is successfully queued, or after at least one selected playlist item is saved. Existing, retried, resumed, and recovered jobs keep their per-job choices and never rewrite the form defaults.
- Validate each saved setting against its supported typed values and fall back independently to the existing defaults when a value is absent or invalid.
- Add focused model, SQLite, retry, reopen, and startup-recovery coverage. Windows UI acceptance remains pending.

## 0.1.10 — Persistent download destination

- Add a saved default download folder and a native Windows folder picker. Each new queue item snapshots the chosen folder; changing the default leaves queued and retryable jobs on their original destination.
- Add a backward-compatible SQLite migration for destination snapshots and preserve playlist-title subfolders under the selected folder. Queue destinations must resolve to an existing directory.
- Add focused settings, legacy-row migration, retry/reopen persistence, path validation, and yt-dlp argument coverage. Windows picker and installed-app verification remain pending.

## 0.1.9 — Video quality presets

- Add per-job Best, 2160p/4K, 1440p, 1080p, 720p, 480p, 360p, and Lowest video quality choices.
- Use yt-dlp height-capped format selectors with a combined-format fallback under the same cap; preserve the choice through database migration, retry, pause/resume, and startup recovery. Audio-only jobs keep their existing behavior.
- Add model, command, migration, persistence, retry, and recovery coverage. Windows UI and site-specific format acceptance remain pending.
- Pass Windows CI and dependency security audit; release MSI and Inno Setup packages installed, launched the app and bundled tools, and uninstalled successfully.

## 0.1.8 — Audio output and queue controls

- Add per-item Video or Audio only selection and Original, MP3, M4A, Opus, AAC, ALAC, FLAC, Vorbis, or WAV output choices, plus persisted Best/320/192/128 kbps quality settings for applicable conversions.
- Preserve choices through SQLite migration, retries, and startup recovery; converted audio uses yt-dlp's FFmpeg postprocessor and reports missing bundled tools clearly. ALAC/FLAC/WAV limitations are explained in the UI and docs.
- Add per-job pause and resume for queued and active downloads. Active pause stops yt-dlp and its Windows process tree, preserves partial files and queue settings, and resumes with yt-dlp `--continue`; queued pause and worker claim use conditional SQLite transitions.
- Keep Paused rows paused after restart and recover interrupted Pausing/Canceling stop requests as Failed with progress retained for review. Add focused command, persistence, and recovery coverage; Windows UI, process-tree, partial-resume, and audio conversion acceptance remain pending.

## 0.1.7 — Active download cancellation

- Cancel active yt-dlp metadata inspection and downloads, including the Windows process tree, while preserving partial progress for retry.
- Keep cancellation, completion, and failure transitions conditional so stale progress cannot overwrite a requested cancellation.
- Recover an interrupted cancellation as Failed with a warning to check for lingering media processes before retrying.
- Show saved failure and recovery warnings ahead of byte progress in queue rows.

## 0.1.6 — Select playlist entries

- Preview up to 50 playlist entries and queue selected entries as separate downloads.
- Preserve each selected entry's original playlist URL and index through SQLite migration, retries, and startup recovery.
- Keep whole-playlist downloads available as a separate queue action.

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
