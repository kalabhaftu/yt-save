# Yt Save

Yt Save is a native Rust desktop media downloader for Windows 10 and 11. It coordinates the official [yt-dlp](https://github.com/yt-dlp/yt-dlp) executable, so supported sites are determined by yt-dlp's extractors rather than a YouTube-only URL allowlist. YouTube's full format support is handled by the separately bundled Deno runtime and yt-dlp's bundled `yt-dlp-ejs` component.

The repository is in active development. This build provides URL analysis, a persisted download queue with a user-set concurrency limit, progress reporting, basic completed-history cleanup, and a custom Slint interface. Playlist preview, per-entry selection, per-job pause/resume, a persistent download-folder preference, opt-in clipboard review and a non-activating popup, and system-tray controls are available; sync subscriptions, library/history browsing, and installed-app acceptance remain tracked in [the feature matrix](docs/FEATURE_MATRIX.md).

## Screenshots

Real application screenshots will be added after the first Windows UI validation. No mock screenshots are shown as working software.

![Yt Save download icon](assets/yt-save.png)

## Current features

- Native Rust application with a Slint UI and Tokio background work.
- Accepts HTTP and HTTPS media URLs from any site yt-dlp can extract.
- Reads media metadata and available format counts through yt-dlp JSON output.
- Previews up to the first 50 playlist entries. Entries are selectable only when yt-dlp returns a positive playlist index; selected entries become separate durable queue jobs, while Add to queue still downloads the whole playlist.
- Persists queued items, progress, completion, and failure records in SQLite.
- Shows queue positions and an aggregate remaining-size estimate when yt-dlp has reported sizes; lets users move queued or paused items up, down, to the top, or to the bottom within their priority group, and supports a High priority flag. One dispatcher starts work in that persisted order, up to the saved parallel-download limit of 1–8; lowering the limit leaves active downloads running and delays new starts until capacity is available.
- Offers Video and Audio only queue choices. Audio can keep its best available source stream in Original format or convert to MP3, M4A, Opus, AAC, ALAC, FLAC, Vorbis, or WAV. Lossy conversions offer Best quality or target 320, 192, and 128 kbps settings and use the bundled FFmpeg tools.
- Offers per-job video quality presets: Best, 2160p/4K, 1440p, 1080p, 720p, 480p, 360p, or Lowest. Resolution presets cap yt-dlp's selected video height and fall back to the best combined format at or below that cap when separate video/audio streams are unavailable.
- Remembers the Video/Audio only mode, video quality, audio format, and audio bitrate shown in New download. Presets are saved after a queue item is successfully added; existing, resumed, and retried jobs keep their own saved choices.
- Offers optional, default-off clipboard monitoring on Windows. When enabled, a copied single HTTP(S) URL appears in a compact popup near the taskbar when Windows allows notifications; otherwise it remains available in the main window when visible. The popup does not take keyboard focus, expires after 12 seconds, and offers Analyze, Add video, Add audio, More options, and Dismiss. No metadata lookup or download starts until you choose an action, and playlist links open the entry-selection view before queueing. Clipboard contents are not stored.
- Provides a system tray menu to reopen or hide the window, pause all downloads, resume paused downloads, open the download folder, and quit after active work has safely paused. Close-to-tray and minimize-to-tray preferences are saved and start off.
- Lets users choose and save a default download folder with the Windows folder picker. New jobs snapshot that folder, so changing the default does not move queued, paused, interrupted, or retried jobs.
- Uses structured process arguments and yt-dlp progress templates; no shell command construction.
- Places playlist entries in an automatically named playlist subfolder while keeping single-video downloads directly in the configured output folder.
- Lets users cancel queued or active yt-dlp jobs, keeps partial files, and retry failed, interrupted, or canceled downloads.
- On Windows, active cancellation invokes `taskkill.exe` with separate `/PID`, PID, `/T`, and `/F` arguments to stop the yt-dlp process tree. If tree termination cannot be confirmed, the job is marked Failed rather than falsely reported as Canceled.
- Lets users pause queued or active jobs and resume paused jobs. Active pause stops yt-dlp and its Windows process tree, preserves partial files, and resumes through yt-dlp's `--continue` option.
- Checks the bundled yt-dlp, Deno, FFmpeg, and FFprobe executables at startup and reports their versions or launch failures; Deno is optional for other yt-dlp sites.
- Lets users open completed files with the operating system's registered media handler.
- Starts with the user's Downloads folder under `Yt Save`; the default destination can be changed in the app.
- Uses pinned, SHA-256-verified upstream releases for yt-dlp, Deno, and FFmpeg/FFprobe.

## Windows requirements

The packaged application bundles its runtime dependencies. Developers need:

- Rust stable with the MSVC x64 toolchain
- Rust 1.92 or newer (required by the selected Slint release)
- Python 3.11 or newer for the standard-library dependency fetch and icon scripts
- WiX Toolset 5.0.2 for MSI builds
- Inno Setup 6 for setup EXE builds

No Python, yt-dlp, Deno, FFmpeg, or PATH configuration is required for end users of a correctly assembled installer.

## Build and run

```powershell
rustup toolchain install stable --profile minimal --component rustfmt --component clippy
python scripts/fetch_dependencies.py --output target\package\bin
cargo build --workspace
$env:YT_SAVE_DEPENDENCY_DIR = (Resolve-Path target\package\bin).Path
cargo run -p yt-save
```

The dependency fetcher reads [dependencies.json](dependencies.json), downloads from pinned GitHub release assets, checks each archive's SHA-256, and fails closed on a mismatch. The official yt-dlp executable includes `yt-dlp-ejs`; the app explicitly supplies the bundled Deno path to yt-dlp. See [dependency and license notes](docs/dependencies.md).

## Installers

Ready-to-install Windows builds are published on the [GitHub Releases page](https://github.com/kalabhaftu/yt-save/releases). Each release includes an MSI, an Inno Setup EXE, and a SHA-256 manifest.

Package the application with both source definitions:

- MSI: [packaging/wix/YtSave.wxs](packaging/wix/YtSave.wxs)
- Inno Setup EXE: [packaging/inno/YtSave.iss](packaging/inno/YtSave.iss)

`scripts/build_windows_package.ps1` builds the app, assembles verified dependencies under `target\package\bin`, and invokes WiX and Inno Setup. It reports missing packaging tools rather than producing placeholder installers. The GitHub release workflow installs the tools and publishes installers only after all build steps succeed.

Both installers use the same per-machine program location and per-user application data location. An installer marker prevents MSI and Inno Setup installs from being layered over each other; uninstall the current package before switching installer types. Uninstall removes application files but preserves downloaded media and application data.

## Development

```powershell
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python -m unittest discover -s tests
```

Slint owns the UI event loop on the main thread. Tokio runs metadata extraction, queue work, database operations, and child processes off that thread. SQLite state lives in the user's local application data directory. The architecture and framework choice are documented in [docs/architecture.md](docs/architecture.md).

## Dependency updates

Cargo and GitHub Actions updates are configured in Dependabot. External binaries are deliberately managed separately: review upstream releases and licensing, update version/URL/SHA-256 entries in `dependencies.json`, run `python scripts/fetch_dependencies.py`, then validate yt-dlp, Deno, FFmpeg, and FFprobe versions before publishing. The fetcher does not follow arbitrary manifest hosts.

## Known limitations

- The queue dispatcher starts High priority items before Normal items, then uses the saved queue position; the persisted parallel-download limit ranges from one to eight, defaults to one, and controls new launches. Lowering the limit does not stop jobs already running. Queued and paused items can be moved within their priority group. Playlist selections are stored as an atomic batch in playlist-index order. Startup recovery requeues queued and interrupted items while paused items remain paused. Queued and active work can be paused or canceled; paused work can resume, and failed, interrupted, or canceled work can be retried. These flows still need Windows runtime validation. Completed files can be opened with the default media player, and finished rows can be removed from history without deleting the files.
- Active cancellation uses Windows `taskkill.exe /T /F` to stop the yt-dlp process tree, then reaps the yt-dlp process. If `taskkill.exe` is unavailable, denied, or times out, the app attempts to stop yt-dlp directly and records Failed because it cannot confirm that descendants stopped. If the app exits while a row is Canceling, startup recovery changes it to Failed, retains partial-file progress, and asks the user to check that no media process is still using the files before retrying.
- Active pause uses the same process-tree stop path and records Paused only after yt-dlp has stopped. If the app exits while a row is Pausing, startup recovery marks it Failed with partial-file progress retained and asks the user to check for a lingering process before retrying. Rows already Paused remain paused after restart.
- Completed rows can also be cleared in bulk. Queued and interrupted items are automatically resumed on the next app launch; yt-dlp's `--continue` option resumes available partial files.
- Playlist preview is capped at 50 entries. Entries without a valid positive playlist index cannot be selected. Sync subscriptions are not implemented.
- Existing SQLite databases gain a nullable playlist index plus defaulted download mode, audio format, audio quality, video quality, queue rank, and priority columns on startup. Legacy queue ranks are initialized from row insertion order. A download-directory snapshot column is backfilled for legacy queue rows; retries and recovery keep each job's saved destination.
- Clipboard popup placement uses the active monitor work area and window DPI, and it is suppressed when Windows reports quiet time or a full-screen/presentation state. Popup, clipboard, and tray behavior still need Windows runtime acceptance. Windows toast notifications, scheduler, media library, and subtitle settings screen are not implemented yet. The New download form presets load from SQLite and update only after a new job is successfully queued; each queue item separately retains its own video, audio, and output-folder choices for retries and recovery.
- Audio conversion requires the bundled FFmpeg and FFprobe executables. Original audio downloads use yt-dlp's best audio-only source format without an FFmpeg conversion step.
- ALAC, FLAC, and WAV output cannot restore quality already lost in a lossy source. Bitrate selection applies to lossy conversions, including Vorbis, and is disabled for Original, ALAC, FLAC, and WAV.
- Full YouTube support requires the bundled Deno runtime. The official yt-dlp executable already contains `yt-dlp-ejs`.
- Website support depends on the upstream yt-dlp extractor and each site's current behavior; no app can promise every URL will work.
- Resolution presets use the heights yt-dlp reports. Actual resolution depends on formats available from the site; a requested cap can fail when the extractor reports no eligible format. Audio-only jobs ignore the video quality choice.
- The v0.1.11 MSI and Inno Setup installers passed clean install, app launch, bundled-tool checks, Deno path validation, and uninstall in GitHub Actions. Upgrade testing, visual UI review, and a full in-app download acceptance pass remain pending.

## License

The application is licensed under GPL-3.0-or-later to match the selected Slint open-source license. Third-party components keep their own licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). Do not redistribute builds without reviewing the included notices and the exact FFmpeg configuration.
