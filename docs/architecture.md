# Architecture and framework decision

## Product identity

The temporary product identity is centralized as **Yt Save**. The Rust package/executable is `yt-save`, the Windows application folder is `Yt Save`, the application data folder is selected through the `directories` crate, and the installer sources carry the same name and version. Branding is intentionally kept out of dependency and queue logic.

## UI framework

The app uses Slint with Rust. Slint supplies a declarative, native desktop UI without an embedded browser runtime and provides Windows desktop support. Its Rust components are created and accessed on the main event-loop thread. Long-running work is kept outside that thread and posted back with `slint::invoke_from_event_loop`.

The selected Slint 1.18 release requires Rust 1.92 or newer. The workspace declares that minimum and pins the stable channel through `rust-toolchain.toml`.

The project selects Slint's GPL-3.0-or-later open-source license and therefore licenses the application code GPL-3.0-or-later. A later switch to a commercial Slint license would require a deliberate licensing review and repository update.

## Runtime boundaries

- **UI:** Slint components and view models only; no process implementation inside `.slint` files.
- **Async work:** Tokio multi-thread runtime for URL analysis, SQLite I/O, queue work, and child process output.
- **Download engine:** the official `yt-dlp.exe`, invoked with structured arguments and no shell.
- **YouTube JavaScript:** Deno is an explicitly located, pinned runtime when available. The official yt-dlp executable bundles `yt-dlp-ejs`; yt-dlp can still process other supported sites without Deno.
- **Media processing:** bundled FFmpeg and FFprobe are passed to yt-dlp through `--ffmpeg-location`.
- **Persistence:** SQLite with WAL mode for durable queue state and settings.
- **Output location:** a user-selected default folder is stored in `app_settings`; every queued item stores its own resolved folder so a later preference change cannot redirect queued, paused, interrupted, or retried work. Playlist-title subfolders are created inside that per-job destination.
- **Windows packaging:** WiX Toolset 5.0.2 for MSI and Inno Setup 6 for EXE installers.

## First vertical slice

The implemented flow accepts any HTTP(S) URL, stores it as a queue record, obtains metadata with yt-dlp JSON output, runs a download, parses machine-readable progress, and stores completion or failure. The app does not maintain a site allowlist. yt-dlp decides whether a URL has a usable extractor. Download destinations are checked as existing directories and canonicalized before a queue item is persisted.

Pause, resume, retries, playlist item selection, process lifetime jobs, tray integration, and complete crash recovery remain open items. See the feature matrix; a configured button or installer file is not evidence that the feature works.
