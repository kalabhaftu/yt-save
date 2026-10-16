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
- **New download presets:** mode, video quality, audio format, and audio bitrate are stored as typed string values in `app_settings`, independently from each job's persisted selections. The app loads them at startup and updates them only after a new queue insertion succeeds (for a playlist batch, at least one item must save); retry, resume, and recovery paths do not write these defaults.
- **Clipboard URL review and tray:** Windows clipboard monitoring is off by default and starts only after an explicit user choice. A dedicated message-loop thread forwards only a locally parsed, single HTTP(S) URL with the foreground monitor's work area and Windows notification state; raw clipboard text is not stored. When notifications are allowed, a Slint popup appears above the taskbar work area without taking keyboard focus. It expires after 12 seconds, and metadata lookup or queueing begins only after an explicit button click. Add-to-queue inspects playlist state first and presents playlist entry selection. The Slint system tray menu exposes window, queue, download-folder, and orderly-quit actions; close-to-tray and minimize-to-tray preferences are off by default and persisted in SQLite. Windows runtime acceptance remains pending.
- **Windows packaging:** WiX Toolset 5.0.2 for MSI and Inno Setup 6 for EXE installers.

## First vertical slice

The implemented flow accepts any HTTP(S) URL, obtains metadata with yt-dlp JSON output, runs a download, parses machine-readable progress, and stores completion or failure. The app does not maintain a site allowlist. yt-dlp decides whether a URL has a usable extractor. Download destinations are checked as existing directories and canonicalized before a queue item is persisted. Queue actions inspect playlist URLs before persistence so users can select entries rather than accidentally queueing a whole playlist.

Pause, resume, retry, cancel, playlist item selection, tray controls, and crash recovery have persisted implementations. They still need the Windows runtime acceptance checks listed in the feature matrix; the clipboard popup also awaits focus and monitor-placement validation there.
