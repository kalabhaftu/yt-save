# Feature verification matrix

Statuses describe evidence in the current repository. **Implemented, unverified** means code is present but a full runtime acceptance check has not yet passed.

| Feature | UI | Backend | Persistence | Tests | Runtime verification / limit | Status |
| --- | --- | --- | --- | --- | --- | --- |
| Native Windows window and initial design | Slint shell, queue view, URL field | Tokio runtime and Slint event-loop handoff | — | Slint build in CI | Windows screenshot and DPI validation pending | In progress |
| General yt-dlp URL intake | HTTP(S) input and Analyze/Add actions | yt-dlp structured process calls, no site allowlist | URL saved per job | URL validation and parser unit tests | Bundled yt-dlp downloaded a public direct MP4 via its generic extractor; in-app UI flow pending | Implemented, unverified |
| Metadata extraction | Title/source/details card | `--dump-single-json`, flat playlist inspection | — | Single-item and playlist JSON parser tests | Network/source variation not yet smoke-tested | Implemented, unverified |
| Single download and progress | Queue rows and real progress fields | yt-dlp process, format fallback, progress template | SQLite job state | Progress parsing test and durable state test | End-to-end media download pending | Implemented, unverified |
| Queue persistence and recovery | Queue list | One active worker; startup requeues queued and interrupted items; yt-dlp continues partial files | SQLite WAL state | Database reopen and startup recovery tests | Restart/download recovery still needs an installed-app acceptance pass | Implemented, unverified |
| Deno and yt-dlp-ejs | Engine readiness label | Explicit Deno runtime path when present; official yt-dlp binary contains EJS; other sites work without Deno | Pinned dependency manifest | Engine argument tests; fetcher SHA-256 checks | Bundled versions were launched during packaging; YouTube runtime smoke test pending | Implemented, unverified |
| FFmpeg / FFprobe bundle | Readiness label | `--ffmpeg-location` | Pinned dependency manifest | Fetcher SHA-256 checks | Bundled executables passed version checks; merge test pending | Implemented, unverified |
| Playlist preview/selection/folders | Not implemented | yt-dlp can expand playlist URLs as a single job | Per-job status only | None | No item selection or playlist folder policy | Not started |
| Pause/resume/retry/cancel/reorder | Not implemented | Partial files retained by yt-dlp's continue behavior | State schema supports status/progress | None | No controls; cannot claim these operations | Not started |
| Library/history/search | Not implemented | — | Completed records retained in SQLite | Database test | No library UI/actions | Not started |
| Clipboard assistant/tray/notifications | Not implemented | — | — | None | Not available | Not started |
| Preferences, scheduler, rules, presets | Not implemented | Basic key-value settings table | SQLite settings table | None | No settings UI or scheduler | Not started |
| Subtitle/audio conversion/metadata tools | Not implemented | FFmpeg path is provided to yt-dlp | — | None | Processing choices not exposed | Not started |
| MSI and Inno Setup packages | Installer source | Versioned release script, bundled runtimes, license notices, per-machine install, cross-installer guard | Installers preserve per-user app data | Installer sources compiled locally | MSI and setup EXE compile succeeded; install/upgrade/uninstall test pending | Implemented, unverified |
| GitHub CI, release and dependency updates | Workflow sources | Rust, Python, and installer build jobs | — | CI workflows | Public repository pipeline must pass after publication | In progress |
