# Logs and troubleshooting

Yt Save writes application and error logs, including panic locations, to `logs\yt-save.log` inside the same per-user local application-data directory that contains `library.sqlite3`. On Windows, open that application-data directory under `%LOCALAPPDATA%`. The active log is limited to 1 MiB, with up to three rotated files named `yt-save.log.1` through `yt-save.log.3`.

The default log level is `info`. To collect more operational detail when launching from PowerShell, set `$env:RUST_LOG = "yt_save=debug"` before starting the app.

The app does not log raw yt-dlp output or cookie values. Logs can still contain ordinary operating-system error details, so review them before sharing.
