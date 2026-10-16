# Yt Save

Yt Save is a native Rust desktop media downloader for Windows 10 and 11. It coordinates the official [yt-dlp](https://github.com/yt-dlp/yt-dlp) executable, so supported sites are determined by yt-dlp's extractors rather than a YouTube-only URL allowlist. YouTube's full format support is handled by the separately bundled Deno runtime and yt-dlp's bundled `yt-dlp-ejs` component.

The repository is in active development. This foundation build provides URL analysis, a persisted single-worker download queue, progress reporting, and a custom Slint interface. Playlist selection, reliable pause/resume, tray operation, clipboard assistance, library/history views, and finished Windows installers are still tracked as unfinished work in [the feature matrix](docs/FEATURE_MATRIX.md).

## Screenshots

Real application screenshots will be added after the first Windows UI validation. No mock screenshots are shown as working software.

![Yt Save download icon](assets/yt-save.png)

## Current features

- Native Rust application with a Slint UI and Tokio background work.
- Accepts HTTP and HTTPS media URLs from any site yt-dlp can extract.
- Reads media metadata and available format counts through yt-dlp JSON output.
- Persists queued items, progress, completion, and failure records in SQLite.
- Uses structured process arguments and yt-dlp progress templates; no shell command construction.
- Stores output in the user's Downloads folder under `Yt Save`.
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

- The queue is single-worker and currently has no interactive pause, retry, cancellation, or reordering controls. Queued and interrupted items are automatically resumed on the next app launch; yt-dlp's `--continue` option resumes available partial files.
- Playlist URLs are handed to yt-dlp as one job; playlist preview, entry selection, dedicated playlist folders, and sync subscriptions are not implemented.
- There is no system tray, clipboard monitor, notification popup, scheduler, media library, or subtitle/conversion settings screen yet.
- Full YouTube support requires the bundled Deno runtime. The official yt-dlp executable already contains `yt-dlp-ejs`.
- Website support depends on the upstream yt-dlp extractor and each site's current behavior; no app can promise every URL will work.
- MSI and Inno Setup source is present, but installers are not marked verified until a Windows install/start/uninstall pass has been performed.

## License

The application is licensed under GPL-3.0-or-later to match the selected Slint open-source license. Third-party components keep their own licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). Do not redistribute builds without reviewing the included notices and the exact FFmpeg configuration.
