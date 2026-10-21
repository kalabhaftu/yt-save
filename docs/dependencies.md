# Dependency management and redistribution

`dependencies.json` is the source of truth for the x64 Windows binary set, including each component's runtime dependencies. `scripts/fetch_dependencies.py` downloads the declared release assets over HTTPS, verifies the full archive SHA-256 before extraction, and copies only the runtime files required by the app. It rejects unapproved download hosts and unsafe ZIP paths and requires every component to declare runtime dependencies. The large binaries remain outside Git. The FFmpeg entry pins the GitHub asset ID because BtbN's `latest` release and asset change over time.

When `GH_TOKEN` or `GITHUB_TOKEN` is available, the fetcher authenticates requests to GitHub's release-asset API to avoid anonymous API rate limits. The release workflow passes its repository token only during package creation. The authorization header is marked unredirected, so urllib drops it when GitHub sends the download to its signed asset host. Local builds can set either token variable when the public API is rate limited; SHA-256 verification remains mandatory.

| Component | Pinned artifact | Upstream | SHA-256 |
| --- | --- | --- | --- |
| yt-dlp | 2026.08.19 `yt-dlp.exe` | [yt-dlp release](https://github.com/yt-dlp/yt-dlp/releases/tag/2026.08.19) | `66674953fe251b89f4d08c5f0e35e0728679bd67ab3d7d05c0562af101dd3e7a` |
| Deno | 2.9.7 Windows x64 ZIP; the pinned archive contains only `deno.exe` | [Deno v2.9.7 release](https://github.com/denoland/deno/releases/tag/v2.9.7) | `a0c3101b4158d1dfb7d6a78a7bf0f3de80c96bb423c152beec8beb22786f2238` |
| FFmpeg + FFprobe | `n8.1.3-18-g46d899d7a0-20261020`, BtbN LGPL shared ZIP (asset ID `659413383`) | [Pinned BtbN asset](https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/assets/659413383) | `3a584c6017cd9c43716d13bb244e77cd7c89a71e1395bb244f7751a405e3b3fc` |

## YouTube JavaScript support

Modern yt-dlp requires both its external JavaScript challenge solver scripts and a supported JavaScript runtime for full YouTube support. The official standalone Windows executable includes `yt-dlp-ejs`; it does not include Deno. The app therefore bundles Deno and always supplies its absolute path through `--js-runtimes deno:<path>`. This also avoids depending on a system-wide PATH entry. See the [yt-dlp EJS guide](https://github.com/yt-dlp/yt-dlp/wiki/EJS) and [Deno installation guide](https://docs.deno.com/runtime/getting_started/installation/).

## Site sign-in and cookies

The optional sign-in view uses Wry 0.57.0 with the installed Microsoft Edge WebView2 Runtime. It requires a runtime version that supports private WebView2 profiles; if no compatible runtime is present, the app offers [Microsoft's official runtime page](https://developer.microsoft.com/en-us/microsoft-edge/webview2/). The runtime is not bundled because the fixed-version distribution adds substantial size and Windows 11 and most Windows 10 installations already have the Evergreen runtime; see Microsoft's [runtime distribution guidance](https://learn.microsoft.com/en-us/microsoft-edge/webview2/concepts/distribution).

Sign-in is limited to HTTPS media URLs. The user opens the exact media URL entered in the app, signs in inside a compact private view, and explicitly chooses **Save cookies**. Yt Save reads cookies matching only that URL, stores the Netscape jar encrypted with the current Windows user's DPAPI key, and gives yt-dlp a short-lived plaintext jar by file path. Normal process completion refreshes the encrypted cookie vault and attempts to remove the lease; RAII cleanup handles early returns, and the engine reports a removal failure without logging cookie values. Active leases are tracked in-process so cleanup cannot delete a jar while the engine refreshes the encrypted vault. After an unexpected app exit, startup and a background sweep every 60 seconds remove generated staging files older than five minutes after checking that no `yt-dlp.exe` child of the owning app process is still running. Legacy generated leases are removed only when no `yt-dlp.exe` process is active. Cookies are not stored in SQLite, logs, diagnostics, or ordinary backups; Yt Save does not extract cookies from other browser profiles.

Some identity providers block embedded sign-in, and pop-up windows or non-HTTPS redirects are blocked. Cookie export is scoped to the entered URL and may not include a separate host used by an extractor; users can clear saved sign-in from the URL form. The WebView2 and Wry behavior has compile-time and fixture coverage, but live sign-in and download acceptance on installed Windows systems is still pending.

## License review

- **Application and Slint:** GPL-3.0-or-later. Slint documents GPLv3 and commercial licensing; this project uses the GPL option.
- **yt-dlp executable:** GPL-3.0-or-later. The standalone binary also bundles `yt-dlp-ejs` and associated third-party JavaScript packages. Keep upstream notices and corresponding source links with every release.
- **Deno:** the v2.9.7 Windows ZIP contains only `deno.exe`, not a license or bundled-dependency notice file. The matching source-tag license texts for Deno, the bundled TypeScript 6.0.3 compiler, Node type definitions, Undici, and WebGPU are stored in `licenses/deno/` and copied by the fetcher. The source tag does not include a consolidated license bundle for all compiled/transitive Rust and V8 dependencies; this limitation is disclosed in the packaged `UPSTREAM-NOTICES.md`.
- **FFmpeg:** the pinned BtbN build reports `n8.1.3-18-g46d899d7a0-20261020`. Its `ffmpeg -L` output specifies LGPL version 3 or later, `-buildconf` sets `--enable-version3` and does not enable GPL, and the archive's `LICENSE.txt` is the GNU LGPL v3 text. The fetcher copies that license file and all DLLs from the archive's `bin` directory beside `ffmpeg.exe` and `ffprobe.exe`. Build variants and enabled libraries affect obligations; inspect the exact configuration and notices whenever the asset changes. See [FFmpeg legal guidance](https://ffmpeg.org/legal.html).
- **Rust crates:** run `cargo audit` and review license metadata before releases. Keep Cargo.lock committed.
- **Wry:** MIT or Apache-2.0. The optional embedded sign-in uses the operating system's WebView2 Runtime and does not redistribute it; runtime presence and private-profile support are checked before enabling sign-in.

This is an engineering inventory, not a legal opinion. A release must use the exact build and notices that were reviewed, and must not switch to a GPL/nonfree FFmpeg variant without updating the decision and redistribution materials.

## Rust dependency license and source policy

`deny.toml` is the machine-checked policy for the Rust dependency graph used by the x64 Windows build. The CI security workflow runs `cargo deny check licenses sources` against the committed lockfile, including development and build dependencies. It allows the permissive license identifiers present in the current locked Windows graph and the workspace's declared `GPL-3.0-or-later` license. Slint's `GPL-3.0-only` option is permitted only for the current Slint crate family; its commercial `LicenseRef` alternatives are not globally allowed. An added license identifier must be reviewed and explicitly approved before updating the policy.

All registry dependencies must come from the crates.io index. Git dependencies are denied; a future Git dependency must be explicitly allowlisted and pinned to a full revision after review. This source check covers Cargo crates only. The separately bundled yt-dlp, Deno, FFmpeg, and FFprobe binaries remain governed by `dependencies.json`, `THIRD_PARTY_NOTICES.md`, and the binary update procedure above.

## Update process

1. Review upstream release notes and supported runtime requirements.
2. Choose an official release artifact and record its architecture, version, URL, license, runtime dependencies, and SHA-256 in `dependencies.json`.
3. Verify hashes against the upstream release metadata/checksum source independently of this manifest.
4. Run the fetch script, inspect the archive's notices/configuration, and validate executable versions.
5. Build both installers and run the Windows install/download/uninstall acceptance checklist.
6. Update this document and the feature verification matrix with actual evidence.

## In-app yt-dlp updates

The Engine Status panel's **Check & install** action checks only the stable release from the fixed `yt-dlp/yt-dlp` GitHub releases API. The updater accepts a dated stable release tag, requires the exact `yt-dlp.exe` asset URL, and requires GitHub's asset metadata to publish a SHA-256 digest. It downloads over HTTPS with a bounded size and timeout, compares the complete file digest, then runs the staged executable with `--version` and requires an exact match to the release tag.

Verified versions are kept in version-specific folders under the user's LocalAppData application-data directory, with a SHA-256 record beside each executable. Startup rechecks that digest and the exact `--version` before selecting an overlay. If the active overlay fails either check, the app tries the previous version and then falls back to the packaged executable; it changes only version markers and preserves the engine files. The app changes a small active-version marker only after candidate validation; the previous marker and binary remain available for rollback. A failed update leaves the current engine selected. The app will not install an engine update while a queue download is active, and a shared process gate prevents racing yt-dlp metadata extraction or queue dispatch. The new executable becomes available to subsequent work in the same app session.

The runtime updater uses GitHub's published asset SHA-256 digest over authenticated HTTPS; it does not currently verify yt-dlp's detached OpenPGP checksum signature. If GitHub omits the SHA-256 asset digest, the updater fails closed and leaves the selected engine unchanged. This trust boundary is distinct from the repository's build-time dependency manifest, whose digests remain pinned and reviewed in source control. Manual dependency updates must still follow the pinned artifact procedure above.

## In-app application updates

The Application Updates card uses the GitHub Releases API for the repository configured in `branding.json`. Stable accepts only canonical stable SemVer tags that GitHub does not mark as prereleases; Preview also accepts GitHub-marked prereleases, including preview releases that use a plain SemVer tag. It requires both versioned x64 installer assets and GitHub's `sha256:` digest in release metadata before showing a candidate. Downloaded installers are streamed with a maximum size, checked against the declared size and digest, placed in a per-user cache, and checked again before handoff and in the helper after the main process exits. The helper verifies the package filename, cache location, expected size, and digest again before running it. A separate cancellation event and process wait prevent a failed handoff from leaving an installer helper armed to install later.

The updater reads MSI and Inno markers from the current user's HKCU and machine HKLM hives. It refuses missing, malformed, duplicated, or conflicting markers rather than silently switching installer technologies; a current-user Inno install is handed to Inno with `/CURRENTUSER` so it stays in the same scope. MSI is per-machine; Inno Setup supports current-user or all-users mode. The cross-installer guards can see current-user HKCU and machine HKLM only, so a machine installer cannot discover Inno installations registered in other Windows accounts' HKCU. It refuses an update while download workers or persisted active jobs remain, pauses queued work durably, and launches the installer interactively. The selected channel is saved in SQLite. After an installer failure, the helper attempts to relaunch the prior installed path; recovery still depends on that path remaining usable. Automated Windows upgrade, UAC, failure-recovery, and data-retention acceptance remains pending. GitHub's API digest is protected by HTTPS but is not an independent publisher signature. Do not describe application updates as signed until release signing and signature validation are implemented.
