# Dependency management and redistribution

`dependencies.json` is the source of truth for the x64 Windows binary set. `scripts/fetch_dependencies.py` downloads the declared release assets over HTTPS, verifies the full archive SHA-256 before extraction, and copies only the runtime files required by the app. It rejects unapproved download hosts and unsafe ZIP paths. The large binaries remain outside Git. The FFmpeg entry pins the GitHub asset ID because BtbN's `latest` URL is mutable.

When `GH_TOKEN` or `GITHUB_TOKEN` is available, the fetcher authenticates requests to GitHub's release-asset API to avoid anonymous API rate limits. The release workflow passes its repository token only during package creation. The authorization header is marked unredirected, so urllib drops it when GitHub sends the download to its signed asset host. Local builds can set either token variable when the public API is rate limited; SHA-256 verification remains mandatory.

| Component | Pinned artifact | Upstream | SHA-256 |
| --- | --- | --- | --- |
| yt-dlp | 2026.08.19 `yt-dlp.exe` | [yt-dlp release](https://github.com/yt-dlp/yt-dlp/releases/tag/2026.08.19) | `66674953fe251b89f4d08c5f0e35e0728679bd67ab3d7d05c0562af101dd3e7a` |
| Deno | 2.9.7 Windows x64 ZIP | [Deno release](https://github.com/denoland/deno/releases/tag/v2.9.7) | `a0c3101b4158d1dfb7d6a78a7bf0f3de80c96bb423c152beec8beb22786f2238` |
| FFmpeg + FFprobe | 8.1 BtbN LGPL shared ZIP | [BtbN build release](https://github.com/BtbN/FFmpeg-Builds/releases/tag/latest) | `d670c3cd76f529d6a4e0de3a6f4732868ad02ddb8f52cfb0428c90aaa08abdee` |

## YouTube JavaScript support

Modern yt-dlp requires both its external JavaScript challenge solver scripts and a supported JavaScript runtime for full YouTube support. The official standalone Windows executable includes `yt-dlp-ejs`; it does not include Deno. The app therefore bundles Deno and always supplies its absolute path through `--js-runtimes deno:<path>`. This also avoids depending on a system-wide PATH entry. See the [yt-dlp EJS guide](https://github.com/yt-dlp/yt-dlp/wiki/EJS) and [Deno installation guide](https://docs.deno.com/runtime/getting_started/installation/).

## License review

- **Application and Slint:** GPL-3.0-or-later. Slint documents GPLv3 and commercial licensing; this project uses the GPL option.
- **yt-dlp executable:** GPL-3.0-or-later. The standalone binary also bundles `yt-dlp-ejs` and associated third-party JavaScript packages. Keep upstream notices and corresponding source links with every release.
- **Deno:** MIT plus notices for bundled dependencies. Retain available notices from the upstream archive/repository.
- **FFmpeg:** the selected artifact name is an LGPL shared build. Build variants and enabled libraries affect obligations; preserve all license/copying files in the archive, ship the DLLs beside the executables, and inspect `ffmpeg -L` and `ffmpeg -buildconf` before a release. FFmpeg upstream links to third-party Windows builds and publishes [legal guidance](https://ffmpeg.org/legal.html).
- **Rust crates:** run `cargo audit` and review license metadata before releases. Keep Cargo.lock committed.

This is an engineering inventory, not a legal opinion. A release must use the exact build and notices that were reviewed, and must not switch to a GPL/nonfree FFmpeg variant without updating the decision and redistribution materials.

## Update process

1. Review upstream release notes and supported runtime requirements.
2. Choose an official release artifact and record its architecture, version, URL, license, and SHA-256 in `dependencies.json`.
3. Verify hashes against the upstream release metadata/checksum source independently of this manifest.
4. Run the fetch script, inspect the archive's notices/configuration, and validate executable versions.
5. Build both installers and run the Windows install/download/uninstall acceptance checklist.
6. Update this document and the feature verification matrix with actual evidence.
