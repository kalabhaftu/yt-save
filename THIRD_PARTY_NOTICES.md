# Third-party notices

Yt Save's application source is GPL-3.0-or-later. The binaries acquired through `dependencies.json` retain their own licenses.

| Component | License | Source and notices |
| --- | --- | --- |
| Slint | GPL-3.0-or-later option selected | [Slint licensing](https://slint.dev/faqs) |
| yt-dlp standalone executable | GPL-3.0-or-later; includes `yt-dlp-ejs` and additional dependencies | [yt-dlp licensing](https://github.com/yt-dlp/yt-dlp#licensing), [yt-dlp-ejs](https://github.com/yt-dlp/ejs) |
| Deno 2.9.7 runtime | Deno MIT; bundled TypeScript compiler Apache-2.0; Node type definitions, Undici, and WebGPU MIT. The pinned source-tree notices are packaged under `bin/licenses/deno/`; that directory discloses unavailable compiled/transitive Rust/V8 notices. | [Deno v2.9.7 source](https://github.com/denoland/deno/tree/v2.9.7), [TypeScript v6.0.3 license](https://github.com/microsoft/TypeScript/blob/v6.0.3/LICENSE.txt) |
| FFmpeg / FFprobe n8.1.3-18-g46d899d7a0-20261020 | LGPL-3.0-or-later shared build. `ffmpeg -L` reports LGPL v3-or-later; `-buildconf` sets `--enable-version3` and does not enable GPL. Archive `LICENSE.txt` is included under `bin/licenses/ffmpeg/`. | [FFmpeg legal guidance](https://ffmpeg.org/legal.html), [pinned BtbN asset](https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/assets/659413383) |
| Rust dependencies | Individual crate licenses recorded in Cargo metadata | Run `cargo tree` and `cargo deny` during release review |
| Wry 0.57.0 embedded WebView | MIT or Apache-2.0; Windows uses the installed Microsoft Edge WebView2 Runtime, which is not bundled | [Wry source and license](https://github.com/tauri-apps/wry), [WebView2 Runtime](https://developer.microsoft.com/en-us/microsoft-edge/webview2/) |
| WiX Toolset 5.0.2 | Build-time installer authoring tool; not bundled in the app; MS-RL | [WiX source and license](https://github.com/wixtoolset/wix) |
| Inno Setup | Build-time installer authoring tool; not bundled in the app | [Inno Setup license](https://jrsoftware.org/files/is/license.txt) |
| Yt Save icon | Generated for this project and supplied as a transparent PNG/ICO asset | `assets/yt-save.png`, `assets/yt-save.ico` |

The inspected Deno v2.9.7 Windows ZIP contains only `deno.exe`, with no license files or consolidated bundled-dependency notice. The matching license texts for Deno, the bundled TypeScript 6.0.3 compiler, Node type definitions, Undici, and WebGPU are copied into `bin/licenses/deno/`. `bin/licenses/deno/UPSTREAM-NOTICES.md` discloses that the Deno source does not provide a complete notice bundle for its compiled/transitive Rust and V8 dependencies; the package does not claim those notices are exhaustive. The source tag and dependency lockfile are linked there for review.

Release assemblers copy license/copying documents available inside the FFmpeg archive into `target/package/bin/licenses/ffmpeg`; the pinned build's `LICENSE.txt` is LGPLv3. Keep yt-dlp and Slint source links available to recipients. The selected yt-dlp and FFmpeg builds must not be changed without a fresh notice/license review.
