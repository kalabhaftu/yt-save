# Third-party notices

Yt Save's application source is GPL-3.0-or-later. The binaries acquired through `dependencies.json` retain their own licenses.

| Component | License | Source and notices |
| --- | --- | --- |
| Slint | GPL-3.0-or-later option selected | [Slint licensing](https://slint.dev/faqs) |
| yt-dlp standalone executable | GPL-3.0-or-later; includes `yt-dlp-ejs` and additional dependencies | [yt-dlp licensing](https://github.com/yt-dlp/yt-dlp#licensing), [yt-dlp-ejs](https://github.com/yt-dlp/ejs) |
| Deno runtime | MIT and bundled dependency licenses | [Deno source](https://github.com/denoland/deno) |
| FFmpeg / FFprobe | BtbN LGPL shared build, subject to its actual enabled libraries | [FFmpeg legal guidance](https://ffmpeg.org/legal.html), [BtbN build recipes](https://github.com/BtbN/FFmpeg-Builds) |
| Rust dependencies | Individual crate licenses recorded in Cargo metadata | Run `cargo tree` and `cargo deny` during release review |
| WiX Toolset 5.0.2 | Build-time installer authoring tool; not bundled in the app; MS-RL | [WiX source and license](https://github.com/wixtoolset/wix) |
| Inno Setup | Build-time installer authoring tool; not bundled in the app | [Inno Setup license](https://jrsoftware.org/files/is/license.txt) |
| Yt Save icon | Generated for this project and supplied as a transparent PNG/ICO asset | `assets/yt-save.png`, `assets/yt-save.ico` |

Release assemblers copy license/copying documents available inside the Deno and FFmpeg archives into `target/package/bin/licenses`. The release process must inspect that output and include it with the installer artifacts. Keep yt-dlp and Slint source links available to recipients. The selected yt-dlp and FFmpeg builds must not be changed without a fresh notice/license review.
