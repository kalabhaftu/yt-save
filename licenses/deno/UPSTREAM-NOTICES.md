# Deno v2.9.7 upstream notice coverage

The official Windows x86_64 release ZIP contains only `deno.exe`; it does not
contain license or third-party notice files. The package includes the license
texts present in the matching Deno source tag for the Deno project, bundled
TypeScript compiler, Node type definitions, Undici, and the WebGPU extension.

| Packaged file | Upstream v2.9.7 source |
| --- | --- |
| `Deno-LICENSE.md` | <https://github.com/denoland/deno/blob/v2.9.7/LICENSE.md> |
| `TypeScript-Apache-2.0-LICENSE.txt` | <https://github.com/microsoft/TypeScript/blob/v6.0.3/LICENSE.txt> (the bundled compiler reports TypeScript 6.0.3) |
| `Node-types-MIT-LICENSE.txt` | <https://github.com/denoland/deno/blob/v2.9.7/cli/tsc/dts/node/LICENSE> |
| `Undici-MIT-LICENSE.txt` | <https://github.com/denoland/deno/blob/v2.9.7/cli/tsc/dts/node/undici/LICENSE> |
| `WebGPU-MIT-LICENSE.md` | <https://github.com/denoland/deno/blob/v2.9.7/ext/webgpu/LICENSE.md> |

The Deno source tag does not provide one consolidated notice bundle for all
compiled and statically linked Rust/V8 dependencies. This package does not claim
that the files above enumerate every license applicable to those transitive
dependencies. The pinned source and dependency lockfile are available at
<https://github.com/denoland/deno/tree/v2.9.7>.
