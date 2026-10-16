# Contributing

## Development setup

Install stable Rust with the MSVC x64 target, then use the commands in the README. The app can launch without the runtime bundle to inspect its dependency status; downloading requires the verified yt-dlp and Deno files.

## Changes

- Keep UI declarations in Slint and backend behavior in Rust modules.
- Do not add a site allowlist: yt-dlp owns extractor support.
- Do not claim functionality from a visible control alone; update the feature matrix with test and runtime evidence.
- Keep external binaries out of Git. Update `dependencies.json` and notices when binary inputs change.
- Pass URL values as individual process arguments and avoid shell invocation.
- Run `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, and `python -m unittest discover -s tests` before submitting.

## Pull requests

Describe user-visible behavior, dependency changes, platform validation, and known limitations. Include screenshots only from an actual running application.
