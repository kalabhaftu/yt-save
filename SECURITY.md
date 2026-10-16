# Security policy

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting for this repository. Do not post unpatched security issues in public issue comments.

## Security boundaries

- The UI accepts only HTTP and HTTPS URLs and passes them as process arguments, never through a shell.
- yt-dlp, Deno, and FFmpeg are external executables. Their versions, sources, licenses, and SHA-256 values are pinned in `dependencies.json`.
- The binary fetcher verifies archives before extraction, rejects unexpected hosts, and checks ZIP paths against traversal.
- Downloaded media metadata is treated as untrusted data and is not interpreted as commands.
- Release builds should run `cargo audit`, scope GitHub token permissions narrowly, and keep signing secrets away from pull requests.

Security is under active development. Clipboard monitoring, browser cookies, update installation, and authentication features are not enabled in the current foundation build.
