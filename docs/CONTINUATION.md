# Yt Save — Continuation Handoff

Updated: 2026-10-22

## Resume instructions

When continuing this project, read this file, the Codex goal, the root `prompt.txt`, and `AGENTS.md` before changing code. The Codex goal is currently **paused**, not complete. Resume that same goal when the user asks to continue; do not replace it with a narrower goal or mark it complete until the master-prompt requirements have been audited against current code and Windows runtime/install evidence.

### Exact saved Codex goal

> Build and ship a production-grade native Rust Windows video/audio downloader in the current Yt Save workspace, following the user's attached master prompt. Support multiple sites through yt-dlp (not YouTube-only), include yt-dlp-ejs and an available JavaScript runtime such as Deno, manage FFmpeg/FFprobe and dependencies, provide a polished Slint/Tokio/SQLite desktop app, package x64 Windows installers, create a transparent app icon, initialize and commit the Git repository, and create/publish a public GitHub repository using the authenticated gh account. Re-read the attached prompt and this goal after compaction.

The root `prompt.txt` is the user's full local master brief. It is deliberately ignored by Git and must remain local and uncommitted, as required by `AGENTS.md`. Its current location is `C:\Users\SlimShady\Documents\Projects\Yt Save\prompt.txt`; check that it remains available and ignored before proceeding. This handoff summarizes the brief but does not replace it. The prompt has 36 sections covering architecture, yt-dlp and binaries, download modes and queue, playlists/channels, cookies, clipboard/tray, library/history, preferences, updates, Windows packaging, GitHub workflows, tests, verification, licenses, and a strict definition of done.

## Product and repository

- Product: **Yt Save**, native Rust desktop app for Windows 10/11 x64.
- UI/runtime/storage: Slint, Tokio, SQLite.
- Download support: accept HTTP(S) media, playlist, and channel links supported by yt-dlp; do not impose a YouTube-only host allowlist or promise every site works.
- Dependencies: pinned official yt-dlp Windows standalone (bundled yt-dlp-ejs), explicitly located Deno runtime, and verified FFmpeg/FFprobe, described in `dependencies.json` and `docs/dependencies.md`.
- Sign-in: explicit compact private WebView2 flow; HTTPS-only, URL-scoped cookie export, DPAPI vault, short-lived plaintext jar, cleanup on exit; never read another browser's cookie store or log cookie values.
- Public GitHub repository: <https://github.com/kalabhaftu/yt-save> (default branch `main`). The existing source repository was already initialized and public. Current committed/pushed snapshot is `5970b47d8adc8f8fc04d1b9ad888a98d122ba9b6` (`Refactor app modules and document continuation status`), based on `2055815 Add per-host queue concurrency limits`.
- The selected user-approved icon is the simple black/white download glyph; transparent PNG/ICO assets are `assets/yt-save.png` and `assets/yt-save.ico`.

## Work in the current working tree

The current accumulated development slice is committed and pushed. The working tree was clean after push; continue from commit `5970b47d8adc8f8fc04d1b9ad888a98d122ba9b6`. Do not reset or discard user files or application data.

- Playlist subscription auto-queue is an explicit per-subscription option, off by default. Newly discovered available entries can be inserted atomically with archive state when opted in; failures roll back the check and queue transaction. Subscription jobs use a sanitized, subscription-specific folder.
- Subscription queueing uses the current form presets, and changing the form settings persists them independently from existing jobs. Retry/resume/recovery retain each job's saved options.
- Orderly shutdown serializes queue additions, pauses active work, and waits for in-flight playlist checks before the process exits.
- `main.rs` initializes bounded rotating application/error logs under the per-user app-data `logs` directory, records startup/shutdown/panic information, and has a stderr fallback. `docs/diagnostics.md` documents the location and privacy limits. Recent engine/playlist/batch failures have privacy-safe tracing events.
- The yt-dlp download command now explicitly uses `--no-overwrites` while retaining `--continue`, protecting completed outputs and related files from accidental replacement.
- Large mixed modules were split along actual responsibilities: tray/shutdown, cookie sign-in, clipboard popup/listener; persistence downloads, settings, subscriptions; persistence tests split into queue/download, settings, and subscriptions. App tests were moved into feature-focused files. `docs/architecture.md` records the module boundaries.
- The app module split was justified by source structure: `app.rs` previously had a multi-thousand-line `run()` wiring unrelated features, while persistence mixed schema, settings, subscription, and queue operations. This is an engineering decision based on coupling and reviewability, not a response made merely to agree with a comment about file size.
- At the pushed snapshot, `app.rs` is about 7,162 lines and `persistence.rs` about 739 lines; the app's view wiring is still large and deserves further measured decomposition. The main reason to split was the roughly four-thousand-line `run()` and unrelated workflow ownership, not a rule that any file over a certain line count must split.
- README, changelog, feature matrix, architecture, and diagnostics docs were updated with the corresponding behavior and limits.
- The user wants implementation work prioritized over optional monitoring/CI work and prefers only the smallest useful verification pass, with focused checks delegated when agents are available. They explicitly authorized helper agents. Keep the code and feature matrix honest; don't claim a full app is done from a compile pass.
- No deletion or project-removal script was made. The user mentioned possibly deleting the project later, then asked to preserve and push the current state; the checkout, `target`, and dependency artifacts remain in place.

## Verification evidence and limits

- The last rough code-only completion estimate was about **65%**. This is an engineering estimate, not a measured metric; it excludes runtime acceptance and does not override the incomplete items below or in the feature matrix.
- `git diff --check` passed before commit.
- Scoped rustfmt checks passed for the persistence parent and app files during extraction. The prior full `cargo fmt --all --check` reported formatting in app/tray/app.rs at an earlier point; later app extraction ran scoped rustfmt.
- The final `cargo fmt --all --check` before push passed (exit 0).
- The latest `cargo check -p yt-save --locked` failed (exit 1): `CloseRequestResponse` is unresolved in `crates/yt-save/src/app/tray.rs` at lines 201 and 214. This is the known next code fix. No files were changed by the check. It remains unresolved in the pushed snapshot because the user requested documentation/preservation only in that step.
- No new runtime download, live sign-in, Windows UI, or installer acceptance was performed in this continuation. Do not claim those results.
- `docs/FEATURE_MATRIX.md` is the current feature-by-feature evidence record. “Implemented, unverified” means exactly that; Windows runtime behavior and clean-system installer acceptance remain incomplete.

## Remaining work

Continue from evidence in the full prompt and feature matrix. At minimum:

1. Review the just-extracted app and persistence modules for correct call sites, visibility, and preserved behavior; fix compile/format diagnostics and run only focused checks needed to establish buildability.
2. Continue implementing missing product requirements rather than treating the current broad feature set as done. The matrix still marks preferences/scheduling/rules/presets and GitHub release readiness as in progress. Custom presets/rules and recurring timed downloads are not implemented; thumbnail-only and metadata-only workflows and some advanced organization options from the master prompt also need an explicit implement-or-limit decision.
3. Complete real Windows acceptance for WebView2 sign-in/cookie refresh and cleanup, non-YouTube extractors, video/audio/subtitle workflows, pause/resume/recovery, clipboard/tray behavior, DPI/focus, and both installers.
4. Audit dependency pins, signatures/digests, licenses/notices, installer scope/upgrade/uninstall behavior, logging/privacy, and preservation of user data against the master prompt.
5. Update the feature matrix only with evidence actually obtained. Commit and push completed code/doc changes to the existing public repository; never add `prompt.txt` to Git.

Do not delete the project, `target/`, downloads, application data, dependency caches, installers, or other generated outputs as part of continuing this goal. No deletion or project-removal script was created in this continuation.
