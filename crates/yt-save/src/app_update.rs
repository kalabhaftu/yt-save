//! Verified application update discovery and handoff.

use std::{
    collections::HashMap,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    process::Child,
    sync::{Mutex, OnceLock},
    time::Duration,
};

#[cfg(windows)]
use std::{
    fs::OpenOptions,
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use url::Url;
use uuid::Uuid;

const GITHUB_API: &str = "https://api.github.com";
const MAX_RELEASE_PAGES: usize = 5;
const RELEASES_PER_PAGE: usize = 100;
const MAX_RELEASE_PAGE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_RELEASES_BYTES: u64 = 8 * 1024 * 1024;
const MAX_TITLE_BYTES: usize = 1024;
const MAX_NOTES_BYTES: usize = 128 * 1024;
const MAX_INSTALLER_BYTES: u64 = 512 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const HELPER_MARKER: &str = "--yt-save-update-helper";
const HELPER_PARENT: &str = "--parent-pid";
const HELPER_KIND: &str = "--installer-kind";
const HELPER_INSTALLER: &str = "--installer-path";
const HELPER_APP: &str = "--app-path";
const HELPER_SIZE: &str = "--expected-size";
const HELPER_SHA256: &str = "--expected-sha256";
const HELPER_CACHE: &str = "--cache-dir";
const HELPER_VERSION: &str = "--expected-version";
const HELPER_READY_EVENT: &str = "--ready-event";
const HELPER_CANCEL_EVENT: &str = "--cancel-event";
const HELPER_READY_TIMEOUT_MS: u32 = 10_000;
const HELPER_PARENT_WAIT_TIMEOUT_MS: u32 = 5 * 60 * 1_000;
const HELPER_CANCEL_GRACE: Duration = Duration::from_secs(1);
const HELPER_TERMINATE_WAIT: Duration = Duration::from_secs(5);
const HELPER_STOP_POLL: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppUpdateChannel {
    Stable,
    Preview,
}

impl AppUpdateChannel {
    pub fn database_value(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::Preview => "preview",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Stable => "Stable",
            Self::Preview => "Preview",
        }
    }

    pub fn from_database_value(value: Option<&str>) -> Self {
        match value {
            Some("preview") => Self::Preview,
            _ => Self::Stable,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckOutcome {
    UpToDate { current_version: Version, release: AppRelease },
    UpdateAvailable { current_version: Version, release: AppRelease },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppRelease {
    pub channel: AppUpdateChannel,
    pub version: Version,
    pub tag: String,
    pub title: String,
    pub body: String,
    pub html_url: Url,
    pub msi: InstallerAsset,
    pub setup_exe: InstallerAsset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallerKind {
    Msi,
    SetupExe,
}

impl InstallerKind {
    fn database_value(self) -> &'static str {
        match self {
            Self::Msi => "msi",
            Self::SetupExe => "setup-exe",
        }
    }

    fn from_value(value: &str) -> Result<Self> {
        match value {
            "msi" => Ok(Self::Msi),
            "setup-exe" => Ok(Self::SetupExe),
            _ => bail!("unsupported installer kind in updater helper arguments"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallerAsset {
    pub name: String,
    pub download_url: Url,
    pub size: u64,
    pub sha256: [u8; 32],
}

#[derive(Debug, Deserialize)]
struct RawRelease {
    tag_name: String,
    name: Option<String>,
    body: Option<String>,
    html_url: String,
    draft: bool,
    prerelease: bool,
    assets: Vec<RawAsset>,
}

#[derive(Debug, Deserialize)]
struct RawAsset {
    name: String,
    browser_download_url: String,
    digest: Option<String>,
    size: u64,
}

struct PartialFile(PathBuf);

impl Drop for PartialFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[derive(Clone, Debug)]
struct VerifiedInstallerMetadata {
    name: String,
    version: Version,
    size: u64,
    sha256: [u8; 32],
}

static VERIFIED_INSTALLERS: OnceLock<Mutex<HashMap<PathBuf, VerifiedInstallerMetadata>>> =
    OnceLock::new();

pub struct UpdateHelper {
    child: Child,
    cancel_event_name: String,
    ready: bool,
    #[cfg(windows)]
    helper_path: PathBuf,
}

impl UpdateHelper {
    pub fn is_ready(&self) -> bool {
        self.ready
    }

    /// Signals cancellation, then confirms the helper process has exited before the app resumes.
    pub fn cancel_and_wait(&mut self) -> Result<()> {
        let cancel_signal = signal_helper_cancel_event(&self.cancel_event_name);
        let canceled =
            cancel_signal.is_ok() && wait_for_child_exit(&mut self.child, HELPER_CANCEL_GRACE);
        if !canceled {
            let kill_result = match self.child.try_wait() {
                Ok(Some(_)) => Ok(()),
                Ok(None) | Err(_) => self.child.kill(),
            };
            if !wait_for_child_exit(&mut self.child, HELPER_TERMINATE_WAIT) {
                if let Err(kill_error) = kill_result {
                    if let Err(cancel_error) = cancel_signal {
                        return Err(kill_error).context(format!(
                            "the updater helper could not be canceled through its event ({cancel_error:#})"
                        ));
                    }
                    return Err(kill_error)
                        .context("the updater helper could not be terminated after cancellation");
                }
                bail!("the updater helper did not exit after cancellation and termination");
            }
        }
        self.child.wait().context("wait for updater helper cancellation")?;
        #[cfg(windows)]
        let _ = fs::remove_file(&self.helper_path);
        Ok(())
    }
}

fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => return false,
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(HELPER_STOP_POLL);
    }
}

/// Checks the public GitHub Releases API for the newest release admitted by `channel`.
/// Preview includes stable releases as well as SemVer prereleases.
pub async fn check_latest(
    channel: AppUpdateChannel,
    current_version: &str,
) -> Result<CheckOutcome> {
    let current_version =
        parse_version(current_version).context("parse the installed app version")?;
    let client = github_client()?;
    let mut releases = Vec::new();
    let mut total_bytes = 0_u64;

    for page in 1..=MAX_RELEASE_PAGES {
        let response = client
            .get(format!(
                "{GITHUB_API}/repos/{}/{}/releases?per_page={RELEASES_PER_PAGE}&page={page}",
                env!("YT_SAVE_GITHUB_OWNER"),
                env!("YT_SAVE_GITHUB_REPOSITORY")
            ))
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .send()
            .await
            .context("request Yt Save release metadata from GitHub")?
            .error_for_status()
            .context("GitHub rejected the Yt Save release metadata request")?;
        let bytes = read_bounded_response(response, MAX_RELEASE_PAGE_BYTES)
            .await
            .context("read Yt Save GitHub release metadata")?;
        total_bytes = total_bytes
            .checked_add(bytes.len() as u64)
            .context("GitHub release metadata size overflowed")?;
        if total_bytes > MAX_RELEASES_BYTES {
            bail!("GitHub release metadata exceeded the updater size limit");
        }
        let page_releases: Vec<RawRelease> =
            serde_json::from_slice(&bytes).context("parse Yt Save GitHub release metadata")?;
        let page_count = page_releases.len();
        releases.extend(page_releases);
        if page_count < RELEASES_PER_PAGE {
            break;
        }
        if page == MAX_RELEASE_PAGES {
            bail!("the release list exceeded the updater's bounded scan limit");
        }
    }

    let release = select_release(channel, releases.iter())?;
    let outcome = if release.version.cmp_precedence(&current_version).is_gt() {
        CheckOutcome::UpdateAvailable { current_version, release }
    } else {
        CheckOutcome::UpToDate { current_version, release }
    };
    Ok(outcome)
}

/// Downloads a release installer to the supplied per-user cache after validating its GitHub
/// metadata and SHA-256 digest. A verified existing file is reused.
pub async fn download_verified_installer(
    release: &AppRelease,
    kind: InstallerKind,
    cache_dir: &Path,
) -> Result<PathBuf> {
    let asset = validated_asset(release, kind)?;
    ensure_user_cache_directory(cache_dir, true)?;
    let final_path = installer_cache_path(cache_dir, &asset.name)?;
    if fs::symlink_metadata(&final_path).is_ok() {
        verify_cached_installer(release, kind, &final_path)
            .context("an existing cached installer failed verification; it was preserved")?;
        return Ok(final_path);
    }

    let client = github_client()?;
    let response = client
        .get(asset.download_url.clone())
        .send()
        .await
        .context("download the Yt Save installer from GitHub")?
        .error_for_status()
        .context("GitHub installer download failed")?;
    if response.content_length().is_some_and(|length| length != asset.size) {
        bail!("GitHub returned an installer with an unexpected size");
    }

    let partial_path = cache_dir.join(format!(".yt-save-update-{}.partial", Uuid::new_v4()));
    let partial = PartialFile(partial_path.clone());
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial_path)
        .await
        .context("create a unique partial installer in the update cache")?;
    let mut hasher = Sha256::new();
    let mut received = 0_u64;
    let mut response = response;
    while let Some(chunk) = response.chunk().await.context("read installer download")? {
        received = received
            .checked_add(chunk.len() as u64)
            .context("installer download size overflowed")?;
        if received > asset.size || received > MAX_INSTALLER_BYTES {
            bail!("the installer download exceeded its declared size");
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await.context("write partial installer download")?;
    }
    if received != asset.size {
        bail!("the installer download was incomplete");
    }
    let actual: [u8; 32] = hasher.finalize().into();
    if actual != asset.sha256 {
        bail!("the installer SHA-256 digest does not match GitHub release metadata");
    }
    file.sync_all().await.context("flush the verified installer download")?;
    drop(file);

    match fs::hard_link(&partial_path, &final_path) {
        Ok(()) => {}
        Err(_error) if fs::symlink_metadata(&final_path).is_ok() => {
            verify_cached_installer(release, kind, &final_path).with_context(|| {
                format!("a concurrent installer cache entry exists at {}", final_path.display())
            })?;
        }
        Err(error) => return Err(error).context("finalize the verified installer download"),
    }
    drop(partial);
    verify_cached_installer(release, kind, &final_path)
        .context("the finalized installer failed its post-download verification")?;
    Ok(final_path)
}

/// Rechecks an installer immediately before it is handed to the update helper.
pub fn verify_cached_installer(
    release: &AppRelease,
    kind: InstallerKind,
    path: &Path,
) -> Result<()> {
    let asset = validated_asset(release, kind)?;
    if path.file_name().and_then(|name| name.to_str()) != Some(asset.name.as_str()) {
        bail!("cached installer filename does not match its release asset");
    }
    let parent = path.parent().context("locate cached installer directory")?;
    validate_existing_cache_directory(parent)?;
    let metadata = fs::symlink_metadata(path).context("inspect cached installer")?;
    if metadata.file_type().is_symlink() || is_reparse_point(&metadata) || !metadata.is_file() {
        bail!("refusing to execute a cached installer that is not a regular file");
    }
    if metadata.len() != asset.size || metadata.len() == 0 || metadata.len() > MAX_INSTALLER_BYTES {
        bail!("cached installer size does not match GitHub release metadata");
    }
    let mut file = File::open(path).context("open cached installer for SHA-256 verification")?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut read_bytes = 0_u64;
    loop {
        let count = file.read(&mut buffer).context("read cached installer")?;
        if count == 0 {
            break;
        }
        read_bytes =
            read_bytes.checked_add(count as u64).context("cached installer size overflowed")?;
        if read_bytes > asset.size {
            bail!("cached installer grew while it was being verified");
        }
        hasher.update(&buffer[..count]);
    }
    let actual: [u8; 32] = hasher.finalize().into();
    if actual != asset.sha256 || read_bytes != asset.size {
        bail!("cached installer failed SHA-256 verification");
    }
    let canonical_path = path.canonicalize().context("resolve verified installer cache path")?;
    let registry = VERIFIED_INSTALLERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry =
        registry.lock().map_err(|_| anyhow::anyhow!("verified installer state is unavailable"))?;
    if registry.len() >= 64 && !registry.contains_key(&canonical_path) {
        registry.clear();
    }
    registry.insert(
        canonical_path,
        VerifiedInstallerMetadata {
            name: asset.name.clone(),
            version: release.version.clone(),
            size: asset.size,
            sha256: asset.sha256,
        },
    );
    Ok(())
}

/// Detects the installation format using the package markers written under the branded HKLM key.
/// Exactly one marker must be present and equal to DWORD 1.
pub fn detect_installed_installer() -> Result<InstallerKind> {
    #[cfg(windows)]
    {
        let key = format!("Software\\{}", env!("YT_SAVE_BUNDLE_IDENTIFIER"));
        let msi = registry_marker(&key, "InstalledByMSI")?;
        let inno = registry_marker(&key, "InstalledByInno")?;
        installer_kind_from_markers(msi, inno)
    }
    #[cfg(not(windows))]
    {
        bail!("installed installer detection is supported only on Windows")
    }
}

/// Copies the current executable to a uniquely named updater helper in the user cache and starts
/// it hidden. The helper waits for the current process before opening the installer.
pub fn launch_update_helper(
    installer: &Path,
    kind: InstallerKind,
    app_path: &Path,
    cache_dir: &Path,
) -> Result<UpdateHelper> {
    #[cfg(windows)]
    {
        let cache_dir = ensure_user_cache_directory(cache_dir, true)?;
        let app_path = app_path.canonicalize().context("resolve installed app executable")?;
        ensure_regular_executable_path(&app_path).context("validate installed app executable")?;
        let installer = installer.canonicalize().context("resolve cached installer")?;
        let verified = lookup_verified_installer(&installer)?;
        ensure_helper_installer_path(&installer, &cache_dir, kind, &verified)?;
        verify_installer_file(&installer, &verified.name, verified.size, &verified.sha256)?;
        let helper_path = cache_dir.join(format!("yt-save-update-helper-{}.exe", Uuid::new_v4()));
        let current_exe =
            std::env::current_exe().context("locate the running Yt Save executable")?;
        copy_executable_create_new(&current_exe, &helper_path)?;
        let parent_pid = std::process::id();
        let ready_event_name = format!(r"Local\YtSave-Update-Ready-{}", Uuid::new_v4());
        let cancel_event_name = format!(r"Local\YtSave-Update-Cancel-{}", Uuid::new_v4());
        let mut ready_event_wide: Vec<u16> =
            ready_event_name.encode_utf16().chain(Some(0)).collect();
        let mut cancel_event_wide: Vec<u16> =
            cancel_event_name.encode_utf16().chain(Some(0)).collect();
        use windows_sys::Win32::{
            Foundation::{CloseHandle, WAIT_OBJECT_0},
            System::Threading::{CreateEventW, SetEvent, WaitForSingleObject},
        };
        let ready_event =
            unsafe { CreateEventW(std::ptr::null(), 1, 0, ready_event_wide.as_mut_ptr()) };
        if ready_event.is_null() {
            let _ = fs::remove_file(&helper_path);
            bail!("could not create updater helper readiness event");
        }
        let cancel_event =
            unsafe { CreateEventW(std::ptr::null(), 1, 0, cancel_event_wide.as_mut_ptr()) };
        if cancel_event.is_null() {
            unsafe { CloseHandle(ready_event) };
            let _ = fs::remove_file(&helper_path);
            bail!("could not create updater helper cancellation event");
        }
        use std::os::windows::process::CommandExt;
        let spawn_result = Command::new(&helper_path)
            .arg(HELPER_MARKER)
            .arg(HELPER_PARENT)
            .arg(parent_pid.to_string())
            .arg(HELPER_KIND)
            .arg(kind.database_value())
            .arg(HELPER_INSTALLER)
            .arg(&installer)
            .arg(HELPER_APP)
            .arg(&app_path)
            .arg(HELPER_SIZE)
            .arg(verified.size.to_string())
            .arg(HELPER_SHA256)
            .arg(hex_digest(&verified.sha256))
            .arg(HELPER_CACHE)
            .arg(&cache_dir)
            .arg(HELPER_VERSION)
            .arg(verified.version.to_string())
            .arg(HELPER_READY_EVENT)
            .arg(&ready_event_name)
            .arg(HELPER_CANCEL_EVENT)
            .arg(&cancel_event_name)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .creation_flags(0x0800_0000)
            .spawn()
            .context("start hidden updater helper");
        let mut child = match spawn_result {
            Ok(child) => child,
            Err(error) => {
                unsafe { CloseHandle(ready_event) };
                unsafe { CloseHandle(cancel_event) };
                let _ = fs::remove_file(&helper_path);
                return Err(error);
            }
        };
        let ready =
            unsafe { WaitForSingleObject(ready_event, HELPER_READY_TIMEOUT_MS) } == WAIT_OBJECT_0;
        if !ready {
            // Readiness was never acknowledged, so the helper may not have opened the
            // cancellation event. Signal it as a best effort, then terminate unconditionally.
            unsafe { SetEvent(cancel_event) };
            unsafe { CloseHandle(ready_event) };
            unsafe { CloseHandle(cancel_event) };
            let _ = child.kill();
            if wait_for_child_exit(&mut child, HELPER_TERMINATE_WAIT) {
                child.wait().context("confirm updater helper stopped after readiness timeout")?;
                let _ = fs::remove_file(&helper_path);
                bail!("updater helper did not open the parent process handle in time");
            }
            return Ok(UpdateHelper { child, cancel_event_name, ready: false, helper_path });
        }
        unsafe { CloseHandle(ready_event) };
        unsafe { CloseHandle(cancel_event) };
        Ok(UpdateHelper { child, cancel_event_name, ready: true, helper_path })
    }
    #[cfg(not(windows))]
    {
        let _ = (installer, kind, app_path, cache_dir);
        bail!("installer handoff is supported only on Windows")
    }
}

/// Runs the hidden helper entry point. `args` are the process arguments after the executable path.
pub fn run_update_helper(args: &[String]) -> Result<i32> {
    let parsed = parse_helper_args(args)?;
    #[cfg(windows)]
    {
        run_helper_windows(parsed)
    }
    #[cfg(not(windows))]
    {
        let _ = parsed;
        bail!("installer update helper is supported only on Windows")
    }
}

fn github_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(concat!("YtSave/", env!("CARGO_PKG_VERSION")))
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= 5 {
                return attempt.error("too many redirects while contacting GitHub");
            }
            let url = attempt.url();
            let allowed_host = matches!(
                url.host_str(),
                Some("api.github.com")
                    | Some("github.com")
                    | Some("release-assets.githubusercontent.com")
                    | Some("objects.githubusercontent.com")
            );
            if url.scheme() == "https"
                && url.port().is_none()
                && url.username().is_empty()
                && url.password().is_none()
                && allowed_host
            {
                attempt.follow()
            } else {
                attempt.error("redirect left the approved GitHub hosts")
            }
        }))
        .build()
        .context("create the GitHub update client")
}

async fn read_bounded_response(mut response: reqwest::Response, maximum: u64) -> Result<Vec<u8>> {
    if response.content_length().is_some_and(|length| length > maximum) {
        bail!("GitHub returned metadata larger than the updater limit");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.context("read GitHub response")? {
        if bytes.len() as u64 + chunk.len() as u64 > maximum {
            bail!("GitHub response exceeded the updater size limit");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn select_release<'a>(
    channel: AppUpdateChannel,
    releases: impl IntoIterator<Item = &'a RawRelease>,
) -> Result<AppRelease> {
    let mut candidates = Vec::new();
    for raw in releases {
        if raw.draft {
            continue;
        }
        let version = parse_tag(&raw.tag_name)?;
        // GitHub's prerelease flag is an intentional channel decision and may be used with a
        // plain SemVer tag while a release is in preview. SemVer still orders versions; stable
        // users must avoid either kind of prerelease marker.
        if channel == AppUpdateChannel::Stable && (raw.prerelease || !version.pre.is_empty()) {
            continue;
        }
        candidates.push((version, raw));
    }
    let (_, selected) = candidates
        .into_iter()
        .max_by(|(left, left_release), (right, right_release)| {
            left.cmp_precedence(right)
                .then_with(|| left_release.tag_name.cmp(&right_release.tag_name))
        })
        .context("GitHub has no release for the selected update channel")?;
    parse_release(selected, channel)
}

fn parse_release(raw: &RawRelease, channel: AppUpdateChannel) -> Result<AppRelease> {
    let version = parse_tag(&raw.tag_name)?;
    let title = raw.name.as_deref().context("release title is missing")?;
    if title.trim().is_empty() || title.len() > MAX_TITLE_BYTES {
        bail!("release title is empty or exceeds the updater limit");
    }
    let body = raw.body.as_deref().context("release notes are missing")?;
    if body.len() > MAX_NOTES_BYTES {
        bail!("release notes exceed the updater limit");
    }
    let expected_release_url = format!(
        "https://github.com/{}/{}/releases/tag/{}",
        env!("YT_SAVE_GITHUB_OWNER"),
        env!("YT_SAVE_GITHUB_REPOSITORY"),
        raw.tag_name
    );
    if raw.html_url != expected_release_url {
        bail!("release page URL does not match the branded GitHub repository and tag");
    }
    let html_url = Url::parse(&expected_release_url).context("parse release page URL")?;
    if raw.assets.len() > 100 {
        bail!("release contains too many assets");
    }
    let msi_name = format!("{}-{}-x64.msi", env!("YT_SAVE_PACKAGE_NAME"), version);
    let exe_name = format!("{}-{}-x64-setup.exe", env!("YT_SAVE_PACKAGE_NAME"), version);
    let msi = parse_asset(raw, &msi_name)?;
    let setup_exe = parse_asset(raw, &exe_name)?;
    Ok(AppRelease {
        channel,
        version,
        tag: raw.tag_name.clone(),
        title: title.to_owned(),
        body: body.to_owned(),
        html_url,
        msi,
        setup_exe,
    })
}

fn parse_asset(raw: &RawRelease, expected_name: &str) -> Result<InstallerAsset> {
    let mut matches = raw.assets.iter().filter(|asset| asset.name == expected_name);
    let asset = matches.next().with_context(|| format!("release is missing {expected_name}"))?;
    if matches.next().is_some() {
        bail!("release contains duplicate installer asset {expected_name}");
    }
    if asset.size == 0 || asset.size > MAX_INSTALLER_BYTES {
        bail!("installer {expected_name} has an invalid size");
    }
    let expected_url = format!(
        "https://github.com/{}/{}/releases/download/{}/{}",
        env!("YT_SAVE_GITHUB_OWNER"),
        env!("YT_SAVE_GITHUB_REPOSITORY"),
        raw.tag_name,
        expected_name
    );
    if asset.browser_download_url != expected_url {
        bail!("installer URL does not match the exact branded GitHub release asset path");
    }
    let download_url = Url::parse(&expected_url).context("parse GitHub installer URL")?;
    let sha256 = parse_sha256_digest(
        asset.digest.as_deref().context("GitHub installer SHA-256 digest is missing")?,
    )?;
    Ok(InstallerAsset { name: expected_name.to_owned(), download_url, size: asset.size, sha256 })
}

fn validated_asset(release: &AppRelease, kind: InstallerKind) -> Result<&InstallerAsset> {
    validate_typed_release(release)?;
    let asset = match kind {
        InstallerKind::Msi => &release.msi,
        InstallerKind::SetupExe => &release.setup_exe,
    };
    let expected_name = match kind {
        InstallerKind::Msi => {
            format!("{}-{}-x64.msi", env!("YT_SAVE_PACKAGE_NAME"), release.version)
        }
        InstallerKind::SetupExe => {
            format!("{}-{}-x64-setup.exe", env!("YT_SAVE_PACKAGE_NAME"), release.version)
        }
    };
    if asset.name != expected_name || asset.size == 0 || asset.size > MAX_INSTALLER_BYTES {
        bail!("installer metadata is inconsistent with the selected release");
    }
    let expected_url = format!(
        "https://github.com/{}/{}/releases/download/{}/{}",
        env!("YT_SAVE_GITHUB_OWNER"),
        env!("YT_SAVE_GITHUB_REPOSITORY"),
        release.tag,
        expected_name
    );
    if asset.download_url.as_str() != expected_url {
        bail!("installer URL does not match the exact branded GitHub release asset path");
    }
    Ok(asset)
}

fn validate_typed_release(release: &AppRelease) -> Result<()> {
    if parse_tag(&release.tag)? != release.version {
        bail!("release tag and SemVer value do not match");
    }
    if release.channel == AppUpdateChannel::Stable && !release.version.pre.is_empty() {
        bail!("a prerelease cannot be used by the stable update channel");
    }
    let expected_page = format!(
        "https://github.com/{}/{}/releases/tag/{}",
        env!("YT_SAVE_GITHUB_OWNER"),
        env!("YT_SAVE_GITHUB_REPOSITORY"),
        release.tag
    );
    if release.html_url.as_str() != expected_page {
        bail!("release page URL does not match the branded GitHub repository");
    }
    if release.title.trim().is_empty()
        || release.title.len() > MAX_TITLE_BYTES
        || release.body.len() > MAX_NOTES_BYTES
    {
        bail!("release title or notes exceed updater metadata constraints");
    }
    Ok(())
}

fn parse_tag(tag: &str) -> Result<Version> {
    let version = tag.strip_prefix('v').context("release tag must start with v")?;
    let parsed = Version::parse(version).context("release tag is not strict SemVer")?;
    if format!("v{parsed}") != tag {
        bail!("release tag is not canonical v-prefixed SemVer");
    }
    Ok(parsed)
}

fn parse_version(value: &str) -> Result<Version> {
    let parsed = Version::parse(value).context("version is not SemVer")?;
    if parsed.to_string() != value {
        bail!("version is not canonical SemVer");
    }
    Ok(parsed)
}

fn parse_sha256_digest(value: &str) -> Result<[u8; 32]> {
    let hex = value.strip_prefix("sha256:").context("GitHub asset digest is not SHA-256")?;
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("GitHub SHA-256 digest is malformed");
    }
    let mut digest = [0_u8; 32];
    for (index, slot) in digest.iter_mut().enumerate() {
        let high = hex_nibble(hex.as_bytes()[index * 2]).expect("validated hexadecimal digest");
        let low = hex_nibble(hex.as_bytes()[index * 2 + 1]).expect("validated hexadecimal digest");
        *slot = (high << 4) | low;
    }
    Ok(digest)
}

fn hex_digest(digest: &[u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn installer_cache_path(cache_dir: &Path, name: &str) -> Result<PathBuf> {
    let valid_basename = !name.is_empty()
        && name != "."
        && name != ".."
        && !name.chars().any(|character| matches!(character, '/' | '\\' | ':'))
        && Path::new(name).file_name().and_then(|part| part.to_str()) == Some(name);
    if !valid_basename {
        bail!("unsafe installer cache filename");
    }
    Ok(cache_dir.join(name))
}

fn ensure_cache_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.file_type().is_symlink()
                || is_reparse_point(&metadata)
                || !metadata.is_dir() =>
        {
            bail!("refusing to use unsafe updater cache directory {}", path.display());
        }
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("inspect updater cache {}", path.display()));
        }
    }
    fs::create_dir_all(path).with_context(|| format!("create updater cache {}", path.display()))?;
    let metadata = fs::symlink_metadata(path).context("recheck updater cache directory")?;
    if metadata.file_type().is_symlink() || is_reparse_point(&metadata) || !metadata.is_dir() {
        bail!("updater cache directory changed to an unsafe path");
    }
    Ok(())
}

fn validate_existing_cache_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).context("inspect installer cache directory")?;
    if metadata.file_type().is_symlink() || is_reparse_point(&metadata) || !metadata.is_dir() {
        bail!("refusing to use unsafe updater cache directory {}", path.display());
    }
    Ok(())
}

fn ensure_user_cache_directory(path: &Path, create: bool) -> Result<PathBuf> {
    let project_dirs = directories::ProjectDirs::from(
        "com",
        env!("YT_SAVE_PUBLISHER"),
        env!("YT_SAVE_PRODUCT_NAME"),
    )
    .context("determine the branded per-user update cache")?;
    let expected = project_dirs.cache_dir().join("app-updates");
    if create {
        ensure_cache_directory(&expected)?;
    }
    validate_existing_cache_directory(path)?;
    let actual = path.canonicalize().context("resolve per-user update cache")?;
    let canonical_expected =
        expected.canonicalize().context("resolve branded per-user update cache")?;
    if actual != canonical_expected {
        bail!("installer cache is outside the branded per-user update cache");
    }
    Ok(actual)
}

fn lookup_verified_installer(path: &Path) -> Result<VerifiedInstallerMetadata> {
    let registry = VERIFIED_INSTALLERS.get().context(
        "verify the cached installer against the selected GitHub release before handoff",
    )?;
    let registry =
        registry.lock().map_err(|_| anyhow::anyhow!("verified installer state is unavailable"))?;
    registry
        .get(path)
        .cloned()
        .context("cached installer has no in-process GitHub verification record")
}

fn verify_installer_file(path: &Path, name: &str, size: u64, expected: &[u8; 32]) -> Result<()> {
    if path.file_name().and_then(|name| name.to_str()) != Some(name) {
        bail!("installer path does not match its expected asset name");
    }
    let metadata = fs::symlink_metadata(path).context("inspect installer before handoff")?;
    if metadata.file_type().is_symlink() || is_reparse_point(&metadata) || !metadata.is_file() {
        bail!("refusing to execute an installer that is not a regular file");
    }
    if metadata.len() != size || size == 0 || size > MAX_INSTALLER_BYTES {
        bail!("installer size changed after GitHub verification");
    }
    let mut file = File::open(path).context("open installer before handoff")?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut read_bytes = 0_u64;
    loop {
        let count = file.read(&mut buffer).context("read installer before handoff")?;
        if count == 0 {
            break;
        }
        read_bytes = read_bytes.checked_add(count as u64).context("installer size overflowed")?;
        if read_bytes > size {
            bail!("installer grew while being verified for handoff");
        }
        hasher.update(&buffer[..count]);
    }
    let actual: [u8; 32] = hasher.finalize().into();
    if read_bytes != size || actual != *expected {
        bail!("installer SHA-256 changed after GitHub verification");
    }
    Ok(())
}

#[cfg(windows)]
fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_reparse_point(_metadata: &fs::Metadata) -> bool {
    false
}

fn installer_kind_from_markers(msi: bool, inno: bool) -> Result<InstallerKind> {
    match (msi, inno) {
        (true, false) => Ok(InstallerKind::Msi),
        (false, true) => Ok(InstallerKind::SetupExe),
        (false, false) => bail!("no installed Yt Save installer marker was found"),
        (true, true) => bail!("both installer markers are present; refusing an ambiguous update"),
    }
}

#[cfg(windows)]
fn registry_marker(subkey: &str, value_name: &str) -> Result<bool> {
    use windows_sys::Win32::{
        Foundation::{ERROR_FILE_NOT_FOUND, ERROR_PATH_NOT_FOUND, ERROR_SUCCESS},
        System::Registry::{
            HKEY_LOCAL_MACHINE, KEY_QUERY_VALUE, KEY_WOW64_64KEY, REG_DWORD, RegCloseKey,
            RegOpenKeyExW, RegQueryValueExW,
        },
    };

    let subkey: Vec<u16> = subkey.encode_utf16().chain(Some(0)).collect();
    let mut key = std::ptr::null_mut();
    let status = unsafe {
        RegOpenKeyExW(
            HKEY_LOCAL_MACHINE,
            subkey.as_ptr(),
            0,
            KEY_QUERY_VALUE | KEY_WOW64_64KEY,
            &mut key,
        )
    };
    if status == ERROR_FILE_NOT_FOUND || status == ERROR_PATH_NOT_FOUND {
        return Ok(false);
    }
    if status != ERROR_SUCCESS {
        bail!("could not read Yt Save installer marker registry key (code {status})");
    }
    let value_name: Vec<u16> = value_name.encode_utf16().chain(Some(0)).collect();
    let mut value_type = 0_u32;
    let mut value = 0_u32;
    let mut bytes = std::mem::size_of::<u32>() as u32;
    let query_status = unsafe {
        RegQueryValueExW(
            key,
            value_name.as_ptr(),
            std::ptr::null(),
            &mut value_type,
            (&mut value as *mut u32).cast::<u8>(),
            &mut bytes,
        )
    };
    unsafe { RegCloseKey(key) };
    if query_status == ERROR_FILE_NOT_FOUND {
        return Ok(false);
    }
    if query_status != ERROR_SUCCESS {
        bail!("could not read Yt Save installer marker value (code {query_status})");
    }
    if value_type != REG_DWORD || bytes != std::mem::size_of::<u32>() as u32 {
        bail!("Yt Save installer marker has an unexpected registry type");
    }
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => bail!("Yt Save installer marker has an unexpected DWORD value"),
    }
}

#[derive(Debug, Eq, PartialEq)]
struct HelperArgs {
    parent_pid: u32,
    kind: InstallerKind,
    installer_path: PathBuf,
    app_path: PathBuf,
    expected_size: u64,
    expected_sha256: [u8; 32],
    cache_dir: PathBuf,
    expected_version: Version,
    ready_event_name: String,
    cancel_event_name: String,
}

fn parse_helper_args(args: &[String]) -> Result<HelperArgs> {
    if args.len() != 21
        || args[0] != HELPER_MARKER
        || args[1] != HELPER_PARENT
        || args[3] != HELPER_KIND
        || args[5] != HELPER_INSTALLER
        || args[7] != HELPER_APP
        || args[9] != HELPER_SIZE
        || args[11] != HELPER_SHA256
        || args[13] != HELPER_CACHE
        || args[15] != HELPER_VERSION
        || args[17] != HELPER_READY_EVENT
        || args[19] != HELPER_CANCEL_EVENT
    {
        bail!("invalid updater helper arguments");
    }
    let parent_pid = args[2].parse::<u32>().context("invalid updater parent process ID")?;
    if parent_pid == 0 || parent_pid == std::process::id() {
        bail!("invalid updater parent process ID");
    }
    let installer_path = PathBuf::from(&args[6]);
    let app_path = PathBuf::from(&args[8]);
    let cache_dir = PathBuf::from(&args[14]);
    if !installer_path.is_absolute() || !app_path.is_absolute() || !cache_dir.is_absolute() {
        bail!("updater helper paths must be absolute");
    }
    let expected_size = args[10].parse::<u64>().context("invalid expected installer size")?;
    if expected_size == 0 || expected_size > MAX_INSTALLER_BYTES {
        bail!("expected installer size is outside the supported range");
    }
    let expected_sha256 = parse_sha256_digest(&format!("sha256:{}", args[12]))?;
    let expected_version =
        parse_version(&args[16]).context("invalid expected installer version")?;
    let ready_event_name = args[18].clone();
    validate_ready_event_name(&ready_event_name)?;
    let cancel_event_name = args[20].clone();
    validate_cancel_event_name(&cancel_event_name)?;
    Ok(HelperArgs {
        parent_pid,
        kind: InstallerKind::from_value(&args[4])?,
        installer_path,
        app_path,
        expected_size,
        expected_sha256,
        cache_dir,
        expected_version,
        ready_event_name,
        cancel_event_name,
    })
}

fn validate_ready_event_name(value: &str) -> Result<()> {
    let suffix = value
        .strip_prefix(r"Local\YtSave-Update-Ready-")
        .context("invalid updater helper readiness event name")?;
    let identifier =
        Uuid::parse_str(suffix).context("invalid updater helper readiness event ID")?;
    if identifier.to_string() != suffix {
        bail!("updater helper readiness event ID is not canonical");
    }
    Ok(())
}

fn validate_cancel_event_name(value: &str) -> Result<()> {
    let suffix = value
        .strip_prefix(r"Local\YtSave-Update-Cancel-")
        .context("invalid updater helper cancellation event name")?;
    let identifier =
        Uuid::parse_str(suffix).context("invalid updater helper cancellation event ID")?;
    if identifier.to_string() != suffix {
        bail!("updater helper cancellation event ID is not canonical");
    }
    Ok(())
}

#[cfg(windows)]
fn copy_executable_create_new(source: &Path, destination: &Path) -> Result<()> {
    let result = (|| {
        let mut source =
            File::open(source).context("open current app executable for helper copy")?;
        let mut destination_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)
            .context("create updater-only helper executable")?;
        std::io::copy(&mut source, &mut destination_file)
            .context("copy updater helper executable")?;
        destination_file.sync_all().context("flush updater helper executable")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(destination);
    }
    result
}

#[cfg(windows)]
fn ensure_regular_executable_path(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).context("inspect app executable")?;
    if metadata.file_type().is_symlink() || is_reparse_point(&metadata) || !metadata.is_file() {
        bail!("app path must refer to a regular executable file");
    }
    Ok(())
}

#[cfg(windows)]
fn ensure_helper_installer_path(
    path: &Path,
    cache_dir: &Path,
    kind: InstallerKind,
    expected: &VerifiedInstallerMetadata,
) -> Result<()> {
    ensure_regular_executable_path(path)?;
    let parent = path.parent().context("locate installer cache directory")?;
    let canonical_parent = parent.canonicalize().context("resolve installer cache directory")?;
    if canonical_parent != cache_dir {
        bail!("installer must be directly inside the branded per-user update cache");
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("installer filename is not UTF-8")?;
    let prefix = format!("{}-", env!("YT_SAVE_PACKAGE_NAME"));
    let suffix = match kind {
        InstallerKind::Msi => "-x64.msi",
        InstallerKind::SetupExe => "-x64-setup.exe",
    };
    let version = name
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix(suffix))
        .context("installer filename does not match the supported Yt Save package format")?;
    let parsed_version = parse_version(version)
        .context("installer filename does not contain a canonical SemVer version")?;
    if name != expected.name || parsed_version != expected.version {
        bail!("installer filename does not match the verified release metadata");
    }
    let metadata = fs::metadata(path).context("inspect installer file size")?;
    if metadata.len() == 0 || metadata.len() > MAX_INSTALLER_BYTES {
        bail!("installer file size is outside the supported range");
    }
    Ok(())
}

#[cfg(windows)]
fn run_helper_windows(args: HelperArgs) -> Result<i32> {
    let installation = wait_for_parent(
        args.parent_pid,
        &args.ready_event_name,
        &args.cancel_event_name,
    )
    .and_then(|()| {
        let cache_dir = ensure_user_cache_directory(&args.cache_dir, false)?;
        let metadata = VerifiedInstallerMetadata {
            name: args
                .installer_path
                .file_name()
                .and_then(|name| name.to_str())
                .context("installer filename is not UTF-8")?
                .to_owned(),
            version: args.expected_version.clone(),
            size: args.expected_size,
            sha256: args.expected_sha256,
        };
        ensure_helper_installer_path(&args.installer_path, &cache_dir, args.kind, &metadata)?;
        verify_installer_file(
            &args.installer_path,
            &metadata.name,
            metadata.size,
            &metadata.sha256,
        )?;
        let mut command = match args.kind {
            InstallerKind::Msi => {
                let system_root =
                    std::env::var_os("SystemRoot").context("SystemRoot is missing")?;
                let msiexec = PathBuf::from(system_root).join("System32").join("msiexec.exe");
                let mut command = Command::new(msiexec);
                command.arg("/i").arg(&args.installer_path);
                command
            }
            InstallerKind::SetupExe => Command::new(&args.installer_path),
        };
        let status =
            command.stdin(Stdio::null()).status().context("run interactive Yt Save installer")?;
        Ok(status.code().unwrap_or(1))
    });
    let relaunch = relaunch_installed_app(&args.app_path);
    match (installation, relaunch) {
        (Ok(code), Ok(())) => Ok(code),
        (Err(install_error), Ok(())) => Err(install_error),
        (Ok(_), Err(relaunch_error)) => Err(relaunch_error),
        (Err(install_error), Err(relaunch_error)) => {
            Err(install_error).context(format!("the app relaunch also failed: {relaunch_error:#}"))
        }
    }
}

#[cfg(windows)]
fn wait_for_parent(parent_pid: u32, ready_event_name: &str, cancel_event_name: &str) -> Result<()> {
    use windows_sys::Win32::{
        Foundation::{CloseHandle, WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT},
        System::Threading::{
            OpenEventW, OpenProcess, PROCESS_SYNCHRONIZE, SYNCHRONIZATION_SYNCHRONIZE,
            WaitForMultipleObjects,
        },
    };
    let process = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, parent_pid) };
    if process.is_null() {
        bail!("could not open the parent app process handle");
    }
    let cancel_event_name: Vec<u16> = cancel_event_name.encode_utf16().chain(Some(0)).collect();
    let cancel_event =
        unsafe { OpenEventW(SYNCHRONIZATION_SYNCHRONIZE, 0, cancel_event_name.as_ptr()) };
    if cancel_event.is_null() {
        unsafe { CloseHandle(process) };
        bail!("could not open the updater helper cancellation event");
    }
    if let Err(error) = signal_helper_ready(ready_event_name) {
        unsafe { CloseHandle(process) };
        unsafe { CloseHandle(cancel_event) };
        return Err(error);
    }
    // WaitForMultipleObjects returns the lowest-index signaled handle. Put cancellation
    // first so a simultaneous parent exit cannot outrank a cancellation request.
    let handles = [cancel_event, process];
    let result = unsafe {
        WaitForMultipleObjects(
            handles.len() as u32,
            handles.as_ptr(),
            0,
            HELPER_PARENT_WAIT_TIMEOUT_MS,
        )
    };
    unsafe { CloseHandle(process) };
    unsafe { CloseHandle(cancel_event) };
    if result == WAIT_OBJECT_0 {
        bail!("the application canceled this updater handoff")
    } else if result == WAIT_OBJECT_0 + 1 {
        Ok(())
    } else if result == WAIT_FAILED {
        bail!("waiting for the parent app process failed")
    } else if result == WAIT_TIMEOUT {
        bail!("the parent app did not exit before the updater wait timed out")
    } else {
        bail!("waiting for the parent app process returned an unexpected result")
    }
}

#[cfg(windows)]
fn signal_helper_ready(event_name: &str) -> Result<()> {
    use windows_sys::Win32::System::Threading::{EVENT_MODIFY_STATE, OpenEventW, SetEvent};

    let event_name: Vec<u16> = event_name.encode_utf16().chain(Some(0)).collect();
    let event = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, event_name.as_ptr()) };
    if event.is_null() {
        bail!("could not open updater helper readiness event");
    }
    let signaled = unsafe { SetEvent(event) } != 0;
    unsafe { windows_sys::Win32::Foundation::CloseHandle(event) };
    if !signaled {
        bail!("could not signal updater helper readiness event");
    }
    Ok(())
}

fn signal_helper_cancel_event(event_name: &str) -> Result<()> {
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Threading::{EVENT_MODIFY_STATE, OpenEventW, SetEvent};
        let event_name: Vec<u16> = event_name.encode_utf16().chain(Some(0)).collect();
        let event = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, event_name.as_ptr()) };
        if event.is_null() {
            bail!("could not open the updater helper cancellation event");
        }
        let signaled = unsafe { SetEvent(event) } != 0;
        unsafe { windows_sys::Win32::Foundation::CloseHandle(event) };
        if !signaled {
            bail!("could not signal the updater helper cancellation event");
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let _ = event_name;
        bail!("updater helper cancellation is supported only on Windows")
    }
}

#[cfg(windows)]
fn relaunch_installed_app(path: &Path) -> Result<()> {
    if wait_briefly_for_existing_instance()? {
        return Ok(());
    }
    ensure_regular_executable_path(path)?;
    Command::new(path)
        .current_dir(path.parent().context("locate installed app directory")?)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("relaunch Yt Save after installer completion")?;
    Ok(())
}

#[cfg(windows)]
fn wait_briefly_for_existing_instance() -> Result<bool> {
    use windows_sys::Win32::{
        Foundation::{CloseHandle, HANDLE},
        System::Threading::{OpenMutexW, SYNCHRONIZATION_SYNCHRONIZE},
    };

    let mutex_name =
        format!(r"Local\{}-{}-instance", env!("YT_SAVE_PUBLISHER"), env!("YT_SAVE_PRODUCT_NAME"));
    let mutex_name: Vec<u16> = mutex_name.encode_utf16().chain(Some(0)).collect();
    for _ in 0..30 {
        let handle: HANDLE =
            unsafe { OpenMutexW(SYNCHRONIZATION_SYNCHRONIZE, 0, mutex_name.as_ptr()) };
        if !handle.is_null() {
            unsafe { CloseHandle(handle) };
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_release(tag: &str, prerelease: bool) -> RawRelease {
        let version = parse_tag(tag).unwrap();
        let msi_name = format!("{}-{version}-x64.msi", env!("YT_SAVE_PACKAGE_NAME"));
        let exe_name = format!("{}-{version}-x64-setup.exe", env!("YT_SAVE_PACKAGE_NAME"));
        let digest = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        RawRelease {
            tag_name: tag.to_owned(),
            name: Some(format!("Yt Save {version}")),
            body: Some("Release notes".to_owned()),
            html_url: format!(
                "https://github.com/{}/{}/releases/tag/{tag}",
                env!("YT_SAVE_GITHUB_OWNER"),
                env!("YT_SAVE_GITHUB_REPOSITORY")
            ),
            draft: false,
            prerelease,
            assets: vec![
                RawAsset {
                    name: msi_name.clone(),
                    browser_download_url: format!(
                        "https://github.com/{}/{}/releases/download/{tag}/{msi_name}",
                        env!("YT_SAVE_GITHUB_OWNER"),
                        env!("YT_SAVE_GITHUB_REPOSITORY")
                    ),
                    digest: Some(digest.to_owned()),
                    size: 123,
                },
                RawAsset {
                    name: exe_name.clone(),
                    browser_download_url: format!(
                        "https://github.com/{}/{}/releases/download/{tag}/{exe_name}",
                        env!("YT_SAVE_GITHUB_OWNER"),
                        env!("YT_SAVE_GITHUB_REPOSITORY")
                    ),
                    digest: Some(digest.to_owned()),
                    size: 456,
                },
            ],
        }
    }

    #[test]
    fn channel_database_values_default_safely_to_stable() {
        assert_eq!(AppUpdateChannel::Stable.database_value(), "stable");
        assert_eq!(AppUpdateChannel::Preview.label(), "Preview");
        assert_eq!(
            AppUpdateChannel::from_database_value(Some("preview")),
            AppUpdateChannel::Preview
        );
        assert_eq!(
            AppUpdateChannel::from_database_value(Some("unknown")),
            AppUpdateChannel::Stable
        );
        assert_eq!(AppUpdateChannel::from_database_value(None), AppUpdateChannel::Stable);
    }

    #[test]
    fn release_selection_uses_semver_and_channel_rules() {
        let stable = raw_release("v1.9.0", false);
        let preview = raw_release("v2.0.0-rc.2", true);
        let latest = raw_release("v2.0.0-rc.10", true);
        let stable_choice =
            select_release(AppUpdateChannel::Stable, [&stable, &preview, &latest]).unwrap();
        assert_eq!(stable_choice.version, Version::parse("1.9.0").unwrap());
        let preview_choice =
            select_release(AppUpdateChannel::Preview, [&stable, &preview, &latest]).unwrap();
        assert_eq!(preview_choice.version, Version::parse("2.0.0-rc.10").unwrap());
        assert_eq!(parse_tag("v1.2.3+build.4").unwrap(), Version::parse("1.2.3+build.4").unwrap());
        assert!(parse_tag("1.2.3").is_err());
        assert!(parse_tag("v01.2.3").is_err());
    }

    #[test]
    fn preview_channel_accepts_github_prerelease_with_plain_semver_tag() {
        let preview = raw_release("v1.2.3", true);
        let release = select_release(AppUpdateChannel::Preview, [&preview]).unwrap();
        assert_eq!(release.version, Version::parse("1.2.3").unwrap());
        assert!(select_release(AppUpdateChannel::Stable, [&preview]).is_err());

        let semver_preview = raw_release("v1.2.4-beta.1", false);
        assert!(select_release(AppUpdateChannel::Stable, [&semver_preview]).is_err());
    }

    #[test]
    fn asset_urls_and_names_must_be_exact() {
        let valid = raw_release("v1.2.3", false);
        assert!(parse_release(&valid, AppUpdateChannel::Stable).is_ok());
        let mut bad_url = raw_release("v1.2.3", false);
        bad_url.assets[0].browser_download_url.push_str("?download=1");
        assert!(parse_release(&bad_url, AppUpdateChannel::Stable).is_err());
        let mut bad_asset = raw_release("v1.2.3", false);
        bad_asset.assets[0].name = "../yt-save-1.2.3-x64.msi".to_owned();
        assert!(parse_release(&bad_asset, AppUpdateChannel::Stable).is_err());
        for unsafe_name in ["../setup.exe", "folder/setup.exe", "folder\\setup.exe", "C:setup.exe"]
        {
            assert!(installer_cache_path(Path::new("C:/cache"), unsafe_name).is_err());
        }
        assert_eq!(
            installer_cache_path(Path::new("C:/cache"), "yt-save-1.2.3-x64.msi").unwrap(),
            Path::new("C:/cache/yt-save-1.2.3-x64.msi")
        );
    }

    #[test]
    fn digest_parser_requires_exact_sha256_hex() {
        let valid = parse_sha256_digest(
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .unwrap();
        assert_eq!(valid[0], 0xba);
        assert_eq!(valid[31], 0xad);
        assert!(
            parse_sha256_digest(
                "sha512:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            )
            .is_err()
        );
        assert!(parse_sha256_digest("sha256:xyz").is_err());
    }

    #[test]
    fn cached_installer_verification_checks_size_and_sha256() {
        let raw = raw_release("v1.2.3", false);
        let mut release = parse_release(&raw, AppUpdateChannel::Stable).unwrap();
        release.msi.size = 3;
        let cache = std::env::temp_dir().join(format!("yt-save-update-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&cache).unwrap();
        let path = cache.join(&release.msi.name);

        std::fs::write(&path, b"abc").unwrap();
        assert!(verify_cached_installer(&release, InstallerKind::Msi, &path).is_ok());

        std::fs::write(&path, b"abd").unwrap();
        assert!(verify_cached_installer(&release, InstallerKind::Msi, &path).is_err());

        std::fs::write(&path, b"long").unwrap();
        assert!(verify_cached_installer(&release, InstallerKind::Msi, &path).is_err());
        std::fs::remove_dir_all(cache).unwrap();
    }

    #[test]
    fn installer_marker_mapping_rejects_neither_or_both_formats() {
        assert_eq!(installer_kind_from_markers(true, false).unwrap(), InstallerKind::Msi);
        assert_eq!(installer_kind_from_markers(false, true).unwrap(), InstallerKind::SetupExe);
        assert!(installer_kind_from_markers(false, false).is_err());
        assert!(installer_kind_from_markers(true, true).is_err());
    }

    #[test]
    fn helper_argument_parser_rejects_missing_or_malformed_values() {
        assert!(parse_helper_args(&[]).is_err());
        let parent_pid = std::process::id().checked_add(1).unwrap_or(1);
        let valid = vec![
            HELPER_MARKER.to_owned(),
            HELPER_PARENT.to_owned(),
            parent_pid.to_string(),
            HELPER_KIND.to_owned(),
            "msi".to_owned(),
            HELPER_INSTALLER.to_owned(),
            format!(r"C:\Users\user\cache\{}-1.2.3-x64.msi", env!("YT_SAVE_PACKAGE_NAME")),
            HELPER_APP.to_owned(),
            r"C:\Program Files\Yt Save\yt-save.exe".to_owned(),
            HELPER_SIZE.to_owned(),
            "123".to_owned(),
            HELPER_SHA256.to_owned(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_owned(),
            HELPER_CACHE.to_owned(),
            r"C:\Users\user\cache".to_owned(),
            HELPER_VERSION.to_owned(),
            "1.2.3".to_owned(),
            HELPER_READY_EVENT.to_owned(),
            format!(r"Local\YtSave-Update-Ready-{}", Uuid::new_v4()),
            HELPER_CANCEL_EVENT.to_owned(),
            format!(r"Local\YtSave-Update-Cancel-{}", Uuid::new_v4()),
        ];
        assert!(parse_helper_args(&valid).is_ok());
        let mut invalid = valid;
        invalid[2] = "not-a-pid".to_owned();
        assert!(parse_helper_args(&invalid).is_err());
        assert!(validate_ready_event_name("Local\\wrong").is_err());
        assert!(validate_cancel_event_name("Local\\wrong").is_err());
    }
}
