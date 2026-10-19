use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::{process::Command, time::timeout};
use tracing::{info, warn};
use uuid::Uuid;

const LATEST_RELEASE_API: &str = "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest";
const MAX_EXECUTABLE_BYTES: u64 = 100 * 1024 * 1024;
const MAX_RELEASE_METADATA_BYTES: u64 = 2 * 1024 * 1024;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(180);
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpdateOutcome {
    AlreadyCurrent { installed_version: String, latest_version: String },
    Updated { previous_version: String, installed_version: String },
}

#[derive(Debug, Deserialize)]
struct Release {
    tag_name: String,
    draft: bool,
    prerelease: bool,
    assets: Vec<ReleaseAsset>,
}

#[derive(Debug, Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
    digest: Option<String>,
}

struct StagedFile(PathBuf);

impl Drop for StagedFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub async fn check_and_install(
    current_executable: &Path,
    user_directory: &Path,
) -> Result<UpdateOutcome> {
    let current_version = probe_version(current_executable).await?;
    let current_key = stable_version_key(&current_version).context(
        "the installed yt-dlp build is not a dated stable release; refusing a downgrade",
    )?;

    let client = reqwest::Client::builder()
        .user_agent(concat!("YtSave/", env!("CARGO_PKG_VERSION")))
        .timeout(DOWNLOAD_TIMEOUT)
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= 5 {
                return attempt.error("too many redirects while contacting the official updater");
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
        .context("create the yt-dlp update client")?;
    let release_response = client
        .get(LATEST_RELEASE_API)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await
        .context("request the official yt-dlp stable release")?
        .error_for_status()
        .context("the official yt-dlp release service returned an error")?;
    if release_response.content_length().is_some_and(|length| length > MAX_RELEASE_METADATA_BYTES) {
        bail!("the official yt-dlp release metadata is unexpectedly large");
    }
    let release_bytes = read_bounded_response(release_response, MAX_RELEASE_METADATA_BYTES)
        .await
        .context("read official yt-dlp release metadata")?;
    let release: Release =
        serde_json::from_slice(&release_bytes).context("parse official yt-dlp release metadata")?;

    if release.draft || release.prerelease {
        bail!("GitHub returned a non-stable yt-dlp release from the stable endpoint.");
    }
    let latest_version = validate_stable_tag(&release.tag_name)?;
    let latest_key = stable_version_key(&latest_version).expect("validated stable release tag");
    if current_key >= latest_key {
        return Ok(UpdateOutcome::AlreadyCurrent {
            installed_version: current_version,
            latest_version,
        });
    }

    let asset = release
        .assets
        .iter()
        .find(|asset| asset.name == "yt-dlp.exe")
        .context("the official stable release has no Windows yt-dlp.exe asset")?;
    validate_asset_url(&asset.browser_download_url, &latest_version)?;
    let expected_hash =
        parse_sha256_digest(asset.digest.as_deref().context(
            "GitHub did not publish a SHA-256 digest for the official yt-dlp executable",
        )?)?;

    let response = client
        .get(&asset.browser_download_url)
        .send()
        .await
        .context("download the official yt-dlp executable")?
        .error_for_status()
        .context("the official yt-dlp executable download failed")?;
    if response.content_length().is_some_and(|length| length == 0 || length > MAX_EXECUTABLE_BYTES)
    {
        bail!("the official yt-dlp executable has an unexpected file size");
    }
    let bytes = read_bounded_response(response, MAX_EXECUTABLE_BYTES)
        .await
        .context("read the official yt-dlp executable")?;
    if bytes.is_empty() {
        bail!("the official yt-dlp executable has an unexpected file size");
    }
    verify_sha256(&bytes, &expected_hash)
        .context("the yt-dlp SHA-256 digest does not match the official GitHub release metadata")?;

    let versions_directory = user_directory.join("versions");
    reject_non_directory_if_present(&versions_directory)?;
    tokio::fs::create_dir_all(&versions_directory)
        .await
        .context("create the per-user yt-dlp versions directory")?;
    let version_directory = versions_directory.join(&latest_version);
    reject_non_directory_if_present(&version_directory)?;
    tokio::fs::create_dir_all(&version_directory).await.with_context(|| {
        format!("create the per-user yt-dlp version directory {}", version_directory.display())
    })?;
    let target = version_directory.join("yt-dlp.exe");
    reject_non_file_if_present(&target)?;
    let stage = version_directory.join(format!("yt-dlp-update-{}.exe", Uuid::new_v4()));
    let staged = StagedFile(stage.clone());
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stage)
        .await
        .context("create a staged yt-dlp update")?;
    tokio::io::AsyncWriteExt::write_all(&mut file, &bytes)
        .await
        .context("write the staged yt-dlp update")?;
    file.sync_all().await.context("flush the staged yt-dlp update")?;
    drop(file);
    let staged_bytes = tokio::fs::read(&stage).await.context("re-read the staged yt-dlp update")?;
    verify_sha256(&staged_bytes, &expected_hash)
        .context("the staged yt-dlp executable changed after its download was verified")?;
    let staged_version = probe_version(&stage).await?;
    validate_candidate_version(&staged_version, &latest_version)?;

    if target.exists() {
        let existing = tokio::fs::read(&target)
            .await
            .context("read the existing versioned yt-dlp executable")?;
        verify_sha256(&existing, &expected_hash).context(format!(
            "a different yt-dlp executable already exists for release {latest_version}; it was preserved"
        ))?;
        let version = probe_version(&target).await?;
        if version != latest_version {
            bail!(
                "the existing yt-dlp executable for {latest_version} reported version {version}; it was preserved"
            );
        }
    } else {
        tokio::fs::rename(&stage, &target)
            .await
            .context("stage the verified yt-dlp executable under its release version")?;
    }
    let installed_version = probe_version(&target).await?;
    if installed_version != latest_version {
        bail!("installed yt-dlp reported version {installed_version}, expected {latest_version}");
    }
    write_checksum_record(&target, &expected_hash)?;
    activate_version(user_directory, &latest_version)?;
    match probe_version(&target).await {
        Ok(version) if version == latest_version => {}
        Ok(version) => {
            restore_previous_version(user_directory)?;
            bail!(
                "the active yt-dlp update reported version {version}, expected {latest_version}; the previous version was restored"
            );
        }
        Err(error) => {
            restore_previous_version(user_directory)?;
            return Err(error).context("the active yt-dlp update failed its post-activation check; the previous version was restored");
        }
    }
    info!(version = %installed_version, "installed a verified yt-dlp stable update");
    drop(staged);
    Ok(UpdateOutcome::Updated { previous_version: current_version, installed_version })
}

/// Selects a previously verified per-user engine, restoring the previous marker when the active
/// overlay is missing, damaged, or reports a different version. The packaged executable remains
/// the final fallback and is never modified.
pub async fn recover_startup_engine(user_directory: &Path, packaged_executable: &Path) -> PathBuf {
    let active_marker = user_directory.join("active-version");
    let previous_marker = user_directory.join("previous-version");

    if let Some(version) = crate::dependencies::read_stable_version_marker(&active_marker) {
        match verify_installed_version(user_directory, &version).await {
            Ok(executable) => return executable,
            Err(error) => {
                warn!(version, %error, "active yt-dlp overlay failed startup verification")
            }
        }
    }

    if let Some(version) = crate::dependencies::read_stable_version_marker(&previous_marker) {
        match verify_installed_version(user_directory, &version).await {
            Ok(executable) => match restore_previous_version(user_directory) {
                Ok(()) => {
                    warn!(version, "restored the previous verified yt-dlp overlay at startup");
                    return executable;
                }
                Err(error) => {
                    warn!(version, %error, "could not restore the previous yt-dlp overlay; using the packaged engine");
                    return packaged_executable.to_path_buf();
                }
            },
            Err(error) => {
                warn!(version, %error, "previous yt-dlp overlay failed startup verification")
            }
        }
    }

    clear_regular_marker(&active_marker);
    clear_regular_marker(&previous_marker);
    packaged_executable.to_path_buf()
}

async fn verify_installed_version(user_directory: &Path, version: &str) -> Result<PathBuf> {
    validate_stable_tag(version)?;
    let versions_directory = user_directory.join("versions");
    reject_non_directory_if_present(&versions_directory)?;
    let version_directory = versions_directory.join(version);
    reject_non_directory_if_present(&version_directory)?;
    let executable = version_directory.join("yt-dlp.exe");
    reject_non_file_if_present(&executable)?;
    if !executable.is_file() {
        bail!("the versioned yt-dlp executable is missing");
    }

    let checksum_path = version_directory.join("yt-dlp.exe.sha256");
    reject_non_file_if_present(&checksum_path)?;
    let checksum = tokio::fs::read_to_string(&checksum_path)
        .await
        .context("read the versioned yt-dlp SHA-256 record")?;
    let expected = parse_sha256_hex(checksum.trim())?;
    let metadata = tokio::fs::metadata(&executable)
        .await
        .context("inspect the versioned yt-dlp executable")?;
    if metadata.len() == 0 || metadata.len() > MAX_EXECUTABLE_BYTES {
        bail!("the versioned yt-dlp executable has an unexpected file size");
    }
    let bytes = tokio::fs::read(&executable)
        .await
        .context("read the versioned yt-dlp executable for startup verification")?;
    verify_sha256(&bytes, &expected)
        .context("the versioned yt-dlp executable failed SHA-256 verification")?;
    let actual_version = probe_version(&executable).await?;
    validate_candidate_version(&actual_version, version)?;
    Ok(executable)
}

fn write_checksum_record(executable: &Path, expected: &[u8; 32]) -> Result<()> {
    let directory = executable.parent().context("locate the versioned yt-dlp directory")?;
    let checksum_path = directory.join("yt-dlp.exe.sha256");
    reject_non_file_if_present(&checksum_path)?;
    let expected_text = hex_digest(expected);
    match std::fs::read_to_string(&checksum_path) {
        Ok(existing) if existing.trim().eq_ignore_ascii_case(&expected_text) => return Ok(()),
        Ok(_) => bail!("the existing yt-dlp SHA-256 record does not match the verified release"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("read the existing yt-dlp SHA-256 record"),
    }

    let staged = directory.join(format!("yt-dlp.exe.sha256-{}.tmp", Uuid::new_v4()));
    let file = StagedFile(staged.clone());
    let mut handle = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staged)
        .context("create a staged yt-dlp SHA-256 record")?;
    use std::io::Write as _;
    handle.write_all(expected_text.as_bytes()).context("write the staged yt-dlp SHA-256 record")?;
    handle.sync_all().context("flush the staged yt-dlp SHA-256 record")?;
    drop(handle);
    std::fs::rename(&staged, &checksum_path)
        .context("activate the verified yt-dlp SHA-256 record")?;
    drop(file);
    Ok(())
}

fn clear_regular_marker(path: &Path) {
    if let Ok(metadata) = std::fs::symlink_metadata(path)
        && metadata.is_file()
        && !metadata.file_type().is_symlink()
        && let Err(error) = std::fs::remove_file(path)
    {
        warn!(path = %path.display(), %error, "could not remove an invalid yt-dlp version marker");
    }
}

async fn read_bounded_response(mut response: reqwest::Response, maximum: u64) -> Result<Vec<u8>> {
    let capacity = response
        .content_length()
        .filter(|length| *length <= maximum)
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default();
    let mut content = Vec::with_capacity(capacity);
    while let Some(chunk) = response.chunk().await.context("read response body")? {
        if content.len() as u64 + chunk.len() as u64 > maximum {
            bail!("the updater response exceeded its size limit");
        }
        content.extend_from_slice(&chunk);
    }
    Ok(content)
}

fn activate_version(user_directory: &Path, version: &str) -> Result<()> {
    validate_stable_tag(version)?;
    let active = user_directory.join("active-version");
    let previous = user_directory.join("previous-version");
    reject_non_file_if_present(&active)?;
    reject_non_file_if_present(&previous)?;
    let stage = user_directory.join(format!("active-version-{}.tmp", Uuid::new_v4()));
    let mut stage_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stage)
        .context("create the staged active yt-dlp version marker")?;
    use std::io::Write as _;
    stage_file
        .write_all(version.as_bytes())
        .context("write the staged active yt-dlp version marker")?;
    stage_file.sync_all().context("flush the staged active yt-dlp version marker")?;
    drop(stage_file);
    let had_active = active.exists();
    if previous.exists() {
        std::fs::remove_file(&previous).context("remove the stale yt-dlp rollback marker")?;
    }
    if had_active && let Err(error) = std::fs::rename(&active, &previous) {
        let _ = std::fs::remove_file(&stage);
        return Err(error).context("preserve the current active yt-dlp version marker");
    }
    if let Err(error) = std::fs::rename(&stage, &active) {
        if had_active && let Err(restore_error) = std::fs::rename(&previous, &active) {
            bail!(
                "could not activate yt-dlp release ({error}) or restore its previous marker ({restore_error})"
            );
        }
        let _ = std::fs::remove_file(&stage);
        return Err(error).context("activate the verified yt-dlp release");
    }
    Ok(())
}

fn restore_previous_version(user_directory: &Path) -> Result<()> {
    let active = user_directory.join("active-version");
    let previous = user_directory.join("previous-version");
    reject_non_file_if_present(&active)?;
    reject_non_file_if_present(&previous)?;
    if active.exists() {
        std::fs::remove_file(&active).context("remove the failed active yt-dlp marker")?;
    }
    if previous.exists() {
        std::fs::rename(&previous, &active)
            .context("restore the previous active yt-dlp version")?;
    }
    Ok(())
}

fn reject_non_directory_if_present(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            bail!("refusing to use unexpected per-user engine directory {}", path.display());
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("inspect engine directory {}", path.display()))
        }
    }
}

fn reject_non_file_if_present(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            bail!("refusing to replace unexpected per-user engine path {}", path.display());
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect engine path {}", path.display())),
    }
}

async fn probe_version(executable: &Path) -> Result<String> {
    let mut command = Command::new(executable);
    command
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(0x0800_0000);
    let output = timeout(PROBE_TIMEOUT, command.output())
        .await
        .context("yt-dlp version check timed out")?
        .with_context(|| format!("run yt-dlp version check for {}", executable.display()))?;
    if !output.status.success() {
        bail!("yt-dlp version check exited with code {:?}", output.status.code());
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_owned)
        .context("yt-dlp returned no version")
}

fn validate_stable_tag(tag: &str) -> Result<String> {
    stable_version_key(tag)
        .context("the official yt-dlp release tag is not a dated stable version")?;
    Ok(tag.to_owned())
}

fn stable_version_key(version: &str) -> Option<(u32, u32, u32)> {
    let version = version.strip_prefix("stable@").unwrap_or(version);
    let mut parts = version.split('.');
    let year = parts.next()?.parse().ok()?;
    let month = parts.next()?.parse::<u32>().ok()?;
    let day = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() || !(2000..=2999).contains(&year) {
        return None;
    }
    chrono::NaiveDate::from_ymd_opt(i32::try_from(year).ok()?, month, day)?;
    Some((year, month, day))
}

fn validate_candidate_version(actual: &str, expected: &str) -> Result<()> {
    if actual != expected {
        bail!("the staged yt-dlp executable reported version {actual}, expected {expected}");
    }
    Ok(())
}

fn validate_asset_url(value: &str, tag: &str) -> Result<()> {
    let url = url::Url::parse(value).context("parse the official yt-dlp release asset URL")?;
    let expected_path = format!("/yt-dlp/yt-dlp/releases/download/{tag}/yt-dlp.exe");
    if url.scheme() != "https"
        || url.host_str() != Some("github.com")
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != expected_path
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("the yt-dlp executable URL did not match the official GitHub release path");
    }
    Ok(())
}

fn parse_sha256_digest(value: &str) -> Result<[u8; 32]> {
    let digest = value.strip_prefix("sha256:").context("GitHub asset digest is not SHA-256")?;
    parse_sha256_hex(digest)
}

fn parse_sha256_hex(digest: &str) -> Result<[u8; 32]> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("yt-dlp SHA-256 value is malformed");
    }
    let mut result = [0_u8; 32];
    for (index, slot) in result.iter_mut().enumerate() {
        let high = hex_nibble(digest.as_bytes()[index * 2]).expect("validated hex digest");
        let low = hex_nibble(digest.as_bytes()[index * 2 + 1]).expect("validated hex digest");
        *slot = (high << 4) | low;
    }
    Ok(result)
}

fn hex_digest(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn constant_time_hash_eq(actual: &[u8], expected: &[u8; 32]) -> bool {
    if actual.len() != expected.len() {
        return false;
    }
    actual.iter().zip(expected).fold(0_u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

fn verify_sha256(bytes: &[u8], expected: &[u8; 32]) -> Result<()> {
    let actual = Sha256::digest(bytes);
    if !constant_time_hash_eq(&actual, expected) {
        bail!("SHA-256 digest mismatch");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_release_tags_are_strict_and_versions_are_ordered_by_date() {
        assert_eq!(stable_version_key("2026.08.19"), Some((2026, 8, 19)));
        assert_eq!(stable_version_key("stable@2026.08.19"), Some((2026, 8, 19)));
        assert!(stable_version_key("2026.08.19-nightly").is_none());
        assert!(stable_version_key("2026.13.01").is_none());
        assert!(stable_version_key("2026.02.31").is_none());
        assert!(stable_version_key("2026.08.19.1").is_none());
        assert!(stable_version_key("2026.08.20") > stable_version_key("2026.08.19"));
    }

    #[test]
    fn release_asset_url_must_match_the_official_stable_executable_path() {
        assert!(
            validate_asset_url(
                "https://github.com/yt-dlp/yt-dlp/releases/download/2026.08.19/yt-dlp.exe",
                "2026.08.19"
            )
            .is_ok()
        );
        for url in [
            "http://github.com/yt-dlp/yt-dlp/releases/download/2026.08.19/yt-dlp.exe",
            "https://example.com/yt-dlp/yt-dlp/releases/download/2026.08.19/yt-dlp.exe",
            "https://github.com/other/repo/releases/download/2026.08.19/yt-dlp.exe",
            "https://github.com/yt-dlp/yt-dlp/releases/download/2026.08.19/other.exe",
        ] {
            assert!(validate_asset_url(url, "2026.08.19").is_err(), "{url}");
        }
    }

    #[test]
    fn github_sha256_digest_parser_rejects_malformed_or_wrong_algorithm_values() {
        let digest = parse_sha256_digest(
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .unwrap();
        assert_eq!(digest[0], 0xba);
        assert_eq!(digest[31], 0xad);
        assert!(parse_sha256_digest("sha512:ba7816bf8f01cfea").is_err());
        assert!(parse_sha256_digest("sha256:xyz").is_err());
    }

    #[test]
    fn candidate_version_must_match_the_release_tag_exactly() {
        assert!(validate_candidate_version("2026.08.19", "2026.08.19").is_ok());
        assert!(validate_candidate_version("2026.08.19-nightly", "2026.08.19").is_err());
        assert!(validate_candidate_version("2026.08.18", "2026.08.19").is_err());
    }

    #[test]
    fn downloaded_bytes_must_match_the_sha256_value_from_release_metadata() {
        let expected = parse_sha256_digest(
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )
        .unwrap();
        assert!(verify_sha256(b"abc", &expected).is_ok());
        assert!(verify_sha256(b"abd", &expected).is_err());
    }

    #[test]
    fn installed_checksum_records_are_strict_hex_sha256_values() {
        let expected =
            parse_sha256_hex("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
                .unwrap();
        assert_eq!(
            hex_digest(&expected),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(
            parse_sha256_hex(
                "SHA256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            )
            .is_err()
        );
        assert!(parse_sha256_hex("abc").is_err());
    }

    #[tokio::test]
    async fn startup_recovery_rejects_a_corrupt_overlay_and_preserves_its_file() {
        let directory = std::env::temp_dir().join(format!("yt-save-recovery-{}", Uuid::new_v4()));
        let version_directory = directory.join("versions/2026.08.19");
        std::fs::create_dir_all(&version_directory).unwrap();
        let executable = version_directory.join("yt-dlp.exe");
        std::fs::write(&executable, b"corrupt executable").unwrap();
        let wrong_digest: [u8; 32] = Sha256::digest(b"different executable").into();
        std::fs::write(version_directory.join("yt-dlp.exe.sha256"), hex_digest(&wrong_digest))
            .unwrap();
        std::fs::write(directory.join("active-version"), "2026.08.19").unwrap();
        let packaged = directory.join("packaged/yt-dlp.exe");

        let selected = recover_startup_engine(&directory, &packaged).await;

        assert_eq!(selected, packaged);
        assert!(!directory.join("active-version").exists());
        assert!(executable.is_file(), "recovery must preserve user engine files");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn activation_keeps_a_previous_version_marker_and_can_roll_back() {
        let directory = std::env::temp_dir().join(format!("yt-save-updater-{}", Uuid::new_v4()));
        std::fs::create_dir_all(directory.join("versions/2026.08.19")).unwrap();
        std::fs::create_dir_all(directory.join("versions/2026.10.20")).unwrap();
        std::fs::write(directory.join("versions/2026.08.19/yt-dlp.exe"), b"old").unwrap();
        std::fs::write(directory.join("versions/2026.10.20/yt-dlp.exe"), b"new").unwrap();
        std::fs::write(directory.join("active-version"), "2026.08.19").unwrap();

        activate_version(&directory, "2026.10.20").unwrap();
        assert_eq!(
            std::fs::read_to_string(directory.join("active-version")).unwrap(),
            "2026.10.20"
        );
        assert_eq!(
            std::fs::read_to_string(directory.join("previous-version")).unwrap(),
            "2026.08.19"
        );
        restore_previous_version(&directory).unwrap();
        assert_eq!(
            std::fs::read_to_string(directory.join("active-version")).unwrap(),
            "2026.08.19"
        );
        assert!(!directory.join("previous-version").exists());
        std::fs::remove_file(directory.join("active-version")).unwrap();
        activate_version(&directory, "2026.10.20").unwrap();
        restore_previous_version(&directory).unwrap();
        assert!(!directory.join("active-version").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
