//! Per-host cookie storage for yt-dlp.
//!
//! Cookie jars are protected with the current Windows user's DPAPI key. A
//! short-lived Netscape-format file is created only while yt-dlp is running;
//! dropping its lease removes that plaintext staging file.

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use tracing::warn;

use url::{Host, Url};
use uuid::Uuid;

const JAR_HEADER: &[u8] = b"# Netscape HTTP Cookie File\r\n";
const ENVELOPE_HEADER: &[u8] = b"YT-SAVE-COOKIE-V1\n";
const MAX_COOKIE_COUNT: usize = 4096;
const MAX_JAR_BYTES: usize = 2 * 1024 * 1024;
const MAX_VAULT_BYTES: u64 = (MAX_JAR_BYTES as u64) + 4096;
const STALE_LEASE_AGE: Duration = Duration::from_secs(5 * 60);

/// A cookie selected from the embedded sign-in WebView.
///
/// `domain` may start with a dot to indicate that subdomains are included.
#[derive(Clone, Eq, PartialEq)]
pub struct CookieRecord {
    pub domain: String,
    pub path: String,
    pub name: String,
    pub value: String,
    pub secure: bool,
    pub http_only: bool,
    /// Unix timestamp; zero represents a session cookie.
    pub expires: i64,
}

impl std::fmt::Debug for CookieRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CookieRecord")
            .field("domain", &self.domain)
            .field("path", &self.path)
            .field("name", &self.name)
            .field("value", &"[REDACTED]")
            .field("secure", &self.secure)
            .field("http_only", &self.http_only)
            .field("expires", &self.expires)
            .finish()
    }
}

/// Encrypted, per-host cookie storage. `disabled()` is a harmless no-op on all
/// platforms. A constructed store uses Windows DPAPI and reports unsupported
/// on other platforms.
#[derive(Clone, Debug)]
pub struct CookieStore {
    root: PathBuf,
    enabled: bool,
    host_generations: Arc<Mutex<HashMap<String, Weak<Mutex<u64>>>>>,
    active_leases: Arc<Mutex<HashSet<PathBuf>>>,
}

impl CookieStore {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            enabled: true,
            host_generations: Arc::new(Mutex::new(HashMap::new())),
            active_leases: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn disabled() -> Self {
        Self {
            root: PathBuf::new(),
            enabled: false,
            host_generations: Arc::new(Mutex::new(HashMap::new())),
            active_leases: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn has_saved_for_url(&self, url: &str) -> Result<bool> {
        if !self.enabled {
            return Ok(false);
        }
        ensure_supported()?;
        let host = validated_url_host(url)?;
        self.vault_path(&host).try_exists().context("Could not check saved sign-in data")
    }

    pub fn save_for_url(&self, url: &str, cookies: &[CookieRecord]) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        ensure_supported()?;
        let host = validated_url_host(url)?;
        let jar = serialize_netscape(&host, cookies)?;
        let generation = self.generation_for_host(&host);
        let mut generation = generation.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        self.write_jar(&host, &jar)?;
        *generation = generation.wrapping_add(1);
        Ok(())
    }

    pub fn clear_for_url(&self, url: &str) -> Result<bool> {
        if !self.enabled {
            return Ok(false);
        }
        ensure_supported()?;
        let host = validated_url_host(url)?;
        let generation = self.generation_for_host(&host);
        let mut generation = generation.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let path = self.vault_path(&host);
        match fs::remove_file(path) {
            Ok(()) => {
                *generation = generation.wrapping_add(1);
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                *generation = generation.wrapping_add(1);
                Ok(false)
            }
            Err(error) => Err(error).context("Could not remove saved sign-in data"),
        }
    }

    pub fn lease_for_url(&self, url: &str) -> Result<Option<CookieLease>> {
        if !self.enabled {
            return Ok(None);
        }
        ensure_supported()?;
        let host = validated_url_host(url)?;
        let generation_state = self.generation_for_host(&host);
        let generation_guard =
            generation_state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let generation = *generation_guard;
        let Some(jar) = self.load_jar(&host)? else {
            return Ok(None);
        };
        drop(generation_guard);

        let staging = self.root.join(".cookie-staging");
        fs::create_dir_all(&staging).context("Could not prepare private cookie staging")?;
        let (path, mut file) = create_unique_lease_file(&staging)?;
        self.active_leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(path.clone());
        if let Err(error) = file.write_all(&jar).and_then(|()| file.sync_all()) {
            drop(file);
            if let Err(cleanup_error) = remove_lease_file(&path) {
                warn!(%cleanup_error, "could not remove incomplete temporary sign-in file");
            }
            self.untrack_active_lease(&path);
            return Err(error).context("Could not prepare temporary cookie file");
        }
        drop(file);

        Ok(Some(CookieLease { path, store: self.clone(), host, generation_state, generation }))
    }

    /// Removes old lease files left behind by an unexpected process exit.
    /// Files owned by a process with a running yt-dlp child are retained.
    pub fn cleanup_stale_leases(&self) -> Result<usize> {
        let cutoff =
            SystemTime::now().checked_sub(STALE_LEASE_AGE).unwrap_or(SystemTime::UNIX_EPOCH);
        self.cleanup_stale_leases_before(cutoff)
    }

    fn cleanup_stale_leases_before(&self, cutoff: SystemTime) -> Result<usize> {
        if !self.enabled {
            return Ok(0);
        }
        ensure_supported()?;
        #[cfg(windows)]
        let active_downloader_parents = active_yt_dlp_parent_pids()?;
        #[cfg(not(windows))]
        let active_downloader_parents = HashSet::new();
        self.cleanup_stale_leases_with_active_pids(cutoff, &active_downloader_parents)
    }

    fn cleanup_stale_leases_with_active_pids(
        &self,
        cutoff: SystemTime,
        active_downloader_parents: &HashSet<u32>,
    ) -> Result<usize> {
        if !self.enabled {
            return Ok(0);
        }
        let staging = self.root.join(".cookie-staging");
        let entries = match fs::read_dir(&staging) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error).context("Could not inspect cookie staging files"),
        };
        let mut removed = 0;
        for entry in entries {
            let entry = entry.context("Could not inspect cookie staging file")?;
            if !entry.file_type().context("Could not inspect cookie staging file type")?.is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(owner) = parse_lease_owner(&name) else {
                continue;
            };
            if self
                .active_leases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains(&entry.path())
            {
                continue;
            }
            let modified = match entry.metadata().and_then(|metadata| metadata.modified()) {
                Ok(modified) => modified,
                Err(_) => continue,
            };
            if modified >= cutoff {
                continue;
            }
            if should_retain_lease(owner, active_downloader_parents) {
                continue;
            }
            fs::remove_file(entry.path()).context("Could not remove stale cookie staging file")?;
            removed += 1;
        }
        Ok(removed)
    }

    fn vault_path(&self, host: &str) -> PathBuf {
        let digest = Sha256::digest(host.as_bytes());
        let mut filename = String::with_capacity(digest.len() * 2 + 5);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(filename, "{byte:02x}");
        }
        filename.push_str(".dpapi");
        self.root.join(filename)
    }

    fn untrack_active_lease(&self, path: &Path) {
        self.active_leases.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(path);
    }

    fn generation_for_host(&self, host: &str) -> Arc<Mutex<u64>> {
        let mut generations =
            self.host_generations.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        generations.retain(|_, generation| generation.strong_count() > 0);
        generations.get(host).and_then(Weak::upgrade).unwrap_or_else(|| {
            let generation = Arc::new(Mutex::new(0));
            generations.insert(host.to_owned(), Arc::downgrade(&generation));
            generation
        })
    }

    fn load_jar(&self, host: &str) -> Result<Option<Vec<u8>>> {
        ensure_supported()?;
        let path = self.vault_path(host);
        let encrypted = match read_bounded(&path, MAX_VAULT_BYTES) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).context("Could not read saved sign-in data"),
        };
        let plaintext = dpapi_unprotect(&encrypted)?;
        let jar = unwrap_envelope(host, &plaintext)?;
        let cookies = parse_netscape(host, jar)?;
        if cookies.is_empty() {
            return Ok(None);
        }
        Ok(Some(serialize_netscape(host, &cookies)?))
    }

    fn write_jar(&self, host: &str, jar: &[u8]) -> Result<()> {
        ensure_supported()?;
        let envelope = wrap_envelope(host, jar);
        let encrypted = dpapi_protect(&envelope)?;
        fs::create_dir_all(&self.root).context("Could not prepare cookie storage")?;

        let (temporary_path, mut file) = create_unique_file(&self.root, "vault")?;
        if let Err(error) = file.write_all(&encrypted).and_then(|()| file.sync_all()) {
            drop(file);
            let _ = fs::remove_file(&temporary_path);
            return Err(error).context("Could not write encrypted sign-in data");
        }
        drop(file);

        let destination = self.vault_path(host);
        if let Err(error) = fs::rename(&temporary_path, &destination) {
            let _ = fs::remove_file(&temporary_path);
            return Err(error).context("Could not save encrypted sign-in data");
        }
        Ok(())
    }
}

/// A temporary plaintext Netscape cookie file for one yt-dlp process.
///
/// Call `finish` after the process exits so yt-dlp's cookie updates are
/// re-encrypted. Dropping the lease always removes the temporary file.
pub struct CookieLease {
    path: PathBuf,
    store: CookieStore,
    host: String,
    generation_state: Arc<Mutex<u64>>,
    generation: u64,
}

impl CookieLease {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn finish(self) -> Result<()> {
        if !self.store.enabled {
            return Ok(());
        }
        let refresh_result: Result<()> = (|| {
            let mut current_generation =
                self.generation_state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if *current_generation != self.generation {
                return Ok(());
            }
            let bytes = read_bounded(&self.path, MAX_JAR_BYTES as u64)
                .context("Could not refresh saved sign-in data")?;
            let cookies = parse_netscape(&self.host, &bytes)?;
            // A downloader can rewrite a jar with no remaining cookies. Keep the
            // last useful encrypted copy instead of replacing it with an empty jar.
            if !cookies.is_empty() {
                let jar = serialize_netscape(&self.host, &cookies)?;
                self.store.write_jar(&self.host, &jar)?;
            }
            *current_generation = current_generation.wrapping_add(1);
            Ok(())
        })();
        let cleanup_result =
            remove_lease_file(&self.path).context("Could not remove temporary sign-in data");
        refresh_result?;
        cleanup_result
    }
}

impl Drop for CookieLease {
    fn drop(&mut self) {
        if let Err(error) = remove_lease_file(&self.path) {
            warn!(%error, "could not remove temporary sign-in file");
        }
        self.store.untrack_active_lease(&self.path);
    }
}

fn remove_lease_file(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

pub(crate) fn validated_url_host(input: &str) -> Result<String> {
    let (_, authority_and_path) =
        input.split_once("://").ok_or_else(|| anyhow!("Enter a valid website URL"))?;
    if authority_and_path.is_empty() || authority_and_path.starts_with('/') {
        bail!("The URL must include a website host");
    }
    let url = Url::parse(input).map_err(|_| anyhow!("Enter a valid website URL"))?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        bail!("Sign-in cookies require an HTTPS URL without embedded credentials");
    }
    url.host().map(host_to_string).ok_or_else(|| anyhow!("The URL must include a website host"))
}

fn host_to_string(host: Host<&str>) -> String {
    match host {
        Host::Domain(domain) => domain.to_ascii_lowercase(),
        Host::Ipv4(address) => address.to_string(),
        Host::Ipv6(address) => format!("[{address}]"),
    }
}

fn normalize_cookie_domain(domain: &str) -> Result<(String, bool)> {
    let includes_subdomains = domain.starts_with('.');
    let raw_domain =
        if includes_subdomains { domain.strip_prefix('.').unwrap_or_default() } else { domain };
    if raw_domain.is_empty() || raw_domain.starts_with('.') || raw_domain.ends_with('.') {
        bail!("Cookie domain is invalid");
    }
    let host = Host::parse(raw_domain).map_err(|_| anyhow!("Cookie domain is invalid"))?;
    let canonical = match host {
        Host::Domain(value) => value.to_ascii_lowercase(),
        Host::Ipv4(address) => address.to_string(),
        Host::Ipv6(address) => format!("[{address}]"),
    };
    if includes_subdomains
        && (canonical.starts_with('[') || canonical.parse::<std::net::IpAddr>().is_ok())
    {
        bail!("IP address cookies cannot include subdomains");
    }
    Ok((canonical, includes_subdomains))
}

fn validate_cookie_domain(domain: &str, target_host: &str) -> Result<(String, bool)> {
    let (canonical, includes_subdomains) = normalize_cookie_domain(domain)?;
    let matches_target = canonical == target_host
        || (includes_subdomains
            && !target_host.starts_with('[')
            && target_host.parse::<std::net::IpAddr>().is_err()
            && target_host.strip_suffix(&canonical).is_some_and(|prefix| prefix.ends_with('.')));
    if !matches_target {
        bail!("Cookie domain does not match the requested website");
    }
    if includes_subdomains && canonical.split('.').count() < 2 && canonical != target_host {
        bail!("Cookie domain is too broad");
    }
    Ok((canonical, includes_subdomains))
}

fn validate_field(value: &str, allow_empty: bool) -> Result<()> {
    if (!allow_empty && value.is_empty())
        || value.chars().any(|character| character.is_control() || character == '\t')
    {
        bail!("Cookie contains an invalid field");
    }
    Ok(())
}

fn serialize_netscape(host: &str, cookies: &[CookieRecord]) -> Result<Vec<u8>> {
    if cookies.is_empty() {
        bail!("No sign-in cookies were found for this website");
    }
    if cookies.len() > MAX_COOKIE_COUNT {
        bail!("The sign-in cookie count is too large");
    }

    let mut output = Vec::with_capacity(JAR_HEADER.len() + cookies.len() * 128);
    output.extend_from_slice(JAR_HEADER);
    for cookie in cookies {
        let (domain, include_subdomains) = validate_cookie_domain(&cookie.domain, host)?;
        validate_field(&cookie.path, false)?;
        validate_field(&cookie.name, false)?;
        validate_field(&cookie.value, true)?;
        if !cookie.path.starts_with('/') || cookie.expires < 0 {
            bail!("Cookie contains an invalid path or expiry");
        }
        if cookie.http_only {
            output.extend_from_slice(b"#HttpOnly_");
        }
        output.extend_from_slice(domain.as_bytes());
        output.push(b'\t');
        output.extend_from_slice(if include_subdomains { b"TRUE" } else { b"FALSE" });
        output.push(b'\t');
        output.extend_from_slice(cookie.path.as_bytes());
        output.push(b'\t');
        output.extend_from_slice(if cookie.secure { b"TRUE" } else { b"FALSE" });
        output.push(b'\t');
        output.extend_from_slice(cookie.expires.to_string().as_bytes());
        output.push(b'\t');
        output.extend_from_slice(cookie.name.as_bytes());
        output.push(b'\t');
        output.extend_from_slice(cookie.value.as_bytes());
        output.extend_from_slice(b"\r\n");
        if output.len() > MAX_JAR_BYTES {
            bail!("The sign-in cookie data is too large");
        }
    }
    Ok(output)
}

fn parse_netscape(host: &str, bytes: &[u8]) -> Result<Vec<CookieRecord>> {
    if bytes.len() > MAX_JAR_BYTES {
        bail!("The sign-in cookie data is too large");
    }
    let text = std::str::from_utf8(bytes).map_err(|_| anyhow!("Cookie file is not valid text"))?;
    let mut lines = text.lines();
    let header = lines.next().ok_or_else(|| anyhow!("Cookie file is empty"))?;
    if !header.starts_with("# Netscape HTTP Cookie File")
        && !header.starts_with("# HTTP Cookie File")
    {
        bail!("Cookie file has an invalid format");
    }

    let mut cookies = Vec::new();
    for line in lines {
        if line.is_empty() || (line.starts_with('#') && !line.starts_with("#HttpOnly_")) {
            continue;
        }
        let mut fields = line.split('\t');
        let raw_domain = fields.next().ok_or_else(|| anyhow!("Cookie row is incomplete"))?;
        let (domain, http_only) = raw_domain
            .strip_prefix("#HttpOnly_")
            .map_or((raw_domain, false), |value| (value, true));
        let include_subdomains =
            fields.next().ok_or_else(|| anyhow!("Cookie row is incomplete"))?;
        let path = fields.next().ok_or_else(|| anyhow!("Cookie row is incomplete"))?;
        let secure = fields.next().ok_or_else(|| anyhow!("Cookie row is incomplete"))?;
        let expires = fields.next().ok_or_else(|| anyhow!("Cookie row is incomplete"))?;
        let name = fields.next().ok_or_else(|| anyhow!("Cookie row is incomplete"))?;
        let value = fields.next().ok_or_else(|| anyhow!("Cookie row is incomplete"))?;
        if fields.next().is_some() {
            bail!("Cookie row has too many fields");
        }

        let includes_subdomains = match include_subdomains.to_ascii_uppercase().as_str() {
            "TRUE" => true,
            "FALSE" => false,
            _ => bail!("Cookie row has an invalid domain scope"),
        };
        let is_secure = match secure.to_ascii_uppercase().as_str() {
            "TRUE" => true,
            "FALSE" => false,
            _ => bail!("Cookie row has an invalid security flag"),
        };
        let expires =
            expires.parse::<i64>().map_err(|_| anyhow!("Cookie row has an invalid expiry"))?;
        let domain_for_validation =
            if includes_subdomains { format!(".{domain}") } else { domain.to_owned() };
        let (domain, _) = validate_cookie_domain(&domain_for_validation, host)?;
        let mut cookie_domain = domain;
        if includes_subdomains {
            cookie_domain.insert(0, '.');
        }
        let cookie = CookieRecord {
            domain: cookie_domain,
            path: path.to_owned(),
            name: name.to_owned(),
            value: value.to_owned(),
            secure: is_secure,
            http_only,
            expires,
        };
        validate_field(&cookie.path, false)?;
        validate_field(&cookie.name, false)?;
        validate_field(&cookie.value, true)?;
        if !cookie.path.starts_with('/') || cookie.expires < 0 {
            bail!("Cookie row has an invalid path or expiry");
        }
        cookies.push(cookie);
        if cookies.len() > MAX_COOKIE_COUNT {
            bail!("The sign-in cookie count is too large");
        }
    }
    Ok(cookies)
}

fn wrap_envelope(host: &str, jar: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(ENVELOPE_HEADER.len() + host.len() + jar.len() + 1);
    output.extend_from_slice(ENVELOPE_HEADER);
    output.extend_from_slice(host.as_bytes());
    output.push(b'\n');
    output.extend_from_slice(jar);
    output
}

fn unwrap_envelope<'a>(host: &str, bytes: &'a [u8]) -> Result<&'a [u8]> {
    let rest = bytes
        .strip_prefix(ENVELOPE_HEADER)
        .ok_or_else(|| anyhow!("Saved sign-in data has an invalid format"))?;
    let separator = rest
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or_else(|| anyhow!("Saved sign-in data has an invalid format"))?;
    if &rest[..separator] != host.as_bytes() {
        bail!("Saved sign-in data belongs to a different website");
    }
    Ok(&rest[separator + 1..])
}

fn create_unique_file(directory: &Path, kind: &str) -> Result<(PathBuf, File)> {
    for _ in 0..8 {
        let path = directory.join(format!(".{kind}-{}.tmp", Uuid::new_v4()));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("Could not create cookie storage file"),
        }
    }
    bail!("Could not allocate a unique cookie storage file")
}

fn create_unique_lease_file(directory: &Path) -> Result<(PathBuf, File)> {
    for _ in 0..8 {
        let path = directory.join(format!(".lease-{}-{}.tmp", std::process::id(), Uuid::new_v4()));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("Could not create cookie storage file"),
        }
    }
    bail!("Could not allocate a unique cookie storage file")
}

#[derive(Debug, Eq, PartialEq)]
enum LeaseOwner {
    Process(u32),
    Legacy,
}

fn parse_lease_owner(filename: &str) -> Option<LeaseOwner> {
    let name = filename.strip_prefix(".lease-")?.strip_suffix(".tmp")?;
    if Uuid::parse_str(name).is_ok() {
        return Some(LeaseOwner::Legacy);
    }
    let (pid, id) = name.split_once('-')?;
    let pid = pid.parse().ok()?;
    Uuid::parse_str(id).ok()?;
    Some(LeaseOwner::Process(pid))
}

fn should_retain_lease(owner: LeaseOwner, active_yt_dlp_parent_pids: &HashSet<u32>) -> bool {
    match owner {
        LeaseOwner::Process(owner_pid) => active_yt_dlp_parent_pids.contains(&owner_pid),
        LeaseOwner::Legacy => !active_yt_dlp_parent_pids.is_empty(),
    }
}

#[cfg(windows)]
fn active_yt_dlp_parent_pids() -> Result<HashSet<u32>> {
    use std::mem::size_of;

    use windows_sys::Win32::{
        Foundation::{CloseHandle, INVALID_HANDLE_VALUE},
        System::Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW,
            TH32CS_SNAPPROCESS,
        },
    };

    // SAFETY: the returned snapshot handle is closed on every path below, and
    // PROCESSENTRY32W is initialized with its required size before enumeration.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error())
            .context("Could not inspect running downloader processes");
    }
    let mut parents = HashSet::new();
    let mut process: PROCESSENTRY32W = unsafe { std::mem::zeroed() };
    process.dwSize = size_of::<PROCESSENTRY32W>() as u32;
    let mut ok = unsafe { Process32FirstW(snapshot, &mut process) };
    while ok != 0 {
        let end =
            process.szExeFile.iter().position(|unit| *unit == 0).unwrap_or(process.szExeFile.len());
        if String::from_utf16_lossy(&process.szExeFile[..end]).eq_ignore_ascii_case("yt-dlp.exe") {
            parents.insert(process.th32ParentProcessID);
        }
        process.dwSize = size_of::<PROCESSENTRY32W>() as u32;
        ok = unsafe { Process32NextW(snapshot, &mut process) };
    }
    let enumeration_error = std::io::Error::last_os_error();
    let close_ok = unsafe { CloseHandle(snapshot) };
    if close_ok == 0 {
        return Err(std::io::Error::last_os_error())
            .context("Could not close downloader process snapshot");
    }
    // ERROR_NO_MORE_FILES is the normal end of enumeration.
    if enumeration_error.raw_os_error().is_some_and(|code| code != 18) {
        return Err(enumeration_error).context("Could not inspect running downloader processes");
    }
    Ok(parents)
}

fn read_bounded(path: &Path, limit: u64) -> std::io::Result<Vec<u8>> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "file exceeds size limit",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "file exceeds size limit",
        ));
    }
    Ok(bytes)
}

#[cfg(windows)]
fn ensure_supported() -> Result<()> {
    Ok(())
}

#[cfg(not(windows))]
fn ensure_supported() -> Result<()> {
    bail!("Encrypted cookie storage is supported only on Windows")
}

#[cfg(windows)]
fn dpapi_protect(input: &[u8]) -> Result<Vec<u8>> {
    use std::ptr::null;
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData},
    };

    let input_blob = CRYPT_INTEGER_BLOB {
        cbData: u32::try_from(input.len()).context("Cookie data is too large")?,
        pbData: input.as_ptr().cast_mut(),
    };
    let mut output_blob = CRYPT_INTEGER_BLOB::default();
    let success = unsafe {
        CryptProtectData(
            &input_blob,
            std::ptr::null(),
            null(),
            null(),
            null(),
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output_blob,
        )
    };
    if success == 0 {
        return Err(std::io::Error::last_os_error())
            .context("Windows could not encrypt sign-in data");
    }
    let encrypted = unsafe {
        std::slice::from_raw_parts(output_blob.pbData, output_blob.cbData as usize).to_vec()
    };
    unsafe {
        LocalFree(output_blob.pbData.cast());
    }
    Ok(encrypted)
}

#[cfg(windows)]
fn dpapi_unprotect(input: &[u8]) -> Result<Vec<u8>> {
    use std::ptr::null;
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{
            CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptUnprotectData,
        },
    };

    let input_blob = CRYPT_INTEGER_BLOB {
        cbData: u32::try_from(input.len()).context("Saved sign-in data is too large")?,
        pbData: input.as_ptr().cast_mut(),
    };
    let mut output_blob = CRYPT_INTEGER_BLOB::default();
    let success = unsafe {
        CryptUnprotectData(
            &input_blob,
            std::ptr::null_mut(),
            null(),
            null(),
            null(),
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output_blob,
        )
    };
    if success == 0 {
        return Err(std::io::Error::last_os_error())
            .context("Windows could not decrypt sign-in data");
    }
    let plaintext = unsafe {
        std::slice::from_raw_parts(output_blob.pbData, output_blob.cbData as usize).to_vec()
    };
    unsafe {
        LocalFree(output_blob.pbData.cast());
    }
    Ok(plaintext)
}

#[cfg(not(windows))]
fn dpapi_protect(_input: &[u8]) -> Result<Vec<u8>> {
    ensure_supported()?;
    unreachable!()
}

#[cfg(not(windows))]
fn dpapi_unprotect(_input: &[u8]) -> Result<Vec<u8>> {
    ensure_supported()?;
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(domain: &str, http_only: bool) -> CookieRecord {
        CookieRecord {
            domain: domain.to_owned(),
            path: "/".to_owned(),
            name: "session".to_owned(),
            value: "secret-value".to_owned(),
            secure: true,
            http_only,
            expires: 1_800_000_000,
        }
    }

    #[test]
    fn netscape_format_preserves_scope_security_and_crlf() {
        let jar = serialize_netscape("media.example.com", &[record(".example.com", true)])
            .expect("serialize cookie jar");
        let text = std::str::from_utf8(&jar).expect("UTF-8 cookie jar");
        assert!(text.starts_with("# Netscape HTTP Cookie File\r\n"));
        assert!(text.contains(
            "#HttpOnly_example.com\tTRUE\t/\tTRUE\t1800000000\tsession\tsecret-value\r\n"
        ));
        assert!(!text.replace("\r\n", "").contains('\n'));

        let parsed = parse_netscape("media.example.com", &jar).expect("parse cookie jar");
        assert_eq!(parsed, vec![record(".example.com", true)]);
    }

    #[test]
    fn cookie_domain_must_be_exact_or_a_real_parent_suffix() {
        assert!(serialize_netscape("media.example.com", &[record(".example.com", false)]).is_ok());
        assert!(
            serialize_netscape("media.example.com", &[record(".evil-example.com", false)]).is_err()
        );
        assert!(serialize_netscape("media.example.com", &[record(".com", false)]).is_err());
        assert!(
            serialize_netscape("media.example.com", &[record("media.example.com.evil", false)])
                .is_err()
        );
    }

    #[test]
    fn rejects_urls_that_cannot_scope_cookies_safely() {
        assert_eq!(
            validated_url_host("https://Media.Example.com/watch").unwrap(),
            "media.example.com"
        );
        assert!(validated_url_host("file:///tmp/video").is_err());
        assert!(validated_url_host("https://user:pass@example.com/video").is_err());
        assert!(validated_url_host("https:///missing-host").is_err());
    }

    #[test]
    fn cookie_fields_cannot_inject_netscape_rows() {
        let mut bad = record("example.com", false);
        bad.value = "value\r\nattacker.example\tTRUE\t/\tFALSE\t0\tx\ty".to_owned();
        assert!(serialize_netscape("example.com", &[bad]).is_err());
    }

    #[test]
    fn envelope_is_bound_to_the_exact_host() {
        let jar = b"# Netscape HTTP Cookie File\r\n";
        let envelope = wrap_envelope("one.example", jar);
        assert_eq!(unwrap_envelope("one.example", &envelope).unwrap(), jar);
        assert!(unwrap_envelope("two.example", &envelope).is_err());
    }

    #[test]
    fn disabled_store_is_a_no_op() {
        let store = CookieStore::disabled();
        assert!(!store.has_saved_for_url("not a url").unwrap());
        assert!(!store.clear_for_url("not a url").unwrap());
        assert!(store.save_for_url("not a url", &[]).is_ok());
        assert!(store.lease_for_url("not a url").unwrap().is_none());
        assert_eq!(store.cleanup_stale_leases().unwrap(), 0);
    }

    #[test]
    fn lease_owner_requires_generated_pid_and_uuid_format() {
        let id = Uuid::new_v4();
        assert!(matches!(
            parse_lease_owner(&format!(".lease-42-{id}.tmp")),
            Some(LeaseOwner::Process(42))
        ));
        assert!(matches!(parse_lease_owner(&format!(".lease-{id}.tmp")), Some(LeaseOwner::Legacy)));
        assert_eq!(parse_lease_owner(".lease-not-a-uuid.tmp"), None);
        assert_eq!(parse_lease_owner(".lease-42-bad.tmp"), None);
        assert_eq!(parse_lease_owner("lease-42-00000000-0000-0000-0000-000000000000.tmp"), None);
    }

    #[test]
    fn stale_cleanup_retains_leases_with_active_downloader_parent() {
        let active_parents = HashSet::from([42]);
        assert!(should_retain_lease(LeaseOwner::Process(42), &active_parents));
        assert!(!should_retain_lease(LeaseOwner::Process(43), &active_parents));
        assert!(should_retain_lease(LeaseOwner::Legacy, &active_parents));
        assert!(!should_retain_lease(LeaseOwner::Legacy, &HashSet::new()));
    }

    #[cfg(windows)]
    #[test]
    fn stale_cleanup_removes_only_old_generated_lease_files() {
        let root = std::env::temp_dir().join(format!("yt-save-cookie-cleanup-{}", Uuid::new_v4()));
        let staging = root.join(".cookie-staging");
        fs::create_dir_all(&staging).unwrap();
        let stale = staging.join(format!(".lease-{}-{}.tmp", std::process::id(), Uuid::new_v4()));
        let legacy = staging.join(format!(".lease-{}.tmp", Uuid::new_v4()));
        let unrelated = staging.join("notes.txt");
        let malformed = staging.join(".lease-not-a-uuid.tmp");
        fs::write(&stale, b"sensitive").unwrap();
        fs::write(&legacy, b"legacy sensitive").unwrap();
        fs::write(&unrelated, b"keep").unwrap();
        fs::write(&malformed, b"keep").unwrap();

        let store = CookieStore::new(root.clone());
        store.active_leases.lock().unwrap().insert(stale.clone());
        let no_active_downloader_parents = HashSet::new();
        let past_cutoff = SystemTime::now().checked_sub(Duration::from_secs(24 * 60 * 60)).unwrap();
        assert_eq!(
            store
                .cleanup_stale_leases_with_active_pids(past_cutoff, &no_active_downloader_parents)
                .unwrap(),
            0
        );
        let future_cutoff = SystemTime::now() + Duration::from_secs(24 * 60 * 60);
        assert_eq!(
            store
                .cleanup_stale_leases_with_active_pids(future_cutoff, &no_active_downloader_parents)
                .unwrap(),
            1
        );
        assert!(stale.exists(), "an active lease remains available for cookie refresh");
        store.active_leases.lock().unwrap().remove(&stale);
        assert_eq!(
            store
                .cleanup_stale_leases_with_active_pids(future_cutoff, &no_active_downloader_parents)
                .unwrap(),
            1
        );
        assert!(!stale.exists());
        assert!(!legacy.exists());
        assert!(unrelated.exists());
        assert!(malformed.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    #[test]
    fn python_style_cookie_loader_can_open_jar() {
        let root = std::env::temp_dir().join(format!("yt-save-cookie-delete-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let (path, app_handle) = create_unique_file(&root, "lease").unwrap();
        fs::write(&path, b"temporary cookie data").unwrap();
        let child_bytes = python_style_read_open(&path)
            .expect("yt-dlp's Python cookie loader can open the temporary jar");
        assert_eq!(child_bytes, b"temporary cookie data");
        drop(app_handle);
        assert!(path.exists(), "the jar remains until the process lease finishes");
        fs::remove_file(&path).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    fn python_style_read_open(path: &Path) -> std::io::Result<Vec<u8>> {
        use std::io::Read as _;
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

        let mut options = OpenOptions::new();
        options.read(true).share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE);
        let mut file = options.open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    #[cfg(windows)]
    #[test]
    fn lease_drop_removes_plaintext_and_finish_refreshes_encrypted_vault() {
        let root = std::env::temp_dir().join(format!("yt-save-cookie-test-{}", Uuid::new_v4()));
        let store = CookieStore::new(root.clone());
        store
            .save_for_url("https://example.com/video", &[record("example.com", false)])
            .expect("save encrypted jar");
        let lease = store
            .lease_for_url("https://example.com/video")
            .expect("load saved jar")
            .expect("cookie lease");
        let lease_path = lease.path().to_owned();
        assert!(lease_path.exists());
        drop(lease);
        assert!(!lease_path.exists());

        let lease = store
            .lease_for_url("https://example.com/video")
            .expect("reload saved jar")
            .expect("cookie lease");
        fs::write(
            lease.path(),
            serialize_netscape("example.com", &[record("example.com", true)]).unwrap(),
        )
        .expect("update temporary cookie jar");
        lease.finish().expect("refresh encrypted jar");
        let refreshed = store
            .lease_for_url("https://example.com/video")
            .expect("load refreshed jar")
            .expect("refreshed cookie lease");
        let refreshed_text = fs::read_to_string(refreshed.path()).expect("read temporary jar");
        assert!(refreshed_text.contains("#HttpOnly_example.com"));
        drop(refreshed);
        assert!(store.clear_for_url("https://example.com/video").unwrap());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    #[test]
    fn stale_lease_cannot_overwrite_newer_save_or_restore_cleared_cookies() {
        let root = std::env::temp_dir().join(format!("yt-save-cookie-race-{}", Uuid::new_v4()));
        let store = CookieStore::new(root.clone());
        let url = "https://example.com/video";
        store.save_for_url(url, &[record("example.com", false)]).unwrap();

        let stale_lease = store.lease_for_url(url).unwrap().unwrap();
        let mut newer_cookie = record("example.com", false);
        newer_cookie.value = "newer-session-value".to_owned();
        store.save_for_url(url, &[newer_cookie]).unwrap();
        stale_lease.finish().unwrap();

        let current_lease = store.lease_for_url(url).unwrap().unwrap();
        let current_jar = fs::read(current_lease.path()).unwrap();
        assert!(
            current_jar
                .windows(b"newer-session-value".len())
                .any(|window| { window == b"newer-session-value" })
        );
        current_lease.finish().unwrap();

        let pending_lease = store.lease_for_url(url).unwrap().unwrap();
        assert!(store.clear_for_url(url).unwrap());
        pending_lease.finish().unwrap();
        assert!(!store.has_saved_for_url(url).unwrap());

        let _ = fs::remove_dir_all(root);
    }
}
