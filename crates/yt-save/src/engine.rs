use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::{ExitStatus, Output, Stdio},
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader},
    process::Command,
    sync::{Mutex as AsyncMutex, OwnedMutexGuard, RwLock as AsyncRwLock, watch},
};
use tracing::{debug, warn};

use crate::{
    cookie_auth::{CookieLease, CookieStore},
    dependencies::DependencyPaths,
    model::{
        AudioFormat, AudioQuality, DownloadMode, MediaMetadata, SubtitleFormat, VideoQuality,
        download_speed_limit_argument, is_safe_format_id, normalize_download_speed_limit,
        normalize_subtitle_languages,
    },
};

const PROGRESS_PREFIX: &str = "__YT_SAVE_PROGRESS__";
const OUTPUT_PREFIX: &str = "__YT_SAVE_OUTPUT__";
const SUBTITLE_OUTPUT_PREFIX: &str = "__YT_SAVE_SUBTITLE_OUTPUT__";
const OUTPUT_TEMPLATE: &str = "%(playlist_title|.)s/%(title)s [%(id)s].%(ext)s";
pub(crate) const PLAYLIST_PAGE_SIZE: usize = 50;

#[derive(Debug)]
struct RetryableEngineError(String);

impl std::fmt::Display for RetryableEngineError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RetryableEngineError {}

pub(crate) fn is_retryable_engine_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.downcast_ref::<RetryableEngineError>().is_some())
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DownloadOptions {
    mode: DownloadMode,
    audio_format: AudioFormat,
    audio_quality: AudioQuality,
    video_quality: VideoQuality,
    subtitle_languages: String,
    subtitle_format: SubtitleFormat,
    include_auto_subtitles: bool,
    concurrent_fragments: usize,
    download_speed_limit_bytes_per_second: u64,
    output_directory: Option<PathBuf>,
    selected_format_id: Option<String>,
}

impl DownloadOptions {
    pub(crate) fn new(
        mode: DownloadMode,
        audio_format: AudioFormat,
        audio_quality: AudioQuality,
    ) -> Self {
        Self {
            mode,
            audio_format,
            audio_quality,
            video_quality: VideoQuality::Best,
            subtitle_languages: "en".to_owned(),
            subtitle_format: SubtitleFormat::Vtt,
            include_auto_subtitles: false,
            concurrent_fragments: 1,
            download_speed_limit_bytes_per_second: 0,
            output_directory: None,
            selected_format_id: None,
        }
    }

    pub(crate) fn with_video_quality(mut self, video_quality: VideoQuality) -> Self {
        self.video_quality = video_quality;
        self
    }

    pub(crate) fn with_output_directory(mut self, output_directory: PathBuf) -> Self {
        self.output_directory = Some(output_directory);
        self
    }

    pub(crate) fn with_selected_format_id(mut self, selected_format_id: Option<String>) -> Self {
        self.selected_format_id = selected_format_id;
        self
    }

    pub(crate) fn with_concurrent_fragments(mut self, concurrent_fragments: usize) -> Self {
        self.concurrent_fragments = concurrent_fragments.clamp(1, 8);
        self
    }

    pub(crate) fn with_download_speed_limit(mut self, bytes_per_second: u64) -> Self {
        self.download_speed_limit_bytes_per_second =
            normalize_download_speed_limit(bytes_per_second);
        self
    }

    pub(crate) fn with_subtitle_options(
        mut self,
        languages: String,
        format: SubtitleFormat,
        include_auto_subtitles: bool,
    ) -> Self {
        self.subtitle_languages = languages;
        self.subtitle_format = format;
        self.include_auto_subtitles = include_auto_subtitles;
        self
    }
}

#[derive(Clone)]
pub struct DownloadEngine {
    dependencies: DependencyPaths,
    yt_dlp_path: Arc<RwLock<PathBuf>>,
    process_gate: Arc<AsyncRwLock<()>>,
    yt_dlp_processes_enabled: Arc<AtomicBool>,
    output_directory: PathBuf,
    cookie_store: CookieStore,
    cookie_host_gates: Arc<Mutex<HashMap<String, Weak<AsyncMutex<()>>>>>,
}

struct CookieProcessAuth {
    lease: CookieLease,
    _host_guard: OwnedMutexGuard<()>,
}

impl CookieProcessAuth {
    fn finish(self) -> Result<()> {
        self.lease.finish()
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ProgressUpdate {
    pub status: Option<String>,
    pub downloaded_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub estimated_total_bytes: Option<u64>,
    pub speed_bytes_per_second: Option<f64>,
    pub eta_seconds: Option<u64>,
}

impl ProgressUpdate {
    pub fn percent(&self) -> Option<f32> {
        let total = self.total_bytes.or(self.estimated_total_bytes)?;
        if total == 0 {
            return None;
        }
        let downloaded = self.downloaded_bytes.unwrap_or_default();
        Some(((downloaded as f64 / total as f64) * 100.0).clamp(0.0, 100.0) as f32)
    }
}

#[derive(Clone, Debug)]
pub struct DownloadResult {
    pub output_path: Option<String>,
}

#[derive(Debug)]
pub enum EngineOutcome<T> {
    Completed(T),
    Paused,
    Canceled,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum JobSignal {
    #[default]
    Continue,
    Pause,
    Cancel,
}

impl DownloadEngine {
    pub fn new(dependencies: DependencyPaths, output_directory: PathBuf) -> Self {
        let yt_dlp_path = Arc::new(RwLock::new(dependencies.yt_dlp.clone()));
        Self {
            dependencies,
            yt_dlp_path,
            process_gate: Arc::new(AsyncRwLock::new(())),
            yt_dlp_processes_enabled: Arc::new(AtomicBool::new(true)),
            output_directory,
            cookie_store: CookieStore::disabled(),
            cookie_host_gates: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn default_output_directory(&self) -> &Path {
        &self.output_directory
    }

    pub fn with_cookie_store(mut self, cookie_store: CookieStore) -> Self {
        self.cookie_store = cookie_store;
        self
    }

    async fn attach_cookie_lease(
        &self,
        url: &str,
        command: &mut Command,
    ) -> Result<Option<CookieProcessAuth>> {
        if url::Url::parse(url).is_ok_and(|parsed| parsed.scheme() != "https") {
            return Ok(None);
        }

        let store = self.cookie_store.clone();
        let url = url.to_owned();
        let has_saved = tokio::task::spawn_blocking({
            let store = store.clone();
            let url = url.clone();
            move || store.has_saved_for_url(&url)
        })
        .await
        .context("check saved sign-in for yt-dlp")??;
        if !has_saved {
            return Ok(None);
        }

        let host = crate::cookie_auth::validated_url_host(&url)?;
        let host_gate = {
            let mut gates =
                self.cookie_host_gates.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            gates.retain(|_, gate| gate.strong_count() > 0);
            gates.get(&host).and_then(Weak::upgrade).unwrap_or_else(|| {
                let gate = Arc::new(AsyncMutex::new(()));
                gates.insert(host.clone(), Arc::downgrade(&gate));
                gate
            })
        };
        let host_guard = host_gate.lock_owned().await;
        let lease = tokio::task::spawn_blocking(move || store.lease_for_url(&url))
            .await
            .context("prepare saved sign-in for yt-dlp")??;
        if let Some(lease) = lease.as_ref() {
            command.arg("--cookies").arg(lease.path());
        }
        Ok(lease.map(|lease| CookieProcessAuth { lease, _host_guard: host_guard }))
    }

    async fn refresh_cookie_lease(auth: Option<CookieProcessAuth>) {
        if let Some(auth) = auth
            && !matches!(tokio::task::spawn_blocking(move || auth.finish()).await, Ok(Ok(())))
        {
            warn!("yt-dlp exited but the encrypted sign-in could not be refreshed");
        }
    }

    pub fn current_yt_dlp_path(&self) -> PathBuf {
        self.yt_dlp_path.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    pub fn use_yt_dlp_path(&self, path: PathBuf) {
        *self.yt_dlp_path.write().unwrap_or_else(std::sync::PoisonError::into_inner) = path;
    }

    pub async fn lock_for_yt_dlp_update(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        self.process_gate.write().await
    }

    pub async fn lock_for_yt_dlp_process(&self) -> Result<tokio::sync::RwLockReadGuard<'_, ()>> {
        let guard = self.process_gate.read().await;
        self.ensure_yt_dlp_processes_enabled()?;
        Ok(guard)
    }

    pub fn stop_accepting_yt_dlp_processes(&self) {
        self.yt_dlp_processes_enabled.store(false, Ordering::Release);
    }

    pub fn resume_accepting_yt_dlp_processes(&self) {
        self.yt_dlp_processes_enabled.store(true, Ordering::Release);
    }

    fn ensure_yt_dlp_processes_enabled(&self) -> Result<()> {
        if !self.yt_dlp_processes_enabled.load(Ordering::Acquire) {
            bail!(
                "Yt Save is preparing an application update and has stopped starting yt-dlp processes."
            );
        }
        Ok(())
    }

    pub async fn inspect(&self, url: &str) -> Result<MediaMetadata> {
        self.inspect_with_playlist_index(url, None).await
    }

    pub(crate) async fn inspect_with_playlist_index(
        &self,
        url: &str,
        playlist_index: Option<i64>,
    ) -> Result<MediaMetadata> {
        let (_signal_sender, mut signal) = watch::channel(JobSignal::Continue);
        match self.inspect_with_playlist_index_cancellable(url, playlist_index, &mut signal).await?
        {
            EngineOutcome::Completed(metadata) => Ok(metadata),
            EngineOutcome::Paused => bail!("metadata inspection was paused"),
            EngineOutcome::Canceled => bail!("metadata inspection was canceled"),
        }
    }

    pub(crate) async fn inspect_with_playlist_index_cancellable(
        &self,
        url: &str,
        playlist_index: Option<i64>,
        signal: &mut watch::Receiver<JobSignal>,
    ) -> Result<EngineOutcome<MediaMetadata>> {
        self.inspect_with_playlist_limit_cancellable(url, playlist_index, None, Some(50), signal)
            .await
    }

    pub(crate) async fn inspect_playlist_page(
        &self,
        url: &str,
        start_index: usize,
        end_index: usize,
    ) -> Result<MediaMetadata> {
        validate_playlist_page_range(start_index, end_index)?;
        let (_signal_sender, mut signal) = watch::channel(JobSignal::Continue);
        match self
            .inspect_with_playlist_limit_cancellable(
                url,
                None,
                Some(start_index),
                Some(end_index),
                &mut signal,
            )
            .await?
        {
            EngineOutcome::Completed(metadata) => Ok(metadata),
            EngineOutcome::Paused => bail!("playlist inspection was paused"),
            EngineOutcome::Canceled => bail!("playlist inspection was canceled"),
        }
    }

    pub(crate) async fn inspect_playlist_for_subscription(
        &self,
        url: &str,
    ) -> Result<MediaMetadata> {
        let (_signal_sender, mut signal) = watch::channel(JobSignal::Continue);
        match self
            .inspect_with_playlist_limit_cancellable(url, None, None, None, &mut signal)
            .await?
        {
            EngineOutcome::Completed(metadata) => Ok(metadata),
            EngineOutcome::Paused => bail!("playlist inspection was paused"),
            EngineOutcome::Canceled => bail!("playlist inspection was canceled"),
        }
    }

    async fn inspect_with_playlist_limit_cancellable(
        &self,
        url: &str,
        playlist_index: Option<i64>,
        playlist_start: Option<usize>,
        playlist_end: Option<usize>,
        signal: &mut watch::Receiver<JobSignal>,
    ) -> Result<EngineOutcome<MediaMetadata>> {
        let _process_guard = self.process_gate.read().await;
        self.ensure_yt_dlp_processes_enabled()?;
        if signal_requested(signal) != JobSignal::Continue {
            return Ok(outcome_for_signal(signal_requested(signal)));
        }
        self.require_yt_dlp()?;
        let mut command =
            self.inspect_command(url, playlist_index, playlist_start, playlist_end)?;
        let cookie_lease = self.attach_cookie_lease(url, &mut command).await?;
        debug!(host = url_host(url), "requesting yt-dlp metadata");
        let mut child = command.spawn().context("start yt-dlp metadata extraction")?;
        let stdout = child.stdout.take().context("open yt-dlp metadata stdout")?;
        let stderr = child.stderr.take().context("open yt-dlp metadata stderr")?;
        let stdout_task = tokio::spawn(read_all(stdout));
        let stderr_task = tokio::spawn(read_all(stderr));
        let (status_result, stop_signal) = tokio::select! {
            biased;
            status = child.wait() => (status.context("wait for yt-dlp metadata"), JobSignal::Continue),
            stop_signal = wait_for_signal(signal) => (terminate_process_tree(&mut child).await, stop_signal),
        };
        let status = match status_result {
            Ok(status) => status,
            Err(error) => {
                stdout_task.abort();
                stderr_task.abort();
                let _ = stdout_task.await;
                let _ = stderr_task.await;
                if force_kill_direct_child(&mut child).await.is_ok() {
                    Self::refresh_cookie_lease(cookie_lease).await;
                }
                return Err(error);
            }
        };
        Self::refresh_cookie_lease(cookie_lease).await;
        let output = Output {
            status,
            stdout: stdout_task.await.context("join yt-dlp metadata stdout reader")??,
            stderr: stderr_task.await.context("join yt-dlp metadata stderr reader")??,
        };
        if stop_signal != JobSignal::Continue && !output.status.success() {
            return Ok(outcome_for_signal(stop_signal));
        }
        if !output.status.success() {
            let message = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            return Err(yt_dlp_error(&message));
        }
        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .context("parse yt-dlp's JSON metadata response")?;
        let metadata =
            MediaMetadata::from_yt_dlp_json_for_playlist_item(&json, url, playlist_index);
        if let Some(selected_index) = playlist_index {
            let selected_entry_found =
                json.get("playlist_index").and_then(serde_json::Value::as_i64)
                    == Some(selected_index)
                    || metadata
                        .playlist_entries
                        .iter()
                        .any(|entry| entry.playlist_index == Some(selected_index));
            if json.get("entries").and_then(serde_json::Value::as_array).is_some()
                && !selected_entry_found
            {
                bail!(
                    "yt-dlp did not return playlist item {selected_index}; it may have been removed."
                );
            }
        }
        Ok(EngineOutcome::Completed(metadata))
    }

    pub(crate) async fn download_with_playlist_index_cancellable<F>(
        &self,
        url: &str,
        playlist_index: Option<i64>,
        options: DownloadOptions,
        signal: &mut watch::Receiver<JobSignal>,
        mut on_progress: F,
    ) -> Result<EngineOutcome<DownloadResult>>
    where
        F: FnMut(ProgressUpdate) + Send,
    {
        let _process_guard = self.process_gate.read().await;
        self.ensure_yt_dlp_processes_enabled()?;
        if signal_requested(signal) != JobSignal::Continue {
            return Ok(outcome_for_signal(signal_requested(signal)));
        }
        let subtitle_only = options.mode == DownloadMode::SubtitlesOnly;
        let subtitle_output_directory =
            options.output_directory.clone().unwrap_or_else(|| self.output_directory.clone());
        self.require_yt_dlp()?;
        let mut command = self.download_command(url, playlist_index, options)?;
        let mut cookie_lease = self.attach_cookie_lease(url, &mut command).await?;
        debug!(host = url_host(url), "starting yt-dlp download");
        let mut child = command.spawn().context("start yt-dlp download")?;
        let stdout = child.stdout.take().context("open yt-dlp stdout")?;
        let stderr = child.stderr.take().context("open yt-dlp stderr")?;
        let stderr_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut bytes = Vec::new();
            let _ = reader.read_to_end(&mut bytes).await;
            String::from_utf8_lossy(&bytes).into_owned()
        });

        let mut output_path = None;
        let mut lines = BufReader::new(stdout).lines();
        let mut stop_signal = JobSignal::Continue;
        loop {
            tokio::select! {
                biased;
                requested = wait_for_signal(signal) => {
                    stop_signal = requested;
                    if let Err(error) = terminate_process_tree(&mut child).await {
                        let stopped = force_kill_direct_child(&mut child).await.is_ok();
                        if stopped {
                            Self::refresh_cookie_lease(cookie_lease.take()).await;
                        } else {
                            warn!("could not confirm yt-dlp stopped; temporary sign-in cleanup is pending");
                        }
                        stderr_task.abort();
                        let _ = stderr_task.await;
                        return Err(error);
                    }
                    break;
                }
                line = lines.next_line() => {
                    match line {
                        Ok(Some(line)) => process_download_line(&line, &mut output_path, &mut on_progress),
                        Ok(None) => break,
                        Err(error) => {
                            let read_error = anyhow::Error::from(error).context("read yt-dlp output");
                            let stopped = match terminate_process_tree(&mut child).await {
                                Ok(_) => true,
                                Err(_) => force_kill_direct_child(&mut child).await.is_ok(),
                            };
                            if stopped {
                                Self::refresh_cookie_lease(cookie_lease.take()).await;
                            } else {
                                warn!("could not confirm yt-dlp stopped; temporary sign-in cleanup is pending");
                            }
                            stderr_task.abort();
                            let _ = stderr_task.await;
                            return Err(read_error);
                        }
                    }
                }
            }
        }
        let status_result = if stop_signal != JobSignal::Continue {
            child.wait().await.context("wait for yt-dlp")
        } else {
            tokio::select! {
                biased;
                requested = wait_for_signal(signal) => {
                    stop_signal = requested;
                    terminate_process_tree(&mut child).await
                }
                status = child.wait() => status.context("wait for yt-dlp"),
            }
        };
        let status = match status_result {
            Ok(status) => status,
            Err(error) => {
                let stopped = force_kill_direct_child(&mut child).await.is_ok();
                if stopped {
                    Self::refresh_cookie_lease(cookie_lease.take()).await;
                } else {
                    warn!("could not confirm yt-dlp stopped; temporary sign-in cleanup is pending");
                }
                stderr_task.abort();
                let _ = stderr_task.await;
                return Err(error);
            }
        };
        Self::refresh_cookie_lease(cookie_lease.take()).await;
        if stop_signal != JobSignal::Continue {
            while let Some(line) = lines.next_line().await.context("read yt-dlp output")? {
                process_download_line(&line, &mut output_path, &mut on_progress);
            }
        }
        let stderr = stderr_task.await.unwrap_or_default();
        if stop_signal != JobSignal::Continue && !status.success() {
            return Ok(outcome_for_signal(stop_signal));
        }
        if !status.success() {
            warn!(code = ?status.code(), "yt-dlp download failed");
            return Err(yt_dlp_error(stderr.trim()));
        }
        if subtitle_only {
            let path = output_path.take().ok_or_else(|| {
                anyhow::anyhow!(
                    "yt-dlp exited successfully but wrote no matching subtitle sidecar. Check the selected language and caption type."
                )
            })?;
            let path = PathBuf::from(path);
            let path = if path.is_absolute() { path } else { subtitle_output_directory.join(path) };
            if !path.is_file() {
                bail!(
                    "yt-dlp reported a subtitle path that does not exist: {}",
                    path.file_name().and_then(|name| name.to_str()).unwrap_or("subtitle")
                );
            }
            output_path = Some(path.to_string_lossy().into_owned());
        }
        Ok(EngineOutcome::Completed(DownloadResult { output_path }))
    }

    fn base_command(&self) -> Command {
        let mut command = Command::new(self.current_yt_dlp_path());
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if self.dependencies.deno.is_file() {
            command.arg("--js-runtimes").arg(format!("deno:{}", self.dependencies.deno.display()));
        }
        if self.dependencies.ffmpeg.is_file() {
            command.arg("--ffmpeg-location").arg(&self.dependencies.directory);
        }
        #[cfg(windows)]
        command.creation_flags(0x0800_0000);
        command
    }

    fn require_yt_dlp(&self) -> Result<()> {
        let path = self.current_yt_dlp_path();
        if !path.is_file() {
            bail!("The bundled yt-dlp executable is missing.");
        }
        Ok(())
    }

    fn download_command(
        &self,
        url: &str,
        playlist_index: Option<i64>,
        options: DownloadOptions,
    ) -> Result<Command> {
        let output_directory =
            options.output_directory.as_deref().unwrap_or(&self.output_directory);
        validate_output_directory(output_directory)?;
        if let Some(format_id) = options.selected_format_id.as_deref() {
            if !is_safe_format_id(format_id) {
                bail!(
                    "The selected format ID is invalid. Analyze the link again and choose another format."
                );
            }
            if options.mode == DownloadMode::SubtitlesOnly {
                bail!("Exact media formats cannot be selected for subtitle-only downloads.");
            }
        }
        let mut command = self.base_command();
        command
            .arg("--no-warnings")
            .arg("--continue")
            .arg("--no-overwrites")
            .arg("--newline")
            .arg("--concurrent-fragments")
            .arg(options.concurrent_fragments.to_string())
            .arg("--no-color")
            .arg("--progress")
            .arg("--progress-template")
            .arg(format!(
                "download:{PROGRESS_PREFIX}%(progress.status)s|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|%(progress.total_bytes_estimate)s|%(progress.speed)s|%(progress.eta)s"
            ))
            .arg("--paths")
            .arg(format!("home:{}", output_directory.display()))
            .arg("--output")
            .arg(OUTPUT_TEMPLATE);
        if let Some(rate) =
            download_speed_limit_argument(options.download_speed_limit_bytes_per_second)
        {
            command.arg("--limit-rate").arg(rate);
        }
        match options.mode {
            DownloadMode::Video => {
                command
                    .arg("--format")
                    .arg(
                        options
                            .selected_format_id
                            .as_deref()
                            .unwrap_or_else(|| options.video_quality.yt_dlp_format()),
                    )
                    .arg("--print")
                    .arg(format!("after_move:{OUTPUT_PREFIX}%(filepath)s"));
            }
            DownloadMode::VideoOnly => {
                command
                    .arg("--format")
                    .arg(
                        options
                            .selected_format_id
                            .as_deref()
                            .unwrap_or_else(|| options.video_quality.yt_dlp_video_only_format()),
                    )
                    .arg("--print")
                    .arg(format!("after_move:{OUTPUT_PREFIX}%(filepath)s"));
            }
            DownloadMode::AudioOnly => {
                command
                    .arg("--format")
                    .arg(options.selected_format_id.as_deref().unwrap_or("bestaudio"));
                if let Some(format) = options.audio_format.yt_dlp_name() {
                    let missing = match (
                        self.dependencies.ffmpeg.is_file(),
                        self.dependencies.ffprobe.is_file(),
                    ) {
                        (false, false) => Some("FFmpeg and FFprobe are missing"),
                        (false, true) => Some("FFmpeg is missing"),
                        (true, false) => Some("FFprobe is missing"),
                        (true, true) => None,
                    };
                    if let Some(missing) = missing {
                        bail!(
                            "Converting audio to {} requires the bundled FFmpeg and FFprobe tools; {missing}.",
                            options.audio_format.label()
                        );
                    }
                    command.arg("--extract-audio").arg("--audio-format").arg(format);
                    if options.audio_format.supports_quality_setting() {
                        command.arg("--audio-quality").arg(options.audio_quality.yt_dlp_argument());
                    }
                }
                command.arg("--print").arg(format!("after_move:{OUTPUT_PREFIX}%(filepath)s"));
            }
            DownloadMode::SubtitlesOnly => {
                if playlist_index.is_some() {
                    bail!(
                        "Subtitle-only playlist downloads are not supported yet; add one media URL at a time."
                    );
                }
                if !self.dependencies.ffmpeg.is_file() {
                    bail!(
                        "Converting subtitles to SRT or VTT requires the bundled FFmpeg executable, which is unavailable."
                    );
                }
                let languages = normalize_subtitle_languages(&options.subtitle_languages)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Enter one or more subtitle language codes, such as en or en,fr."
                        )
                    })?;
                command.arg("--skip-download").arg("--write-subs");
                if options.include_auto_subtitles {
                    command.arg("--write-auto-subs");
                }
                let format = options.subtitle_format.as_database_value();
                command
                    .arg("--sub-langs")
                    .arg(languages)
                    .arg("--sub-format")
                    .arg(format!("{format}/best"))
                    .arg("--convert-subs")
                    .arg(format)
                    .arg("--print")
                    .arg(format!("after_move:{SUBTITLE_OUTPUT_PREFIX}%(requested_subtitles)j"));
            }
        }
        add_playlist_item_argument(&mut command, playlist_index)?;
        command.arg(url);
        Ok(command)
    }

    fn inspect_command(
        &self,
        url: &str,
        playlist_index: Option<i64>,
        playlist_start: Option<usize>,
        playlist_end: Option<usize>,
    ) -> Result<Command> {
        match (playlist_start, playlist_end) {
            (Some(start), Some(end)) => validate_playlist_page_range(start, end)?,
            (Some(_), None) => bail!("a playlist page requires both start and end indexes"),
            (None, _) => {}
        }
        let mut command = self.base_command();
        command.arg("--no-warnings").arg("--skip-download").arg("--flat-playlist");
        if let Some(playlist_start) = playlist_start {
            command.arg("--playlist-start").arg(playlist_start.to_string());
        }
        if let Some(playlist_end) = playlist_end {
            command.arg("--playlist-end").arg(playlist_end.to_string());
        }
        add_playlist_item_argument(&mut command, playlist_index)?;
        command.arg("--dump-single-json").arg(url);
        Ok(command)
    }
}

fn validate_output_directory(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        bail!("download destination must be an absolute path: {}", path.display());
    }
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("download destination is unavailable: {}", path.display()))?;
    if !metadata.is_dir() {
        bail!("download destination is not a folder: {}", path.display());
    }
    Ok(())
}

fn validate_playlist_page_range(start_index: usize, end_index: usize) -> Result<()> {
    if start_index == 0 {
        bail!("playlist indexes start at 1");
    }
    let page_size = end_index
        .checked_sub(start_index)
        .and_then(|width| width.checked_add(1))
        .ok_or_else(|| anyhow::anyhow!("playlist end index must be at or after its start index"))?;
    if page_size > PLAYLIST_PAGE_SIZE {
        bail!("playlist pages cannot contain more than {PLAYLIST_PAGE_SIZE} entries");
    }
    Ok(())
}

fn process_download_line<F>(line: &str, output_path: &mut Option<String>, on_progress: &mut F)
where
    F: FnMut(ProgressUpdate),
{
    if let Some(encoded) = line.strip_prefix(PROGRESS_PREFIX) {
        if let Some(progress) = parse_progress_line(encoded) {
            on_progress(progress);
        }
    } else if let Some(path) = line.strip_prefix(OUTPUT_PREFIX) {
        *output_path = Some(path.to_owned());
    } else if let Some(subtitles) = line.strip_prefix(SUBTITLE_OUTPUT_PREFIX) {
        if let Some(path) = parse_requested_subtitle_path(subtitles) {
            *output_path = Some(path);
        }
    } else if !line.trim().is_empty() {
        debug!("yt-dlp emitted status output");
    }
}

fn parse_requested_subtitle_path(value: &str) -> Option<String> {
    let requested: serde_json::Value = serde_json::from_str(value).ok()?;
    requested.as_object()?.values().find_map(|subtitle| {
        subtitle
            .get("filepath")
            .or_else(|| subtitle.get("filename"))
            .and_then(serde_json::Value::as_str)
            .filter(|path| !path.trim().is_empty())
            .map(str::to_owned)
    })
}

async fn read_all<R>(mut reader: R) -> std::io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

fn signal_requested(signal: &watch::Receiver<JobSignal>) -> JobSignal {
    *signal.borrow()
}

fn outcome_for_signal<T>(signal: JobSignal) -> EngineOutcome<T> {
    match signal {
        JobSignal::Pause => EngineOutcome::Paused,
        JobSignal::Cancel => EngineOutcome::Canceled,
        JobSignal::Continue => unreachable!("continue is not a stop request"),
    }
}

async fn wait_for_signal(signal: &mut watch::Receiver<JobSignal>) -> JobSignal {
    loop {
        let requested = signal_requested(signal);
        if requested != JobSignal::Continue {
            return requested;
        }
        if signal.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

async fn terminate_process_tree(child: &mut tokio::process::Child) -> Result<ExitStatus> {
    #[cfg(windows)]
    {
        if let Some(pid) = child.id() {
            let mut taskkill = Command::new(taskkill_executable());
            taskkill
                .args(taskkill_arguments(pid))
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .creation_flags(0x0800_0000);
            match tokio::time::timeout(std::time::Duration::from_secs(10), taskkill.status()).await
            {
                Ok(Ok(status)) if status.success() => {
                    return child.wait().await.context("reap terminated yt-dlp process tree");
                }
                Ok(Ok(status)) => {
                    if let Some(child_status) =
                        child.try_wait().context("check yt-dlp after taskkill failure")?
                        && child_status.success()
                    {
                        return Ok(child_status);
                    }
                    let reason = format!("taskkill exited with status {:?}", status.code());
                    force_kill_direct_child(child).await?;
                    bail!("{reason}; yt-dlp stopped, but its process tree could not be confirmed")
                }
                Ok(Err(error)) => {
                    if child
                        .try_wait()
                        .context("check yt-dlp after taskkill launch error")?
                        .is_some_and(|status| status.success())
                    {
                        return child.wait().await.context("reap completed yt-dlp process");
                    }
                    force_kill_direct_child(child).await?;
                    bail!("could not launch taskkill to stop yt-dlp's process tree: {error}")
                }
                Err(_) => {
                    force_kill_direct_child(child).await?;
                    bail!(
                        "taskkill timed out; yt-dlp stopped, but its process tree could not be confirmed"
                    )
                }
            }
        }
    }

    force_kill_direct_child(child).await
}

async fn force_kill_direct_child(child: &mut tokio::process::Child) -> Result<ExitStatus> {
    if child.try_wait().context("check yt-dlp before termination")?.is_none() {
        child.start_kill().context("send termination to yt-dlp")?;
    }
    child.wait().await.context("reap terminated yt-dlp process")
}

#[cfg(windows)]
fn taskkill_executable() -> PathBuf {
    std::env::var_os("SystemRoot")
        .map(|root| PathBuf::from(root).join("System32").join("taskkill.exe"))
        .unwrap_or_else(|| PathBuf::from("taskkill.exe"))
}

#[cfg(any(windows, test))]
fn taskkill_arguments(pid: u32) -> [String; 4] {
    ["/PID".to_owned(), pid.to_string(), "/T".to_owned(), "/F".to_owned()]
}

fn add_playlist_item_argument(command: &mut Command, playlist_index: Option<i64>) -> Result<()> {
    if let Some(index) = playlist_index {
        if index <= 0 {
            bail!("playlist item index must be a positive integer");
        }
        command.arg("--playlist-items").arg(index.to_string());
    }
    Ok(())
}

fn parse_progress_line(value: &str) -> Option<ProgressUpdate> {
    let mut fields = value.split('|');
    Some(ProgressUpdate {
        status: parse_text(fields.next()?),
        downloaded_bytes: parse_number(fields.next()?),
        total_bytes: parse_number(fields.next()?),
        estimated_total_bytes: parse_number(fields.next()?),
        speed_bytes_per_second: parse_number(fields.next()?),
        eta_seconds: parse_number(fields.next()?),
    })
}

fn parse_text(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty() && value != "NA").then(|| value.to_owned())
}

fn parse_number<T: std::str::FromStr>(value: &str) -> Option<T> {
    value.trim().parse().ok()
}

fn yt_dlp_error(value: &str) -> anyhow::Error {
    let message = readable_error(value);
    if is_transient_yt_dlp_error(value) {
        anyhow::Error::new(RetryableEngineError(message))
    } else {
        anyhow::anyhow!(message)
    }
}

fn is_transient_yt_dlp_error(value: &str) -> bool {
    let message = value.to_ascii_lowercase();
    message.contains("http error 408")
        || message.contains("http error 429")
        || message.contains("http error 5")
        || message.contains("too many requests")
        || message.contains("rate limit")
        || message.contains("temporarily unavailable")
        || message.contains("temporary failure")
        || message.contains("timed out")
        || message.contains("timeout")
        || message.contains("connection reset")
        || message.contains("connection refused")
        || message.contains("connection aborted")
        || message.contains("remote end closed")
        || message.contains("network is unreachable")
        || message.contains("network unreachable")
        || message.contains("temporary failure in name resolution")
        || message.contains("getaddrinfo failed")
        || message.contains("name or service not known")
        || message.contains("could not resolve host")
}

fn readable_error(value: &str) -> String {
    let message = value.to_ascii_lowercase();
    if is_transient_yt_dlp_error(value) {
        "A temporary network, server, or rate limit error interrupted this media request."
            .to_owned()
    } else if message.contains("unsupported url") || message.contains("no suitable extractor") {
        "yt-dlp does not currently support this URL.".to_owned()
    } else if message.contains("ffmpeg not found")
        || message.contains("ffprobe not found")
        || message.contains("ffmpeg-location")
    {
        "Media processing needs the bundled FFmpeg and FFprobe tools, but yt-dlp could not locate them. Restore the bundled media tools and try again.".to_owned()
    } else if message.contains("http error 404") || message.contains("not found") {
        "The media was not found. Check the URL and try again.".to_owned()
    } else if message.contains("http error 403") || message.contains("forbidden") {
        "The site denied access to this media. It may require an account or be unavailable in your region.".to_owned()
    } else if message.contains("private")
        || message.contains("sign in")
        || message.contains("login required")
    {
        "This media may require an account or may be private.".to_owned()
    } else if message.contains("requested format is not available")
        || message.contains("requested formats are not available")
    {
        "No available media format matches the selected download mode and quality. Choose a lower quality or another mode.".to_owned()
    } else if message.trim().is_empty() {
        "yt-dlp exited without an error message.".to_owned()
    } else {
        "yt-dlp could not process this media. Check that it is publicly accessible and try again."
            .to_owned()
    }
}

fn url_host(value: &str) -> &str {
    // URLs are validated by the app before reaching the engine. If a caller bypasses
    // that layer, avoid logging the raw URL or its query parameters.
    value
        .split_once("://")
        .and_then(|(_, remainder)| remainder.split('/').next())
        .and_then(|authority| authority.rsplit('@').next())
        .and_then(|authority| authority.split(':').next())
        .filter(|host| !host.is_empty())
        .unwrap_or("unknown")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dependencies::DependencyPaths;

    #[cfg(windows)]
    #[tokio::test]
    async fn yt_dlp_receives_a_temporary_cookie_path_without_cookie_values_in_arguments() {
        use crate::cookie_auth::CookieRecord;

        let root =
            std::env::temp_dir().join(format!("yt-save-engine-cookie-{}", uuid::Uuid::new_v4()));
        let cookie_store = CookieStore::new(root.clone());
        cookie_store
            .save_for_url(
                "https://media.example.test/watch",
                &[CookieRecord {
                    domain: "media.example.test".to_owned(),
                    path: "/".to_owned(),
                    name: "session".to_owned(),
                    value: "sensitive-session-value".to_owned(),
                    secure: true,
                    http_only: true,
                    expires: 1_800_000_000,
                }],
            )
            .expect("protect cookie jar");
        let engine = DownloadEngine::new(
            DependencyPaths {
                directory: PathBuf::new(),
                yt_dlp: PathBuf::new(),
                deno: PathBuf::new(),
                ffmpeg: PathBuf::new(),
                ffprobe: PathBuf::new(),
            },
            PathBuf::new(),
        )
        .with_cookie_store(cookie_store);
        let mut command = engine.base_command();
        let lease = engine
            .attach_cookie_lease("https://media.example.test/watch", &mut command)
            .await
            .expect("prepare cookie lease")
            .expect("saved cookie lease");
        let expected_path = lease.lease.path().to_string_lossy().into_owned();
        let arguments = command
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let cookies_index = arguments.iter().position(|argument| argument == "--cookies").unwrap();
        assert_eq!(arguments.get(cookies_index + 1), Some(&expected_path));
        assert!(!arguments.iter().any(|argument| argument.contains("sensitive-session-value")));

        let second_engine = engine.clone();
        let mut second_lease = tokio::spawn(async move {
            let mut second_command = second_engine.base_command();
            second_engine
                .attach_cookie_lease("https://media.example.test/another", &mut second_command)
                .await
                .expect("prepare second cookie lease")
                .expect("second saved cookie lease")
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut second_lease)
                .await
                .is_err()
        );
        lease.finish().expect("refresh protected cookie jar");
        second_lease
            .await
            .expect("second cookie lease task")
            .finish()
            .expect("refresh second protected cookie jar");
        std::fs::remove_dir_all(root).expect("remove test cookie files");
    }

    #[tokio::test]
    async fn update_shutdown_rejects_late_process_starts_and_can_recover() {
        let engine = DownloadEngine::new(
            DependencyPaths {
                directory: PathBuf::new(),
                yt_dlp: PathBuf::new(),
                deno: PathBuf::new(),
                ffmpeg: PathBuf::new(),
                ffprobe: PathBuf::new(),
            },
            PathBuf::new(),
        );

        assert!(engine.ensure_yt_dlp_processes_enabled().is_ok());
        engine.stop_accepting_yt_dlp_processes();
        assert!(engine.ensure_yt_dlp_processes_enabled().is_err());
        assert!(engine.lock_for_yt_dlp_process().await.is_err());
        engine.resume_accepting_yt_dlp_processes();
        assert!(engine.ensure_yt_dlp_processes_enabled().is_ok());
        assert!(engine.lock_for_yt_dlp_process().await.is_ok());
    }

    #[test]
    fn parses_machine_readable_progress_without_fabricating_totals() {
        let update = parse_progress_line("downloading|500|1000|NA|250|2").unwrap();
        assert_eq!(update.downloaded_bytes, Some(500));
        assert_eq!(update.total_bytes, Some(1000));
        assert_eq!(update.percent(), Some(50.0));

        let unknown = parse_progress_line("downloading|500|NA|NA|NA|NA").unwrap();
        assert_eq!(unknown.percent(), None);
        assert_eq!(unknown.speed_bytes_per_second, None);
    }

    #[test]
    fn windows_process_tree_termination_uses_pid_argument_without_shell_interpolation() {
        assert_eq!(
            taskkill_arguments(4242),
            ["/PID".to_owned(), "4242".to_owned(), "/T".to_owned(), "/F".to_owned(),]
        );
    }

    #[test]
    fn stop_signals_produce_distinct_paused_and_canceled_outcomes() {
        assert!(matches!(outcome_for_signal::<()>(JobSignal::Pause), EngineOutcome::Paused));
        assert!(matches!(outcome_for_signal::<()>(JobSignal::Cancel), EngineOutcome::Canceled));
    }

    #[test]
    fn missing_deno_does_not_block_non_youtube_extractors() {
        let root = std::env::temp_dir().join(format!("yt-save-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let yt_dlp = root.join("yt-dlp.exe");
        std::fs::write(&yt_dlp, b"test executable placeholder").unwrap();
        let dependencies = DependencyPaths {
            directory: root.clone(),
            yt_dlp,
            deno: root.join("deno.exe"),
            ffmpeg: root.join("ffmpeg.exe"),
            ffprobe: root.join("ffprobe.exe"),
        };
        let engine = DownloadEngine::new(dependencies, root.clone());
        let arguments = engine
            .base_command()
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(!arguments.iter().any(|argument| argument == "--js-runtimes"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn playlist_page_inspection_uses_global_ranges_and_rejects_unsafe_ranges() {
        let root = std::env::temp_dir().join(format!("yt-save-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let yt_dlp = root.join("yt-dlp.exe");
        std::fs::write(&yt_dlp, b"test executable placeholder").unwrap();
        let dependencies = DependencyPaths {
            directory: root.clone(),
            yt_dlp,
            deno: root.join("deno.exe"),
            ffmpeg: root.join("ffmpeg.exe"),
            ffprobe: root.join("ffprobe.exe"),
        };
        let engine = DownloadEngine::new(dependencies, root.clone());
        let args = |start, end| {
            engine
                .inspect_command("https://example.test/playlist", None, Some(start), Some(end))
                .unwrap()
                .as_std()
                .get_args()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
        };

        for (start, end) in [(51, 100), (101, 150)] {
            let arguments = args(start, end);
            let start_flag =
                arguments.iter().position(|argument| argument == "--playlist-start").unwrap();
            let end_flag =
                arguments.iter().position(|argument| argument == "--playlist-end").unwrap();
            assert_eq!(
                arguments.get(start_flag + 1).map(String::as_str),
                Some(start.to_string().as_str())
            );
            assert_eq!(
                arguments.get(end_flag + 1).map(String::as_str),
                Some(end.to_string().as_str())
            );
        }

        assert!(
            engine.inspect_command("https://example.test/list", None, Some(0), Some(50)).is_err()
        );
        assert!(
            engine.inspect_command("https://example.test/list", None, Some(51), Some(50)).is_err()
        );
        assert!(
            engine.inspect_command("https://example.test/list", None, Some(1), Some(51)).is_err()
        );
        assert!(engine.inspect_command("https://example.test/list", None, Some(1), None).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bundled_deno_is_passed_to_yt_dlp_for_full_javascript_support() {
        let root = std::env::temp_dir().join(format!("yt-save-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let yt_dlp = root.join("yt-dlp.exe");
        let deno = root.join("deno.exe");
        std::fs::write(&yt_dlp, b"test executable placeholder").unwrap();
        std::fs::write(&deno, b"test runtime placeholder").unwrap();
        let dependencies = DependencyPaths {
            directory: root.clone(),
            yt_dlp,
            deno: deno.clone(),
            ffmpeg: root.join("ffmpeg.exe"),
            ffprobe: root.join("ffprobe.exe"),
        };
        let engine = DownloadEngine::new(dependencies, root.clone());
        let arguments = engine
            .base_command()
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(arguments.first().map(String::as_str), Some("--js-runtimes"));
        let expected_runtime = format!("deno:{}", deno.display());
        assert_eq!(arguments.get(1).map(String::as_str), Some(expected_runtime.as_str()));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn download_command_uses_playlist_folder_fallback_and_configured_output_path() {
        let root = std::env::temp_dir().join(format!("yt-save-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let yt_dlp = root.join("yt-dlp.exe");
        std::fs::write(&yt_dlp, b"test executable placeholder").unwrap();
        let output_directory = root.join("downloads");
        let dependencies = DependencyPaths {
            directory: root.clone(),
            yt_dlp,
            deno: root.join("deno.exe"),
            ffmpeg: root.join("ffmpeg.exe"),
            ffprobe: root.join("ffprobe.exe"),
        };
        let engine = DownloadEngine::new(dependencies, output_directory.clone());
        std::fs::create_dir_all(&output_directory).unwrap();
        let arguments = engine
            .download_command(
                "https://example.test/watch/123",
                None,
                DownloadOptions::default().with_concurrent_fragments(4),
            )
            .unwrap()
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(!arguments.iter().any(|argument| argument == "--playlist-items"));
        assert!(arguments.iter().any(|argument| argument == "--continue"));
        assert!(!arguments.iter().any(|argument| argument == "--limit-rate"));
        let fragment_flag =
            arguments.iter().position(|argument| argument == "--concurrent-fragments").unwrap();
        assert_eq!(arguments.get(fragment_flag + 1).map(String::as_str), Some("4"));

        let limited_arguments = engine
            .download_command(
                "https://example.test/watch/123",
                None,
                DownloadOptions::default().with_download_speed_limit(2_000_000),
            )
            .unwrap()
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let rate_flag =
            limited_arguments.iter().position(|argument| argument == "--limit-rate").unwrap();
        assert_eq!(limited_arguments.get(rate_flag + 1).map(String::as_str), Some("2000000"));

        let output_flag = arguments.iter().position(|argument| argument == "--output").unwrap();
        assert_eq!(arguments.get(output_flag + 1).map(String::as_str), Some(OUTPUT_TEMPLATE));
        let paths_flag = arguments.iter().position(|argument| argument == "--paths").unwrap();
        assert_eq!(
            arguments.get(paths_flag + 1).map(String::as_str),
            Some(format!("home:{}", output_directory.display()).as_str())
        );
        assert_eq!(arguments.last().map(String::as_str), Some("https://example.test/watch/123"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn subtitle_command_selects_language_type_and_converts_to_requested_sidecar_format() {
        let root = std::env::temp_dir().join(format!("yt-save-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let yt_dlp = root.join("yt-dlp.exe");
        std::fs::write(&yt_dlp, b"test executable placeholder").unwrap();
        std::fs::write(root.join("ffmpeg.exe"), b"test ffmpeg placeholder").unwrap();
        let dependencies = DependencyPaths {
            directory: root.clone(),
            yt_dlp,
            deno: root.join("deno.exe"),
            ffmpeg: root.join("ffmpeg.exe"),
            ffprobe: root.join("ffprobe.exe"),
        };
        let engine = DownloadEngine::new(dependencies, root.clone());
        let arguments = engine
            .download_command(
                "https://example.test/watch/123",
                None,
                DownloadOptions::new(
                    DownloadMode::SubtitlesOnly,
                    AudioFormat::Original,
                    AudioQuality::Best,
                )
                .with_subtitle_options(
                    "en,fr".to_owned(),
                    SubtitleFormat::Srt,
                    true,
                ),
            )
            .unwrap()
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        for flag in ["--skip-download", "--write-subs", "--write-auto-subs"] {
            assert!(arguments.iter().any(|argument| argument == flag));
        }
        assert!(!arguments.iter().any(|argument| argument == "--format"));
        let language_flag =
            arguments.iter().position(|argument| argument == "--sub-langs").unwrap();
        assert_eq!(arguments.get(language_flag + 1).map(String::as_str), Some("en,fr"));
        let format_flag =
            arguments.iter().position(|argument| argument == "--convert-subs").unwrap();
        assert_eq!(arguments.get(format_flag + 1).map(String::as_str), Some("srt"));
        assert!(arguments.iter().any(|argument| argument.contains("requested_subtitles")));
        assert!(
            engine
                .download_command(
                    "https://example.test/playlist",
                    Some(2),
                    DownloadOptions::new(
                        DownloadMode::SubtitlesOnly,
                        AudioFormat::Original,
                        AudioQuality::Best
                    ),
                )
                .is_err()
        );
        assert!(
            engine
                .download_command(
                    "https://example.test/watch/123",
                    None,
                    DownloadOptions::new(
                        DownloadMode::SubtitlesOnly,
                        AudioFormat::Original,
                        AudioQuality::Best
                    )
                    .with_subtitle_options(
                        "en;--exec".to_owned(),
                        SubtitleFormat::Vtt,
                        false
                    ),
                )
                .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn requested_subtitle_output_marker_extracts_a_sidecar_path() {
        let value = r#"{"en":{"ext":"srt","filepath":"C:/Media/clip.en.srt"}}"#;
        let mut output_path = None;
        process_download_line(
            &format!("{SUBTITLE_OUTPUT_PREFIX}{value}"),
            &mut output_path,
            &mut |_| {},
        );
        assert_eq!(output_path.as_deref(), Some("C:/Media/clip.en.srt"));
        assert_eq!(parse_requested_subtitle_path("not json"), None);
    }

    #[test]
    fn download_command_uses_per_job_destination_and_rejects_missing_folders() {
        let root = std::env::temp_dir().join(format!("yt-save-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let yt_dlp = root.join("yt-dlp.exe");
        std::fs::write(&yt_dlp, b"test executable placeholder").unwrap();
        let default_directory = root.join("default");
        let selected_directory = root.join("selected");
        std::fs::create_dir_all(&default_directory).unwrap();
        std::fs::create_dir_all(&selected_directory).unwrap();
        let dependencies = DependencyPaths {
            directory: root.clone(),
            yt_dlp,
            deno: root.join("deno.exe"),
            ffmpeg: root.join("ffmpeg.exe"),
            ffprobe: root.join("ffprobe.exe"),
        };
        let engine = DownloadEngine::new(dependencies, default_directory.clone());
        let args = engine
            .download_command(
                "https://example.test/watch/123",
                None,
                DownloadOptions::default().with_output_directory(selected_directory.clone()),
            )
            .unwrap()
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let paths_flag = args.iter().position(|argument| argument == "--paths").unwrap();
        assert_eq!(
            args.get(paths_flag + 1).map(String::as_str),
            Some(format!("home:{}", selected_directory.display()).as_str())
        );

        let missing_directory = root.join("missing");
        assert!(
            engine
                .download_command(
                    "https://example.test/watch/123",
                    None,
                    DownloadOptions::default().with_output_directory(missing_directory),
                )
                .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn selected_playlist_item_index_is_passed_to_inspection_and_download_only_for_selected_jobs() {
        let root = std::env::temp_dir().join(format!("yt-save-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let yt_dlp = root.join("yt-dlp.exe");
        std::fs::write(&yt_dlp, b"test executable placeholder").unwrap();
        let dependencies = DependencyPaths {
            directory: root.clone(),
            yt_dlp,
            deno: root.join("deno.exe"),
            ffmpeg: root.join("ffmpeg.exe"),
            ffprobe: root.join("ffprobe.exe"),
        };
        let engine = DownloadEngine::new(dependencies, root.clone());
        let args = |command: Command| {
            command
                .as_std()
                .get_args()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
        };

        let ordinary_inspection = args(
            engine.inspect_command("https://example.test/playlist", None, None, Some(50)).unwrap(),
        );
        assert!(!ordinary_inspection.iter().any(|arg| arg == "--playlist-items"));

        let selected_inspection = args(
            engine
                .inspect_command("https://example.test/playlist", Some(7), None, Some(50))
                .unwrap(),
        );
        let inspection_end =
            selected_inspection.iter().position(|arg| arg == "--playlist-end").unwrap();
        assert_eq!(selected_inspection.get(inspection_end + 1).map(String::as_str), Some("50"));
        let subscription_inspection = args(
            engine.inspect_command("https://example.test/playlist", None, None, None).unwrap(),
        );
        assert!(!subscription_inspection.iter().any(|arg| arg == "--playlist-end"));
        let selected_download = args(
            engine
                .download_command(
                    "https://example.test/playlist",
                    Some(7),
                    DownloadOptions::default(),
                )
                .unwrap(),
        );
        for arguments in [&selected_inspection, &selected_download] {
            let option = arguments.iter().position(|arg| arg == "--playlist-items").unwrap();
            assert_eq!(arguments.get(option + 1).map(String::as_str), Some("7"));
        }

        assert!(
            engine.inspect_command("https://example.test/list", Some(0), None, Some(50)).is_err()
        );
        assert!(
            engine
                .download_command(
                    "https://example.test/list",
                    Some(-2),
                    DownloadOptions::default(),
                )
                .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn audio_command_selects_original_source_or_structured_ffmpeg_conversion() {
        let root = std::env::temp_dir().join(format!("yt-save-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let yt_dlp = root.join("yt-dlp.exe");
        std::fs::write(&yt_dlp, b"test executable placeholder").unwrap();
        let dependencies = DependencyPaths {
            directory: root.clone(),
            yt_dlp,
            deno: root.join("deno.exe"),
            ffmpeg: root.join("ffmpeg.exe"),
            ffprobe: root.join("ffprobe.exe"),
        };
        std::fs::write(&dependencies.ffmpeg, b"test executable placeholder").unwrap();
        std::fs::write(&dependencies.ffprobe, b"test executable placeholder").unwrap();
        let engine = DownloadEngine::new(dependencies, root.clone());
        let args = |mode, format, quality| {
            engine
                .download_command(
                    "https://example.test/audio",
                    None,
                    DownloadOptions::new(mode, format, quality),
                )
                .unwrap()
                .as_std()
                .get_args()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
        };

        let original = args(DownloadMode::AudioOnly, AudioFormat::Original, AudioQuality::K192);
        let original_format = original.iter().position(|arg| arg == "--format").unwrap();
        assert_eq!(original.get(original_format + 1).map(String::as_str), Some("bestaudio"));
        assert!(!original.iter().any(|arg| arg == "--extract-audio"));
        assert!(!original.iter().any(|arg| arg == "--audio-format"));
        assert!(!original.iter().any(|arg| arg == "--audio-quality"));

        for (format, yt_dlp_format) in [
            (AudioFormat::Mp3, "mp3"),
            (AudioFormat::M4a, "m4a"),
            (AudioFormat::Opus, "opus"),
            (AudioFormat::Aac, "aac"),
            (AudioFormat::Alac, "alac"),
            (AudioFormat::Flac, "flac"),
            (AudioFormat::Vorbis, "vorbis"),
            (AudioFormat::Wav, "wav"),
        ] {
            let converted = args(DownloadMode::AudioOnly, format, AudioQuality::K192);
            assert!(converted.iter().any(|arg| arg == "--extract-audio"));
            let format_flag = converted.iter().position(|arg| arg == "--audio-format").unwrap();
            assert_eq!(converted.get(format_flag + 1).map(String::as_str), Some(yt_dlp_format));
            if format.supports_quality_setting() {
                let quality_flag =
                    converted.iter().position(|arg| arg == "--audio-quality").unwrap();
                assert_eq!(converted.get(quality_flag + 1).map(String::as_str), Some("192K"));
            } else {
                assert!(!converted.iter().any(|arg| arg == "--audio-quality"));
            }
        }

        let video = args(DownloadMode::Video, AudioFormat::Mp3, AudioQuality::K192);
        let video_format = video.iter().position(|arg| arg == "--format").unwrap();
        assert_eq!(video.get(video_format + 1).map(String::as_str), Some("bv*+ba/b"));
        assert!(!video.iter().any(|arg| arg == "--extract-audio"));
        assert!(!video.iter().any(|arg| arg == "--audio-quality"));

        let audio_ignores_video_quality = engine
            .download_command(
                "https://example.test/audio",
                None,
                DownloadOptions::new(
                    DownloadMode::AudioOnly,
                    AudioFormat::Original,
                    AudioQuality::Best,
                )
                .with_video_quality(VideoQuality::P2160),
            )
            .unwrap()
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let audio_format_flag =
            audio_ignores_video_quality.iter().position(|arg| arg == "--format").unwrap();
        assert_eq!(
            audio_ignores_video_quality.get(audio_format_flag + 1).map(String::as_str),
            Some("bestaudio")
        );

        for quality in [
            VideoQuality::Best,
            VideoQuality::P2160,
            VideoQuality::P1440,
            VideoQuality::P1080,
            VideoQuality::P720,
            VideoQuality::P480,
            VideoQuality::P360,
            VideoQuality::Lowest,
        ] {
            let arguments = engine
                .download_command(
                    "https://example.test/video",
                    None,
                    DownloadOptions::new(
                        DownloadMode::Video,
                        AudioFormat::Original,
                        AudioQuality::Best,
                    )
                    .with_video_quality(quality),
                )
                .unwrap()
                .as_std()
                .get_args()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            let format_flag = arguments.iter().position(|arg| arg == "--format").unwrap();
            assert_eq!(
                arguments.get(format_flag + 1).map(String::as_str),
                Some(quality.yt_dlp_format())
            );
        }
        for quality in [
            VideoQuality::Best,
            VideoQuality::P2160,
            VideoQuality::P1440,
            VideoQuality::P1080,
            VideoQuality::P720,
            VideoQuality::P480,
            VideoQuality::P360,
            VideoQuality::Lowest,
        ] {
            let arguments = engine
                .download_command(
                    "https://example.test/video-only",
                    None,
                    DownloadOptions::new(
                        DownloadMode::VideoOnly,
                        AudioFormat::Original,
                        AudioQuality::Best,
                    )
                    .with_video_quality(quality),
                )
                .unwrap()
                .as_std()
                .get_args()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            let format_flag = arguments.iter().position(|arg| arg == "--format").unwrap();
            let selector = arguments.get(format_flag + 1).map(String::as_str).unwrap();
            assert_eq!(selector, quality.yt_dlp_video_only_format());
            assert!(!selector.contains("audio"));
            assert!(!selector.contains('/'));
            assert!(!arguments.iter().any(|arg| {
                matches!(arg.as_str(), "--extract-audio" | "--audio-format" | "--audio-quality")
            }));
        }
        for (mode, format_id) in [
            (DownloadMode::Video, "combined-1"),
            (DownloadMode::VideoOnly, "video-137"),
            (DownloadMode::AudioOnly, "audio-140"),
        ] {
            let arguments = engine
                .download_command(
                    "https://example.test/media",
                    None,
                    DownloadOptions::new(mode, AudioFormat::Original, AudioQuality::Best)
                        .with_selected_format_id(Some(format_id.to_owned())),
                )
                .unwrap()
                .as_std()
                .get_args()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            let format_flag = arguments.iter().position(|arg| arg == "--format").unwrap();
            assert_eq!(arguments.get(format_flag + 1).map(String::as_str), Some(format_id));
        }
        assert!(
            engine
                .download_command(
                    "https://example.test/media",
                    None,
                    DownloadOptions::new(
                        DownloadMode::Video,
                        AudioFormat::Original,
                        AudioQuality::Best,
                    )
                    .with_selected_format_id(Some("137/140".to_owned())),
                )
                .is_err()
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn converted_lossy_audio_passes_best_and_bitrate_qualities_as_separate_arguments() {
        let root = std::env::temp_dir().join(format!("yt-save-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let yt_dlp = root.join("yt-dlp.exe");
        std::fs::write(&yt_dlp, b"test executable placeholder").unwrap();
        let dependencies = DependencyPaths {
            directory: root.clone(),
            yt_dlp,
            deno: root.join("deno.exe"),
            ffmpeg: root.join("ffmpeg.exe"),
            ffprobe: root.join("ffprobe.exe"),
        };
        std::fs::write(&dependencies.ffmpeg, b"test executable placeholder").unwrap();
        std::fs::write(&dependencies.ffprobe, b"test executable placeholder").unwrap();
        let engine = DownloadEngine::new(dependencies, root.clone());
        for (quality, expected) in [
            (AudioQuality::Best, "0"),
            (AudioQuality::K320, "320K"),
            (AudioQuality::K192, "192K"),
            (AudioQuality::K128, "128K"),
        ] {
            let arguments = engine
                .download_command(
                    "https://example.test/audio",
                    None,
                    DownloadOptions::new(DownloadMode::AudioOnly, AudioFormat::Mp3, quality),
                )
                .unwrap()
                .as_std()
                .get_args()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            let quality_flag = arguments.iter().position(|arg| arg == "--audio-quality").unwrap();
            assert_eq!(arguments.get(quality_flag + 1).map(String::as_str), Some(expected));
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn audio_conversion_reports_missing_ffmpeg_and_ffprobe_before_spawn() {
        let root = std::env::temp_dir().join(format!("yt-save-engine-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let yt_dlp = root.join("yt-dlp.exe");
        std::fs::write(&yt_dlp, b"test executable placeholder").unwrap();
        let dependencies = DependencyPaths {
            directory: root.clone(),
            yt_dlp,
            deno: root.join("deno.exe"),
            ffmpeg: root.join("ffmpeg.exe"),
            ffprobe: root.join("ffprobe.exe"),
        };
        let engine = DownloadEngine::new(dependencies, root.clone());
        assert!(
            engine
                .download_command(
                    "https://example.test/audio",
                    None,
                    DownloadOptions::new(
                        DownloadMode::AudioOnly,
                        AudioFormat::Original,
                        AudioQuality::Best,
                    ),
                )
                .is_ok()
        );
        let error = match engine.download_command(
            "https://example.test/audio",
            None,
            DownloadOptions::new(DownloadMode::AudioOnly, AudioFormat::Mp3, AudioQuality::Best),
        ) {
            Ok(_) => panic!("conversion should reject missing bundled FFmpeg tools"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("requires the bundled FFmpeg and FFprobe"));
        assert!(error.to_string().contains("FFmpeg and FFprobe are missing"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn yt_dlp_errors_do_not_echo_private_media_urls() {
        let error =
            readable_error("ERROR: failed at https://media.example/watch?token=private-value");
        assert!(!error.contains("media.example"));
        assert!(!error.contains("private-value"));
        assert_eq!(
            readable_error("ERROR: Unsupported URL: https://example.test/x"),
            "yt-dlp does not currently support this URL."
        );
    }

    #[test]
    fn unavailable_format_error_explains_that_the_selected_mode_or_quality_must_change() {
        assert_eq!(
            readable_error("ERROR: [site] Requested format is not available. Use --list-formats"),
            "No available media format matches the selected download mode and quality. Choose a lower quality or another mode."
        );
    }

    #[test]
    fn only_transient_network_server_and_rate_limit_errors_are_retryable() {
        for message in [
            "ERROR: HTTP Error 503: Service Unavailable",
            "ERROR: HTTP Error 429: Too Many Requests",
            "ERROR: connection reset by peer",
            "ERROR: Temporary failure in name resolution",
            "ERROR: [Errno 11001] getaddrinfo failed",
            "ERROR: [Errno -2] Name or service not known",
        ] {
            let error = yt_dlp_error(message);
            assert!(is_retryable_engine_error(&error), "{message}");
            assert!(!error.to_string().contains("example.test"));
        }

        for message in [
            "ERROR: Unsupported URL: https://example.test/watch",
            "ERROR: HTTP Error 404: Not Found",
            "ERROR: Sign in to confirm your age",
            "ERROR: ffmpeg not found",
        ] {
            let error = yt_dlp_error(message);
            assert!(!is_retryable_engine_error(&error), "{message}");
        }
    }

    #[test]
    fn unavailable_ffmpeg_errors_are_not_reported_as_missing_media() {
        let error = readable_error(
            "ERROR: Postprocessing: ffmpeg not found. Please install or provide the path using --ffmpeg-location",
        );
        assert!(error.contains("Media processing needs the bundled FFmpeg and FFprobe"));
        assert!(!error.contains("media was not found"));
    }
}
