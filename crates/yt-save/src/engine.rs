use std::{
    path::{Path, PathBuf},
    process::{ExitStatus, Output, Stdio},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader},
    process::Command,
    sync::watch,
};
use tracing::{debug, warn};

use crate::{
    dependencies::DependencyPaths,
    model::{AudioFormat, AudioQuality, DownloadMode, MediaMetadata, VideoQuality},
};

const PROGRESS_PREFIX: &str = "__YT_SAVE_PROGRESS__";
const OUTPUT_PREFIX: &str = "__YT_SAVE_OUTPUT__";
const OUTPUT_TEMPLATE: &str = "%(playlist_title|.)s/%(title)s [%(id)s].%(ext)s";

#[derive(Clone, Debug, Default)]
pub(crate) struct DownloadOptions {
    mode: DownloadMode,
    audio_format: AudioFormat,
    audio_quality: AudioQuality,
    video_quality: VideoQuality,
    output_directory: Option<PathBuf>,
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
            output_directory: None,
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
}

#[derive(Clone)]
pub struct DownloadEngine {
    dependencies: DependencyPaths,
    output_directory: PathBuf,
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
        Self { dependencies, output_directory }
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
        if signal_requested(signal) != JobSignal::Continue {
            return Ok(outcome_for_signal(signal_requested(signal)));
        }
        self.dependencies.require_for_download()?;
        let mut command = self.inspect_command(url, playlist_index)?;
        debug!(host = url_host(url), "requesting yt-dlp metadata");
        let mut child = command.spawn().context("start yt-dlp metadata extraction")?;
        let stdout = child.stdout.take().context("open yt-dlp metadata stdout")?;
        let stderr = child.stderr.take().context("open yt-dlp metadata stderr")?;
        let stdout_task = tokio::spawn(read_all(stdout));
        let stderr_task = tokio::spawn(read_all(stderr));
        let (status, stop_signal) = tokio::select! {
            biased;
            status = child.wait() => (status.context("wait for yt-dlp metadata")?, JobSignal::Continue),
            stop_signal = wait_for_signal(signal) => {
                match terminate_process_tree(&mut child).await {
                    Ok(status) => (status, stop_signal),
                    Err(error) => {
                        stdout_task.abort();
                        stderr_task.abort();
                        let _ = stdout_task.await;
                        let _ = stderr_task.await;
                        return Err(error);
                    }
                }
            },
        };
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
            bail!("yt-dlp could not read this URL: {}", readable_error(&message));
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
        if signal_requested(signal) != JobSignal::Continue {
            return Ok(outcome_for_signal(signal_requested(signal)));
        }
        self.dependencies.require_for_download()?;
        let mut command = self.download_command(url, playlist_index, options)?;
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
                        stderr_task.abort();
                        let _ = stderr_task.await;
                        return Err(error);
                    }
                    break;
                }
                line = lines.next_line() => {
                    let Some(line) = line.context("read yt-dlp output")? else {
                        break;
                    };
                    process_download_line(&line, &mut output_path, &mut on_progress);
                }
            }
        }
        let status = if stop_signal != JobSignal::Continue {
            child.wait().await.context("wait for yt-dlp")?
        } else {
            tokio::select! {
                biased;
                requested = wait_for_signal(signal) => {
                    stop_signal = requested;
                    match terminate_process_tree(&mut child).await {
                        Ok(status) => status,
                        Err(error) => {
                            stderr_task.abort();
                            let _ = stderr_task.await;
                            return Err(error);
                        }
                    }
                }
                status = child.wait() => status.context("wait for yt-dlp")?,
            }
        };
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
            bail!("{}", readable_error(stderr.trim()));
        }
        Ok(EngineOutcome::Completed(DownloadResult { output_path }))
    }

    fn base_command(&self) -> Command {
        let mut command = Command::new(&self.dependencies.yt_dlp);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if self.dependencies.deno.is_file() {
            command.arg("--js-runtimes").arg(format!("deno:{}", self.dependencies.deno.display()));
        }
        if self.dependencies.ffmpeg.is_file() && self.dependencies.ffprobe.is_file() {
            command.arg("--ffmpeg-location").arg(&self.dependencies.directory);
        }
        #[cfg(windows)]
        command.creation_flags(0x0800_0000);
        command
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
        let mut command = self.base_command();
        command
            .arg("--no-warnings")
            .arg("--continue")
            .arg("--newline")
            .arg("--no-color")
            .arg("--progress")
            .arg("--progress-template")
            .arg(format!(
                "download:{PROGRESS_PREFIX}%(progress.status)s|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|%(progress.total_bytes_estimate)s|%(progress.speed)s|%(progress.eta)s"
            ))
            .arg("--print")
            .arg(format!("after_move:{OUTPUT_PREFIX}%(filepath)s"))
            .arg("--paths")
            .arg(format!("home:{}", output_directory.display()))
            .arg("--output")
            .arg(OUTPUT_TEMPLATE)
            .arg("--format");
        match options.mode {
            DownloadMode::Video => {
                command.arg(options.video_quality.yt_dlp_format());
            }
            DownloadMode::AudioOnly => {
                command.arg("bestaudio");
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
            }
        }
        add_playlist_item_argument(&mut command, playlist_index)?;
        command.arg(url);
        Ok(command)
    }

    fn inspect_command(&self, url: &str, playlist_index: Option<i64>) -> Result<Command> {
        let mut command = self.base_command();
        command
            .arg("--no-warnings")
            .arg("--skip-download")
            .arg("--flat-playlist")
            .arg("--playlist-end")
            .arg("50");
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
    } else if !line.trim().is_empty() {
        debug!("yt-dlp emitted status output");
    }
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

fn readable_error(value: &str) -> String {
    let message = value.to_ascii_lowercase();
    if message.contains("unsupported url") || message.contains("no suitable extractor") {
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
        dependencies.require_for_download().unwrap();

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
            .download_command("https://example.test/watch/123", None, DownloadOptions::default())
            .unwrap()
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(!arguments.iter().any(|argument| argument == "--playlist-items"));
        assert!(arguments.iter().any(|argument| argument == "--continue"));

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

        let ordinary_inspection =
            args(engine.inspect_command("https://example.test/playlist", None).unwrap());
        assert!(!ordinary_inspection.iter().any(|arg| arg == "--playlist-items"));

        let selected_inspection =
            args(engine.inspect_command("https://example.test/playlist", Some(7)).unwrap());
        let inspection_end =
            selected_inspection.iter().position(|arg| arg == "--playlist-end").unwrap();
        assert_eq!(selected_inspection.get(inspection_end + 1).map(String::as_str), Some("50"));
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

        assert!(engine.inspect_command("https://example.test/list", Some(0)).is_err());
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
    fn unavailable_ffmpeg_errors_are_not_reported_as_missing_media() {
        let error = readable_error(
            "ERROR: Postprocessing: ffmpeg not found. Please install or provide the path using --ffmpeg-location",
        );
        assert!(error.contains("Media processing needs the bundled FFmpeg and FFprobe"));
        assert!(!error.contains("media was not found"));
    }
}
