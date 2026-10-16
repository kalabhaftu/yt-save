use std::{path::PathBuf, process::Stdio};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
};
use tracing::{debug, warn};

use crate::{dependencies::DependencyPaths, model::MediaMetadata};

const PROGRESS_PREFIX: &str = "__YT_SAVE_PROGRESS__";
const OUTPUT_PREFIX: &str = "__YT_SAVE_OUTPUT__";

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

impl DownloadEngine {
    pub fn new(dependencies: DependencyPaths, output_directory: PathBuf) -> Self {
        Self { dependencies, output_directory }
    }

    pub async fn inspect(&self, url: &str) -> Result<MediaMetadata> {
        self.dependencies.require_for_download()?;
        let mut command = self.base_command();
        command
            .arg("--no-warnings")
            .arg("--skip-download")
            .arg("--flat-playlist")
            .arg("--playlist-end")
            .arg("50")
            .arg("--dump-single-json")
            .arg(url);
        debug!(host = url_host(url), "requesting yt-dlp metadata");
        let output = command.output().await.context("start yt-dlp metadata extraction")?;
        if !output.status.success() {
            let message = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            bail!("yt-dlp could not read this URL: {}", readable_error(&message));
        }
        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .context("parse yt-dlp's JSON metadata response")?;
        Ok(MediaMetadata::from_yt_dlp_json(&json, url))
    }

    pub async fn download<F>(&self, url: &str, mut on_progress: F) -> Result<DownloadResult>
    where
        F: FnMut(ProgressUpdate) + Send,
    {
        self.dependencies.require_for_download()?;
        tokio::fs::create_dir_all(&self.output_directory)
            .await
            .with_context(|| format!("create {}", self.output_directory.display()))?;

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
            .arg(format!("home:{}", self.output_directory.display()))
            .arg("--output")
            .arg("%(title)s [%(id)s].%(ext)s")
            .arg("--format")
            .arg("bv*+ba/b")
            .arg(url);

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
        while let Some(line) = lines.next_line().await.context("read yt-dlp output")? {
            if let Some(encoded) = line.strip_prefix(PROGRESS_PREFIX) {
                if let Some(progress) = parse_progress_line(encoded) {
                    on_progress(progress);
                }
            } else if let Some(path) = line.strip_prefix(OUTPUT_PREFIX) {
                output_path = Some(path.to_owned());
            } else if !line.trim().is_empty() {
                debug!("yt-dlp emitted status output");
            }
        }
        let status = child.wait().await.context("wait for yt-dlp")?;
        let stderr = stderr_task.await.unwrap_or_default();
        if !status.success() {
            warn!(code = ?status.code(), "yt-dlp download failed");
            bail!("{}", readable_error(stderr.trim()));
        }
        Ok(DownloadResult { output_path })
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
}
