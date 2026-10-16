use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use tokio::{process::Command, time::timeout};

const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct DependencyPaths {
    pub directory: PathBuf,
    pub yt_dlp: PathBuf,
    pub deno: PathBuf,
    pub ffmpeg: PathBuf,
    pub ffprobe: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutableStatus {
    Ready(String),
    Missing,
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DependencyReadiness {
    pub yt_dlp: ExecutableStatus,
    pub deno: ExecutableStatus,
    pub ffmpeg: ExecutableStatus,
    pub ffprobe: ExecutableStatus,
}

impl DependencyPaths {
    pub fn discover() -> Result<Self> {
        let candidates = dependency_directories()?;
        for directory in candidates {
            let yt_dlp = directory.join("yt-dlp.exe");
            let deno = directory.join("deno.exe");
            if yt_dlp.is_file() && deno.is_file() {
                return Ok(Self {
                    ffmpeg: directory.join("ffmpeg.exe"),
                    ffprobe: directory.join("ffprobe.exe"),
                    directory,
                    yt_dlp,
                    deno,
                });
            }
        }
        let directory = dependency_directories()?
            .into_iter()
            .next()
            .context("could not determine the bundled dependency directory")?;
        Ok(Self {
            yt_dlp: directory.join("yt-dlp.exe"),
            deno: directory.join("deno.exe"),
            ffmpeg: directory.join("ffmpeg.exe"),
            ffprobe: directory.join("ffprobe.exe"),
            directory,
        })
    }

    pub async fn preflight(&self) -> DependencyReadiness {
        let (yt_dlp, deno, ffmpeg, ffprobe) = tokio::join!(
            probe_executable(&self.yt_dlp, &["--version"]),
            probe_executable(&self.deno, &["--version"]),
            probe_executable(&self.ffmpeg, &["-version"]),
            probe_executable(&self.ffprobe, &["-version"]),
        );
        DependencyReadiness { yt_dlp, deno, ffmpeg, ffprobe }
    }

    pub fn require_for_download(&self) -> Result<()> {
        if !self.yt_dlp.is_file() {
            bail!("The bundled yt-dlp executable is missing.");
        }
        Ok(())
    }
}

impl DependencyReadiness {
    pub fn summary(&self) -> String {
        format!(
            "{} · {} · {} · {}",
            self.yt_dlp.label("yt-dlp", "required"),
            self.deno.label("Deno", "JavaScript-based formats may be incomplete"),
            self.ffmpeg.label("FFmpeg", "merging and conversion unavailable"),
            self.ffprobe.label("FFprobe", "media inspection unavailable"),
        )
    }

    pub fn yt_dlp_ready(&self) -> bool {
        matches!(&self.yt_dlp, ExecutableStatus::Ready(_))
    }
}

impl ExecutableStatus {
    fn label(&self, name: &str, missing_note: &str) -> String {
        match self {
            Self::Ready(version) => format!("{name} {version}"),
            Self::Missing => format!("{name} missing ({missing_note})"),
            Self::Failed(reason) => format!("{name} failed ({reason})"),
        }
    }
}

async fn probe_executable(path: &Path, arguments: &[&str]) -> ExecutableStatus {
    if !path.is_file() {
        return ExecutableStatus::Missing;
    }

    let mut command = Command::new(path);
    command
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.creation_flags(0x0800_0000);

    match timeout(PREFLIGHT_TIMEOUT, command.output()).await {
        Err(_) => ExecutableStatus::Failed("version check timed out".to_owned()),
        Ok(Err(_)) => ExecutableStatus::Failed("could not launch".to_owned()),
        Ok(Ok(output)) if !output.status.success() => {
            let code =
                output.status.code().map_or_else(|| "unknown".to_owned(), |code| code.to_string());
            let detail = first_line(&output.stdout, &output.stderr);
            if detail.is_empty() {
                ExecutableStatus::Failed(format!("version check exited with code {code}"))
            } else {
                ExecutableStatus::Failed(format!("version check exited with code {code}: {detail}"))
            }
        }
        Ok(Ok(output)) => {
            let version = first_line(&output.stdout, &output.stderr);
            if version.is_empty() {
                ExecutableStatus::Failed("version check returned no version line".to_owned())
            } else {
                ExecutableStatus::Ready(version)
            }
        }
    }
}

fn first_line(stdout: &[u8], stderr: &[u8]) -> String {
    [stdout, stderr]
        .into_iter()
        .flat_map(|bytes| {
            String::from_utf8_lossy(bytes).lines().map(str::to_owned).collect::<Vec<_>>()
        })
        .map(|line| line.trim().to_owned())
        .find(|line| !line.is_empty())
        .map(|line| line.chars().take(120).collect())
        .unwrap_or_default()
}

fn dependency_directories() -> Result<Vec<PathBuf>> {
    let mut candidates = Vec::new();
    if let Some(directory) = std::env::var_os("YT_SAVE_DEPENDENCY_DIR") {
        candidates.push(PathBuf::from(directory));
    }

    let executable = std::env::current_exe().context("locate the application executable")?;
    if let Some(parent) = executable.parent() {
        candidates.push(parent.join("bin"));
        candidates.push(parent.join("dependencies"));
        candidates.push(parent.to_path_buf());
        if let Some(grandparent) = parent.parent() {
            candidates.push(grandparent.join("dependencies"));
            candidates.push(grandparent.join("bin"));
        }
    }
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_summary_shows_versions_and_optional_dependency_degradation() {
        let readiness = DependencyReadiness {
            yt_dlp: ExecutableStatus::Ready("2026.08.19".to_owned()),
            deno: ExecutableStatus::Missing,
            ffmpeg: ExecutableStatus::Ready("ffmpeg version 8.1".to_owned()),
            ffprobe: ExecutableStatus::Failed("could not launch".to_owned()),
        };

        assert_eq!(
            readiness.summary(),
            "yt-dlp 2026.08.19 · Deno missing (JavaScript-based formats may be incomplete) · FFmpeg ffmpeg version 8.1 · FFprobe failed (could not launch)"
        );
        assert!(readiness.yt_dlp_ready());
    }

    #[test]
    fn only_a_runnable_yt_dlp_is_required_for_download_readiness() {
        let optional_dependencies_unavailable = DependencyReadiness {
            yt_dlp: ExecutableStatus::Ready("2026.08.19".to_owned()),
            deno: ExecutableStatus::Failed("version check timed out".to_owned()),
            ffmpeg: ExecutableStatus::Missing,
            ffprobe: ExecutableStatus::Missing,
        };
        assert!(optional_dependencies_unavailable.yt_dlp_ready());

        let yt_dlp_unavailable = DependencyReadiness {
            yt_dlp: ExecutableStatus::Failed("could not launch".to_owned()),
            ..optional_dependencies_unavailable
        };
        assert!(!yt_dlp_unavailable.yt_dlp_ready());
    }
}
