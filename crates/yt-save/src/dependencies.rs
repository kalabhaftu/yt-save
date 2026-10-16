use std::path::PathBuf;

use anyhow::{Context, Result, bail};

#[derive(Clone, Debug)]
pub struct DependencyPaths {
    pub directory: PathBuf,
    pub yt_dlp: PathBuf,
    pub deno: PathBuf,
    pub ffmpeg: PathBuf,
    pub ffprobe: PathBuf,
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

    pub fn readiness_summary(&self) -> String {
        let yt_dlp = if self.yt_dlp.is_file() { "yt-dlp ready" } else { "yt-dlp missing" };
        let deno = if self.deno.is_file() {
            "Deno ready"
        } else {
            "Deno missing (YouTube formats may be incomplete)"
        };
        let ffmpeg = if self.ffmpeg.is_file() && self.ffprobe.is_file() {
            "FFmpeg ready"
        } else {
            "FFmpeg missing (merging and conversion unavailable)"
        };
        format!("{yt_dlp} · {deno} · {ffmpeg}")
    }

    pub fn require_for_download(&self) -> Result<()> {
        if !self.yt_dlp.is_file() {
            bail!("The bundled yt-dlp executable is missing.");
        }
        Ok(())
    }
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
