use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use anyhow::Context;
use directories::ProjectDirs;
use tracing_subscriber::fmt::writer::MakeWriter;

mod app;
mod app_update;
mod clipboard_monitor;
mod cookie_auth;
mod dependencies;
mod engine;
mod history;
mod model;
mod persistence;
#[cfg(windows)]
mod single_instance;
#[cfg(windows)]
mod window_activation;
mod yt_dlp_update;

slint::include_modules!();

const MAX_LOG_FILE_BYTES: u64 = 1024 * 1024;
const LOG_BACKUP_COUNT: usize = 3;
const LOG_FILE_NAME: &str = "yt-save.log";

#[derive(Clone)]
struct AppLogMakeWriter(Arc<Mutex<RotatingLog>>);

struct AppLogGuard<'a>(MutexGuard<'a, RotatingLog>);

impl<'a> MakeWriter<'a> for AppLogMakeWriter {
    type Writer = AppLogGuard<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        AppLogGuard(self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner))
    }
}

impl Write for AppLogGuard<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

struct RotatingLog {
    path: PathBuf,
    file: Option<File>,
    bytes_written: u64,
    max_file_bytes: u64,
    backup_count: usize,
}

impl RotatingLog {
    fn open(directory: &Path, max_file_bytes: u64, backup_count: usize) -> io::Result<Self> {
        if max_file_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "maximum log file size must be greater than zero",
            ));
        }

        fs::create_dir_all(directory)?;
        let path = directory.join(LOG_FILE_NAME);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let bytes_written = file.metadata()?.len();
        let mut log = Self { path, file: Some(file), bytes_written, max_file_bytes, backup_count };
        if log.bytes_written >= log.max_file_bytes {
            log.rotate()?;
        }
        Ok(log)
    }

    fn rotate(&mut self) -> io::Result<()> {
        if let Some(mut file) = self.file.take() {
            file.flush()?;
        }

        if let Err(rotation_error) = rotate_log_files(&self.path, self.backup_count) {
            // Keep logging bounded even if a stale backup cannot be renamed. In this
            // exceptional case, preserve the ability to record new diagnostics by
            // starting a fresh active file.
            match OpenOptions::new().create(true).write(true).truncate(true).open(&self.path) {
                Ok(file) => {
                    self.file = Some(file);
                    self.bytes_written = 0;
                    return Ok(());
                }
                Err(open_error) => {
                    return Err(io::Error::new(
                        open_error.kind(),
                        format!(
                            "rotate application log ({rotation_error}) and reopen it ({open_error})"
                        ),
                    ));
                }
            }
        }

        let file = OpenOptions::new().create(true).append(true).open(&self.path)?;
        self.bytes_written = file.metadata()?.len();
        self.file = Some(file);
        Ok(())
    }
}

impl Write for RotatingLog {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.bytes_written >= self.max_file_bytes {
            self.rotate()?;
        }

        let remaining = self.max_file_bytes - self.bytes_written;
        let write_length = buffer.len().min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let written = self
            .file
            .as_mut()
            .context("application log file is not open")
            .map_err(|error| io::Error::other(error.to_string()))?
            .write(&buffer[..write_length])?;
        self.bytes_written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file
            .as_mut()
            .context("application log file is not open")
            .map_err(|error| io::Error::other(error.to_string()))?
            .flush()
    }
}

fn rotate_log_files(path: &Path, backup_count: usize) -> io::Result<()> {
    for index in (1..=backup_count).rev() {
        let source = if index == 1 { path.to_path_buf() } else { backup_path(path, index - 1) };
        let destination = backup_path(path, index);
        if destination.exists() {
            fs::remove_file(&destination)?;
        }
        if source.exists() {
            fs::rename(source, destination)?;
        }
    }
    if backup_count == 0 && path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn backup_path(path: &Path, index: usize) -> PathBuf {
    path.with_file_name(format!("{LOG_FILE_NAME}.{index}"))
}

fn initialize_logging() -> anyhow::Result<PathBuf> {
    let project_dirs =
        ProjectDirs::from("com", env!("YT_SAVE_PUBLISHER"), env!("YT_SAVE_PRODUCT_NAME"))
            .context("determine the Yt Save application-data directory for logs")?;
    let directory = project_dirs.data_local_dir().join("logs");
    let writer = AppLogMakeWriter(Arc::new(Mutex::new(RotatingLog::open(
        &directory,
        MAX_LOG_FILE_BYTES,
        LOG_BACKUP_COUNT,
    )?)));

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(writer)
        .with_ansi(false)
        .with_target(false)
        .with_thread_ids(true)
        .try_init()
        .map_err(|error| anyhow::anyhow!("initialize the Yt Save file logger: {error}"))?;

    Ok(directory.join(LOG_FILE_NAME))
}

fn main() -> anyhow::Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    match initialize_logging() {
        Ok(log_file) => {
            tracing::info!(log_file = %log_file.display(), "application logging started")
        }
        Err(error) => {
            eprintln!("Yt Save could not initialize file logging: {error:#}");
            // Logging is diagnostic infrastructure; an unavailable log directory
            // must not prevent the downloader from starting.
            let _ = tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                )
                .with_target(false)
                .try_init();
        }
    }
    std::panic::set_hook(Box::new(|panic| {
        if let Some(location) = panic.location() {
            tracing::error!(file = location.file(), line = location.line(), "application panicked");
        } else {
            tracing::error!("application panicked");
        }
    }));

    if arguments.first().map(String::as_str) == Some("--yt-save-update-helper") {
        let exit_code = match app_update::run_update_helper(&arguments) {
            Ok(exit_code) => exit_code,
            Err(error) => {
                tracing::error!(error = %format!("{error:#}"), "application update helper failed");
                return Err(error);
            }
        };
        tracing::info!(exit_code, "application update helper finished");
        std::process::exit(exit_code);
    }

    let result = run_application();
    if let Err(error) = &result {
        tracing::error!(error = %format!("{error:#}"), "application startup or shutdown failed");
    } else {
        tracing::info!("application exited cleanly");
    }
    result
}

fn run_application() -> anyhow::Result<()> {
    #[cfg(windows)]
    slint::BackendSelector::new()
        .backend_name("winit".into())
        .with_winit_window_attributes_hook(|attributes| {
            if window_activation::consume_non_activating_window_marker() {
                use slint::winit_030::winit::platform::windows::WindowAttributesExtWindows;

                attributes.with_active(false).with_skip_taskbar(true)
            } else {
                attributes
            }
        })
        .select()?;

    app::run()
}
