use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use directories::{ProjectDirs, UserDirs};
use slint::{ComponentHandle, ModelRc, SharedString, VecModel};
use tokio::sync::{Semaphore, mpsc};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    DownloadRowData, MainWindow,
    dependencies::DependencyPaths,
    engine::{DownloadEngine, ProgressUpdate},
    model::{DownloadRecord, MediaMetadata, format_bytes, format_duration},
    persistence::Store,
};

enum UiEvent {
    Refresh,
    Status(String),
    DependencyStatus(String),
    Metadata(MediaMetadata),
}

pub fn run() -> Result<()> {
    let paths = AppPaths::discover()?;
    std::fs::create_dir_all(&paths.downloads)
        .with_context(|| format!("create download directory {}", paths.downloads.display()))?;
    let store = Store::open(paths.database.clone())?;
    let dependencies = DependencyPaths::discover()?;
    let engine = DownloadEngine::new(dependencies.clone(), paths.downloads.clone());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("yt-save-worker")
        .build()
        .context("create background runtime")?;
    let handle = runtime.handle().clone();
    let window = MainWindow::new().context("create main window")?;
    window.set_product_name(env!("YT_SAVE_PRODUCT_NAME").into());
    window.set_app_version(env!("YT_SAVE_PRODUCT_VERSION").into());
    let saved_rows =
        runtime.block_on(store.list_downloads())?.iter().map(row_view).collect::<Vec<_>>();
    window.set_downloads(ModelRc::new(VecModel::from(saved_rows)));

    let (events_tx, mut events_rx) = mpsc::unbounded_channel::<UiEvent>();
    let weak_window = window.as_weak();
    let event_store = store.clone();
    runtime.spawn(async move {
        while let Some(event) = events_rx.recv().await {
            match event {
                UiEvent::Refresh => match event_store.list_downloads().await {
                    Ok(records) => {
                        let rows = records.iter().map(row_view).collect::<Vec<_>>();
                        let weak = weak_window.clone();
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(window) = weak.upgrade() {
                                window.set_downloads(ModelRc::new(VecModel::from(rows)));
                            }
                        });
                    }
                    Err(error) => error!(%error, "refresh download list"),
                },
                UiEvent::Status(message) => {
                    let weak = weak_window.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_status_message(message.into());
                        }
                    });
                }
                UiEvent::DependencyStatus(message) => {
                    let weak = weak_window.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_dependency_status(message.into());
                        }
                    });
                }
                UiEvent::Metadata(metadata) => {
                    let format_summary = if metadata.is_playlist {
                        String::new()
                    } else {
                        format!(" · {} formats", metadata.format_count)
                    };
                    let playlist_summary = metadata.playlist_count.map_or(String::new(), |count| {
                        format!(" · up to {count} playlist entries inspected")
                    });
                    let details = format!(
                        "{} · {}{}{}",
                        metadata.source,
                        format_duration(metadata.duration_seconds),
                        format_summary,
                        playlist_summary,
                    );
                    let weak = weak_window.clone();
                    let title = metadata.title;
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_analyzed_title(title.into());
                            window.set_analyzed_details(details.into());
                        }
                    });
                }
            }
        }
    });

    let dependency_events = events_tx.clone();
    let dependency_paths = dependencies.clone();
    handle.spawn(async move {
        let readiness = dependency_paths.preflight().await;
        let summary = readiness.summary();
        let status = if readiness.yt_dlp_ready() {
            summary
        } else {
            format!("yt-dlp failed its startup check; download jobs may fail. {summary}")
        };
        let _ = dependency_events.send(UiEvent::DependencyStatus(status));
    });

    let analyze_events = events_tx.clone();
    let analyze_engine = engine.clone();
    let analyze_handle = handle.clone();
    window.on_analyze_requested(move |submitted_url| {
        let url = match validate_url(&submitted_url) {
            Ok(url) => url,
            Err(message) => {
                let _ = analyze_events.send(UiEvent::Status(message));
                return;
            }
        };
        let _ = analyze_events.send(UiEvent::Status("Reading media details…".to_owned()));
        let engine = analyze_engine.clone();
        let events = analyze_events.clone();
        analyze_handle.spawn(async move {
            match engine.inspect(&url).await {
                Ok(metadata) => {
                    let _ = events.send(UiEvent::Status("Media details loaded.".to_owned()));
                    let _ = events.send(UiEvent::Metadata(metadata));
                }
                Err(error) => {
                    let _ = events.send(UiEvent::Status(error.to_string()));
                }
            }
        });
    });

    let queue_store = store.clone();
    let queue_engine = engine.clone();
    let queue_events = events_tx.clone();
    let queue_slots = Arc::new(Semaphore::new(1));
    let enqueue_slots = queue_slots.clone();
    let queue_handle = handle.clone();
    let pending = runtime.block_on(recover_pending(&store))?;
    for record in pending {
        let store = store.clone();
        let engine = engine.clone();
        let events = events_tx.clone();
        let slots = enqueue_slots.clone();
        handle.spawn(run_download_job(record, store, engine, events, slots));
    }
    let _ = events_tx.send(UiEvent::Refresh);

    window.on_enqueue_requested(move |submitted_url| {
        let url = match validate_url(&submitted_url) {
            Ok(url) => url,
            Err(message) => {
                let _ = queue_events.send(UiEvent::Status(message));
                return;
            }
        };
        let record = DownloadRecord::queued(Uuid::new_v4().to_string(), url, now());
        let store = queue_store.clone();
        let engine = queue_engine.clone();
        let events = queue_events.clone();
        let slots = enqueue_slots.clone();
        queue_handle.spawn(async move {
            if let Err(error) = store.save_download(record.clone()).await {
                let _ = events.send(UiEvent::Status(format!("Could not save queue item: {error}")));
                return;
            }
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status("Added to the download queue.".to_owned()));
            run_download_job(record, store, engine, events, slots).await;
        });
    });

    let retry_store = store.clone();
    let retry_engine = engine.clone();
    let retry_events = events_tx.clone();
    let retry_slots = queue_slots.clone();
    let retry_handle = handle.clone();
    window.on_retry_download_requested(move |id| {
        let id = id.to_string();
        let store = retry_store.clone();
        let engine = retry_engine.clone();
        let events = retry_events.clone();
        let slots = retry_slots.clone();
        retry_handle.spawn(async move {
            match store.retry_download(&id, now()).await {
                Ok(Some(record)) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(
                        "Retry queued. yt-dlp will continue a compatible partial file.".to_owned(),
                    ));
                    run_download_job(record, store, engine, events, slots).await;
                }
                Ok(None) => {
                    let _ = events.send(UiEvent::Status(
                        "Only failed or interrupted downloads can be retried.".to_owned(),
                    ));
                }
                Err(error) => {
                    error!(%error, job_id = %id, "queue download retry");
                    let _ = events.send(UiEvent::Status(format!("Could not queue retry: {error}")));
                }
            }
        });
    });

    let folder = paths.downloads.clone();
    let folder_events = events_tx.clone();
    window.on_open_download_folder(move || {
        if let Err(error) = open_folder(&folder) {
            let _ = folder_events
                .send(UiEvent::Status(format!("Could not open download folder: {error}")));
        }
    });

    let clear_store = store.clone();
    let clear_events = events_tx.clone();
    let clear_handle = handle.clone();
    window.on_clear_completed_requested(move || {
        let store = clear_store.clone();
        let events = clear_events.clone();
        clear_handle.spawn(async move {
            match store.clear_completed_downloads().await {
                Ok(0) => {
                    let _ = events.send(UiEvent::Status("There are no completed downloads to clear.".to_owned()));
                }
                Ok(count) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(format!(
                        "Removed {count} completed items from history. Downloaded files were left in place."
                    )));
                }
                Err(error) => {
                    error!(%error, "clear completed download history");
                    let _ = events.send(UiEvent::Status(format!("Could not clear completed history: {error}")));
                }
            }
        });
    });

    let remove_store = store.clone();
    let remove_events = events_tx.clone();
    let remove_handle = handle.clone();
    window.on_remove_download_requested(move |id| {
        let id = id.to_string();
        let store = remove_store.clone();
        let events = remove_events.clone();
        remove_handle.spawn(async move {
            match store.remove_download(&id).await {
                Ok(true) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(
                        "Removed the history entry. Any downloaded file was left in place."
                            .to_owned(),
                    ));
                }
                Ok(false) => {
                    let _ = events.send(UiEvent::Status(
                        "Only finished or stopped downloads can be removed from the queue."
                            .to_owned(),
                    ));
                }
                Err(error) => {
                    error!(%error, job_id = %id, "remove download history entry");
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not remove that history entry: {error}"
                    )));
                }
            }
        });
    });

    let output_events = events_tx.clone();
    let output_handle = handle.clone();
    window.on_open_output_requested(move |output_path| {
        let path = PathBuf::from(output_path.as_str());
        let events = output_events.clone();
        output_handle.spawn_blocking(move || {
            if let Err(error) = open_file(&path) {
                let _ = events
                    .send(UiEvent::Status(format!("Could not open downloaded file: {error}")));
            }
        });
    });

    info!(product = env!("YT_SAVE_PRODUCT_NAME"), "application started");
    window.run().context("run Slint event loop")?;
    runtime.shutdown_timeout(Duration::from_secs(3));
    Ok(())
}

async fn recover_pending(store: &Store) -> Result<Vec<DownloadRecord>> {
    let mut pending = Vec::new();
    for mut record in store.list_downloads().await? {
        if matches!(record.status.as_str(), "Analyzing" | "Downloading" | "Post-processing") {
            record.status = "Interrupted".to_owned();
            record.error =
                Some("The app closed while this item was active; it will resume now.".to_owned());
            record.updated_at = now();
        }
        if matches!(record.status.as_str(), "Queued" | "Interrupted") {
            record.status = "Queued".to_owned();
            record.error = None;
            record.updated_at = now();
            store.save_download(record.clone()).await?;
            pending.push(record);
        }
    }
    pending.sort_by_key(|record| (record.created_at, record.id.clone()));
    Ok(pending)
}

async fn run_download_job(
    mut active: DownloadRecord,
    store: Store,
    engine: DownloadEngine,
    events: mpsc::UnboundedSender<UiEvent>,
    slots: Arc<Semaphore>,
) {
    let Ok(_slot) = slots.acquire_owned().await else {
        return;
    };
    active.status = "Analyzing".to_owned();
    active.updated_at = now();
    if let Err(error) = store.save_download(active.clone()).await {
        error!(%error, job_id = %active.id, "persist analyzing state");
        return;
    }
    let _ = events.send(UiEvent::Refresh);

    match engine.inspect(&active.url).await {
        Ok(metadata) => {
            active.title = metadata.title;
            active.source = metadata.source;
            active.status = "Downloading".to_owned();
            active.updated_at = now();
            active.error = None;
            if let Err(error) = store.save_download(active.clone()).await {
                error!(%error, job_id = %active.id, "persist download state");
            }
            let _ = events.send(UiEvent::Refresh);
        }
        Err(error) => {
            active.status = "Failed".to_owned();
            active.error = Some(error.to_string());
            active.updated_at = now();
            if let Err(save_error) = store.save_download(active.clone()).await {
                error!(%save_error, job_id = %active.id, "persist analysis failure");
            }
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status(error.to_string()));
            return;
        }
    }

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<ProgressUpdate>();
    let progress_store = store.clone();
    let progress_events = events.clone();
    let mut progress_record = active.clone();
    let progress_task = tokio::spawn(async move {
        let mut last_persist = tokio::time::Instant::now() - Duration::from_secs(5);
        while let Some(progress) = progress_rx.recv().await {
            if let Some(phase) = progress.status.as_deref() {
                progress_record.status = match phase {
                    "downloading" => "Downloading".to_owned(),
                    "finished" => "Post-processing".to_owned(),
                    _ => progress_record.status.clone(),
                };
            }
            progress_record.progress = progress.percent().unwrap_or(progress_record.progress);
            progress_record.downloaded_bytes = progress.downloaded_bytes;
            progress_record.total_bytes = progress.total_bytes.or(progress.estimated_total_bytes);
            progress_record.speed_bytes_per_second = progress.speed_bytes_per_second;
            progress_record.eta_seconds = progress.eta_seconds;
            progress_record.updated_at = now();
            if last_persist.elapsed() >= Duration::from_secs(2) {
                if let Err(error) = progress_store.save_download(progress_record.clone()).await {
                    error!(%error, job_id = %progress_record.id, "persist progress");
                }
                last_persist = tokio::time::Instant::now();
                let _ = progress_events.send(UiEvent::Refresh);
            }
        }
        progress_record
    });

    let download_result = engine
        .download(&active.url, move |progress| {
            let _ = progress_tx.send(progress);
        })
        .await;
    let mut final_record = progress_task.await.unwrap_or_else(|_| active.clone());
    let downloaded_successfully = download_result.is_ok();
    let status_message = match download_result {
        Ok(result) => {
            final_record.status = "Completed".to_owned();
            final_record.progress = 100.0;
            final_record.output_path = result.output_path;
            final_record.error = None;
            "Download complete.".to_owned()
        }
        Err(error) => {
            final_record.status = "Failed".to_owned();
            final_record.error = Some(error.to_string());
            error.to_string()
        }
    };
    final_record.updated_at = now();
    if let Err(error) = store.save_download(final_record).await {
        error!(%error, job_id = %active.id, "persist terminal download state");
        let message = if downloaded_successfully {
            "Media downloaded, but the app could not save its completed state to the library."
        } else {
            "The download failed, and the app could not save the final queue state."
        };
        let _ = events.send(UiEvent::Status(message.to_owned()));
    } else {
        let _ = events.send(UiEvent::Refresh);
        let _ = events.send(UiEvent::Status(status_message));
    }
}

fn validate_url(value: &str) -> std::result::Result<String, String> {
    let trimmed = value.trim();
    let parsed =
        url::Url::parse(trimmed).map_err(|_| "Enter a complete http or https URL.".to_owned())?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err("Only http and https media URLs are accepted.".to_owned());
    }
    Ok(parsed.to_string())
}

fn row_view(record: &DownloadRecord) -> DownloadRowData {
    let details = match (record.downloaded_bytes, record.total_bytes) {
        (Some(downloaded), Some(total)) => {
            format!("{} of {}", format_bytes(Some(downloaded)), format_bytes(Some(total)))
        }
        (Some(downloaded), None) => format!("{} downloaded", format_bytes(Some(downloaded))),
        _ => record.error.clone().unwrap_or_else(|| record.url.clone()),
    };
    DownloadRowData {
        id: SharedString::from(record.id.as_str()),
        title: SharedString::from(record.title.as_str()),
        source: SharedString::from(record.source.as_str()),
        status: SharedString::from(record.status.as_str()),
        progress: record.progress,
        detail: SharedString::from(details),
        output_path: SharedString::from(record.output_path.as_deref().unwrap_or_default()),
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn open_folder(path: &PathBuf) -> Result<()> {
    let mut command = if cfg!(windows) {
        let mut command = Command::new("explorer.exe");
        command.arg(path);
        command
    } else if cfg!(target_os = "macos") {
        let mut command = Command::new("open");
        command.arg(path);
        command
    } else {
        let mut command = Command::new("xdg-open");
        command.arg(path);
        command
    };
    command.spawn().context("launch file manager")?;
    Ok(())
}

fn validate_output_file(path: &Path) -> Result<()> {
    if !path.is_file() {
        anyhow::bail!("file does not exist at {}", path.display());
    }
    Ok(())
}

fn open_file(path: &Path) -> Result<()> {
    validate_output_file(path)?;

    open_with_platform_handler(path)
}

#[cfg(windows)]
fn open_with_platform_handler(path: &Path) -> Result<()> {
    use std::ffi::OsStr;
    use windows_sys::Win32::UI::{Shell::ShellExecuteW, WindowsAndMessaging::SW_SHOWNORMAL};

    // ShellExecuteW opens the file through its registered Windows association.
    let operation = wide_null_terminated(OsStr::new("open"));
    let file = wide_null_terminated(path.as_os_str());
    let result = unsafe {
        ShellExecuteW(
            std::ptr::null_mut(),
            operation.as_ptr(),
            file.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            SW_SHOWNORMAL,
        )
    };
    if (result as isize) <= 32 {
        anyhow::bail!("Windows could not open the file (ShellExecute error {})", result as isize);
    }
    Ok(())
}

#[cfg(not(windows))]
fn open_with_platform_handler(path: &Path) -> Result<()> {
    let mut command =
        if cfg!(target_os = "macos") { Command::new("open") } else { Command::new("xdg-open") };
    command.arg(path).spawn().context("launch the default media player")?;
    Ok(())
}

#[cfg(windows)]
fn wide_null_terminated(value: &std::ffi::OsStr) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    value.encode_wide().chain(Some(0)).collect()
}

struct AppPaths {
    database: PathBuf,
    downloads: PathBuf,
}

impl AppPaths {
    fn discover() -> Result<Self> {
        let project_dirs =
            ProjectDirs::from("com", env!("YT_SAVE_PUBLISHER"), env!("YT_SAVE_PRODUCT_NAME"))
                .context("determine the Yt Save application-data directory")?;
        let downloads = UserDirs::new()
            .and_then(|user_dirs| user_dirs.download_dir().map(PathBuf::from))
            .unwrap_or_else(|| project_dirs.data_local_dir().join("Downloads"))
            .join("Yt Save");
        Ok(Self { database: project_dirs.data_local_dir().join("library.sqlite3"), downloads })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_http_urls_without_restricting_sites() {
        assert_eq!(
            validate_url(" https://peertube.example/videos/watch/abc ").unwrap(),
            "https://peertube.example/videos/watch/abc"
        );
        assert_eq!(
            validate_url("https://www.youtube.com/watch?v=abc").unwrap(),
            "https://www.youtube.com/watch?v=abc"
        );
        assert!(validate_url("file:///C:/private.mp4").is_err());
        assert!(validate_url("not a URL").is_err());
    }

    #[test]
    fn missing_output_file_fails_before_launching_a_handler() {
        let path = std::env::temp_dir().join(format!("yt-save-missing-{}.mp4", Uuid::new_v4()));
        assert!(open_file(&path).is_err());
    }

    #[test]
    fn existing_media_file_passes_validation_without_launching_a_handler() {
        let path = std::env::temp_dir().join(format!("yt-save-media {}.mp4", Uuid::new_v4()));
        std::fs::write(&path, b"fixture media").unwrap();

        let result = validate_output_file(&path);

        std::fs::remove_file(&path).unwrap();
        assert!(result.is_ok());
    }

    #[test]
    fn a_directory_is_not_accepted_as_a_completed_media_file() {
        let path = std::env::temp_dir().join(format!("yt-save-directory-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();

        let result = open_file(&path);

        std::fs::remove_dir(&path).unwrap();
        assert!(result.is_err());
    }

    #[cfg(windows)]
    #[test]
    fn shell_open_arguments_preserve_spaces_and_unicode_without_shell_quoting() {
        use std::os::windows::ffi::OsStringExt;

        let path = std::env::temp_dir().join("Yt Save media Ω.mp4");
        let encoded_path = wide_null_terminated(path.as_os_str());
        let decoded_path = std::ffi::OsString::from_wide(&encoded_path[..encoded_path.len() - 1]);
        let encoded_operation = wide_null_terminated(std::ffi::OsStr::new("open"));

        assert_eq!(decoded_path, path.as_os_str());
        assert_eq!(encoded_path.last(), Some(&0));
        assert_eq!(
            std::ffi::OsString::from_wide(&encoded_operation[..encoded_operation.len() - 1])
                .as_os_str(),
            std::ffi::OsStr::new("open")
        );
    }

    #[tokio::test]
    async fn startup_requeues_saved_and_interrupted_downloads() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/queued".to_owned(),
            10,
        );
        let mut active = DownloadRecord::queued(
            "active".to_owned(),
            "https://example.test/active".to_owned(),
            20,
        );
        active.status = "Downloading".to_owned();
        active.downloaded_bytes = Some(75);
        active.total_bytes = Some(100);
        store.save_download(queued.clone()).await.unwrap();
        store.save_download(active.clone()).await.unwrap();

        let pending = recover_pending(&store).await.unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|record| record.status == "Queued"));
        assert_eq!(
            pending.iter().map(|record| record.id.as_str()).collect::<Vec<_>>(),
            ["queued", "active"]
        );
        let rows = store.list_downloads().await.unwrap();
        let recovered_active = rows.iter().find(|record| record.id == "active").unwrap();
        assert_eq!(recovered_active.status, "Queued");
        assert_eq!(recovered_active.downloaded_bytes, Some(75));
        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}
