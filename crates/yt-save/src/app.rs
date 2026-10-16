use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use directories::{ProjectDirs, UserDirs};
use slint::{ComponentHandle, ModelRc, SharedString, VecModel};
use tokio::sync::{Semaphore, mpsc, watch};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    DownloadRowData, MainWindow, PlaylistEntryRowData,
    dependencies::DependencyPaths,
    engine::{DownloadEngine, EngineOutcome, ProgressUpdate},
    model::{
        AudioFormat, AudioQuality, DownloadMode, DownloadRecord, MediaMetadata, PlaylistEntry,
        format_bytes, format_duration,
    },
    persistence::Store,
};

enum UiEvent {
    Refresh,
    Status(String),
    DependencyStatus(String),
    ClearPreview { request_id: u64 },
    Metadata { request_id: u64, source_url: String, metadata: MediaMetadata },
}

type ActiveJobs = Arc<Mutex<std::collections::HashMap<String, Arc<watch::Sender<bool>>>>>;

struct ActiveJobRegistration {
    id: String,
    jobs: ActiveJobs,
    sender: Arc<watch::Sender<bool>>,
}

impl Drop for ActiveJobRegistration {
    fn drop(&mut self) {
        if let Ok(mut jobs) = self.jobs.lock()
            && jobs.get(&self.id).is_some_and(|sender| Arc::ptr_eq(sender, &self.sender))
        {
            jobs.remove(&self.id);
        }
    }
}

#[derive(Clone)]
struct PlaylistSelectionState {
    source_url: String,
    source: String,
    entries: Vec<PlaylistEntry>,
    selected: Vec<bool>,
}

impl PlaylistSelectionState {
    fn selected_count(&self) -> usize {
        self.selected.iter().filter(|selected| **selected).count()
    }

    fn selectable_count(&self) -> usize {
        self.entries.iter().filter(|entry| entry.playlist_index.is_some()).count()
    }
}

fn playlist_entry_rows(selection: &PlaylistSelectionState) -> Vec<PlaylistEntryRowData> {
    selection
        .entries
        .iter()
        .zip(selection.selected.iter())
        .enumerate()
        .map(|(row_index, (entry, selected))| PlaylistEntryRowData {
            row_index: i32::try_from(row_index).unwrap_or(i32::MAX),
            ordinal: SharedString::from(
                entry.playlist_index.map_or_else(|| "—".to_owned(), |index| index.to_string()),
            ),
            title: SharedString::from(entry.title.as_str()),
            selectable: entry.playlist_index.is_some(),
            selected: *selected,
        })
        .collect()
}

fn selection_summary(selected_count: usize, selectable_count: usize) -> SharedString {
    if selectable_count == 0 {
        SharedString::from("No entries have a valid playlist index for selection.")
    } else {
        SharedString::from(format!(
            "Selected {selected_count} of {selectable_count} selectable entries."
        ))
    }
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
    let playlist_selection = Arc::new(Mutex::new(None::<PlaylistSelectionState>));
    let weak_window = window.as_weak();
    let event_store = store.clone();
    let event_playlist_selection = playlist_selection.clone();
    runtime.spawn(async move {
        let mut latest_analysis_id = 0;
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
                UiEvent::ClearPreview { request_id } => {
                    latest_analysis_id = request_id;
                    if let Ok(mut state) = event_playlist_selection.lock() {
                        *state = None;
                    }
                    let weak = weak_window.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_analyzed_title(SharedString::default());
                            window.set_analyzed_details(SharedString::default());
                            window.set_analyzed_is_playlist(false);
                            window.set_playlist_entries(ModelRc::new(VecModel::from(Vec::new())));
                            window.set_selected_playlist_count(0);
                            window.set_selectable_playlist_count(0);
                            window.set_playlist_selection_summary(SharedString::default());
                        }
                    });
                }
                UiEvent::Metadata { request_id, source_url, metadata } => {
                    if request_id != latest_analysis_id {
                        continue;
                    }
                    let format_summary = if metadata.is_playlist {
                        String::new()
                    } else {
                        format!(" · {} formats", metadata.format_count)
                    };
                    let playlist_summary = metadata.playlist_count.map_or(String::new(), |count| {
                        format!(" · {count} entries in preview; preview capped at 50")
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
                    let is_playlist = metadata.is_playlist;
                    let preview_state = if is_playlist {
                        let entries = metadata.playlist_entries;
                        Some(PlaylistSelectionState {
                            source_url,
                            source: metadata.source,
                            selected: vec![false; entries.len()],
                            entries,
                        })
                    } else {
                        None
                    };
                    let preview_rows =
                        preview_state.as_ref().map(playlist_entry_rows).unwrap_or_default();
                    let selectable_count =
                        preview_state.as_ref().map_or(0, PlaylistSelectionState::selectable_count);
                    if let Ok(mut state) = event_playlist_selection.lock() {
                        *state = preview_state;
                    }
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_analyzed_title(title.into());
                            window.set_analyzed_details(details.into());
                            window.set_analyzed_is_playlist(is_playlist);
                            window.set_playlist_entries(ModelRc::new(VecModel::from(preview_rows)));
                            window.set_selected_playlist_count(0);
                            window.set_selectable_playlist_count(selectable_count as i32);
                            window.set_playlist_selection_summary(selection_summary(
                                0,
                                selectable_count,
                            ));
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
    let analysis_generation = Arc::new(AtomicU64::new(0));
    let analyze_generation = analysis_generation.clone();
    window.on_analyze_requested(move |submitted_url| {
        let request_id = analyze_generation.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = analyze_events.send(UiEvent::ClearPreview { request_id });
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
                    let _ =
                        events.send(UiEvent::Metadata { request_id, source_url: url, metadata });
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
    let enqueue_window = window.as_weak();
    let queue_slots = Arc::new(Semaphore::new(1));
    let enqueue_slots = queue_slots.clone();
    let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let enqueue_active_jobs = active_jobs.clone();
    let queue_handle = handle.clone();
    let pending = runtime.block_on(recover_pending(&store))?;
    for record in pending {
        let store = store.clone();
        let engine = engine.clone();
        let events = events_tx.clone();
        let slots = enqueue_slots.clone();
        let active_jobs = active_jobs.clone();
        handle.spawn(run_download_job(record, store, engine, events, slots, active_jobs));
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
        let (audio_only, selected_format, selected_quality) = enqueue_window
            .upgrade()
            .map(|window| {
                (
                    window.get_audio_only(),
                    window.get_audio_format().to_string(),
                    window.get_audio_quality().to_string(),
                )
            })
            .unwrap_or_default();
        let mut record = DownloadRecord::queued(Uuid::new_v4().to_string(), url, now());
        record.download_mode =
            if audio_only { DownloadMode::AudioOnly } else { DownloadMode::Video };
        record.audio_format =
            AudioFormat::from_database_value(&selected_format).unwrap_or_default();
        record.audio_quality =
            AudioQuality::from_database_value(&selected_quality).unwrap_or_default();
        let store = queue_store.clone();
        let engine = queue_engine.clone();
        let events = queue_events.clone();
        let slots = enqueue_slots.clone();
        let active_jobs = enqueue_active_jobs.clone();
        queue_handle.spawn(async move {
            if let Err(error) = store.save_download(record.clone()).await {
                let _ = events.send(UiEvent::Status(format!("Could not save queue item: {error}")));
                return;
            }
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status("Added to the download queue.".to_owned()));
            run_download_job(record, store, engine, events, slots, active_jobs).await;
        });
    });

    let toggle_selection = playlist_selection.clone();
    let toggle_window = window.as_weak();
    window.on_playlist_entry_toggle_requested(move |row_index| {
        let Ok(row_index) = usize::try_from(row_index) else { return };
        let Ok(mut selection) = toggle_selection.lock() else { return };
        let Some(selection) = selection.as_mut() else { return };
        if selection.entries.get(row_index).is_none_or(|entry| entry.playlist_index.is_none()) {
            return;
        }
        let Some(selected) = selection.selected.get_mut(row_index) else { return };
        *selected = !*selected;
        let rows = playlist_entry_rows(selection);
        let selected_count = selection.selected_count();
        let selectable_count = selection.selectable_count();
        if let Some(window) = toggle_window.upgrade() {
            window.set_playlist_entries(ModelRc::new(VecModel::from(rows)));
            window.set_selected_playlist_count(selected_count as i32);
            window.set_selectable_playlist_count(selectable_count as i32);
            window.set_playlist_selection_summary(selection_summary(
                selected_count,
                selectable_count,
            ));
        }
    });

    let selected_queue_store = store.clone();
    let selected_queue_engine = engine.clone();
    let selected_queue_events = events_tx.clone();
    let selected_queue_handle = handle.clone();
    let selected_queue_slots = queue_slots.clone();
    let selected_active_jobs = active_jobs.clone();
    let selected_playlist = playlist_selection.clone();
    let selected_queue_window = window.as_weak();
    window.on_enqueue_selected_playlist_entries_requested(move || {
        let selection = selected_playlist.lock().ok().and_then(|state| state.clone());
        let Some(selection) = selection else {
            let _ = selected_queue_events
                .send(UiEvent::Status("Analyze a playlist before selecting entries.".to_owned()));
            return;
        };
        let mut selected_entries = selection
            .entries
            .iter()
            .zip(selection.selected.iter())
            .filter_map(|(entry, selected)| {
                (*selected)
                    .then_some(entry.playlist_index)
                    .flatten()
                    .map(|index| (entry.title.clone(), index))
            })
            .collect::<Vec<_>>();
        selected_entries.sort_by_key(|(_, playlist_index)| *playlist_index);
        if selected_entries.is_empty() {
            let _ = selected_queue_events.send(UiEvent::Status(
                "Select one or more entries with a valid playlist index.".to_owned(),
            ));
            return;
        }
        let (audio_only, selected_format, selected_quality) = selected_queue_window
            .upgrade()
            .map(|window| {
                (
                    window.get_audio_only(),
                    window.get_audio_format().to_string(),
                    window.get_audio_quality().to_string(),
                )
            })
            .unwrap_or_default();
        let download_mode = if audio_only { DownloadMode::AudioOnly } else { DownloadMode::Video };
        let audio_format = AudioFormat::from_database_value(&selected_format).unwrap_or_default();
        let audio_quality =
            AudioQuality::from_database_value(&selected_quality).unwrap_or_default();
        let _ = selected_queue_events.send(UiEvent::Status(format!(
            "Adding {} selected playlist entries to the queue…",
            selected_entries.len()
        )));
        let store = selected_queue_store.clone();
        let engine = selected_queue_engine.clone();
        let events = selected_queue_events.clone();
        let slots = selected_queue_slots.clone();
        let active_jobs = selected_active_jobs.clone();
        selected_queue_handle.spawn(async move {
            let mut saved = Vec::new();
            let mut save_error = None;
            let selected_count = selected_entries.len();
            for (title, playlist_index) in selected_entries {
                let mut record = DownloadRecord::queued(
                    Uuid::new_v4().to_string(),
                    selection.source_url.clone(),
                    now(),
                );
                record.title = title;
                record.source = selection.source.clone();
                record.playlist_index = Some(playlist_index);
                record.download_mode = download_mode;
                record.audio_format = audio_format;
                record.audio_quality = audio_quality;
                match store.save_download(record.clone()).await {
                    Ok(()) => saved.push(record),
                    Err(error) => {
                        error!(%error, playlist_index, "persist selected playlist item");
                        save_error =
                            Some(format!("Saving playlist item {playlist_index} failed: {error}"));
                        break;
                    }
                }
            }
            if saved.is_empty() {
                let _ = events.send(UiEvent::Refresh);
                if let Some(error) = save_error {
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not save any selected playlist entries. {error}"
                    )));
                }
                return;
            }
            let _ = events.send(UiEvent::Refresh);
            let status = if let Some(error) = save_error {
                format!(
                    "Queued {} of {selected_count} selected playlist entries. {error}",
                    saved.len()
                )
            } else {
                format!(
                    "Queued {} selected playlist entries. Each item will download separately.",
                    saved.len()
                )
            };
            let _ = events.send(UiEvent::Status(status));
            for record in saved {
                run_download_job(
                    record,
                    store.clone(),
                    engine.clone(),
                    events.clone(),
                    slots.clone(),
                    active_jobs.clone(),
                )
                .await;
            }
        });
    });

    let retry_store = store.clone();
    let retry_engine = engine.clone();
    let retry_events = events_tx.clone();
    let retry_slots = queue_slots.clone();
    let retry_active_jobs = active_jobs.clone();
    let retry_handle = handle.clone();
    window.on_retry_download_requested(move |id| {
        let id = id.to_string();
        let store = retry_store.clone();
        let engine = retry_engine.clone();
        let events = retry_events.clone();
        let slots = retry_slots.clone();
        let active_jobs = retry_active_jobs.clone();
        retry_handle.spawn(async move {
            match store.retry_download(&id, now()).await {
                Ok(Some(record)) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(
                        "Retry queued. yt-dlp will continue a compatible partial file.".to_owned(),
                    ));
                    run_download_job(record, store, engine, events, slots, active_jobs).await;
                }
                Ok(None) => {
                    let _ = events.send(UiEvent::Status(
                        "Only failed, interrupted, or canceled downloads can be retried."
                            .to_owned(),
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

    let cancel_store = store.clone();
    let cancel_events = events_tx.clone();
    let cancel_handle = handle.clone();
    let cancel_active_jobs = active_jobs.clone();
    window.on_cancel_download_requested(move |id| {
        let id = id.to_string();
        let store = cancel_store.clone();
        let events = cancel_events.clone();
        let active_jobs = cancel_active_jobs.clone();
        cancel_handle.spawn(async move {
            match store.cancel_queued_download(&id, now()).await {
                Ok(true) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(
                        "Canceled the queued item before it started.".to_owned(),
                    ));
                }
                Ok(false) => {
                    let cancel_sender =
                        active_jobs.lock().ok().and_then(|jobs| jobs.get(&id).cloned());
                    let Some(cancel_sender) = cancel_sender else {
                        let _ = events.send(UiEvent::Refresh);
                        let _ = events.send(UiEvent::Status(
                            "This item has already started or is no longer queued.".to_owned(),
                        ));
                        return;
                    };
                    match store.request_active_cancel(&id, now()).await {
                        Ok(true) => {
                            cancel_sender.send_replace(true);
                            let _ = events.send(UiEvent::Refresh);
                            let _ = events.send(UiEvent::Status(
                                "Stopping the active download; partial files will be kept."
                                    .to_owned(),
                            ));
                        }
                        Ok(false) => {
                            let _ = events.send(UiEvent::Refresh);
                            let _ = events.send(UiEvent::Status(
                                "This item has already finished or is being stopped.".to_owned(),
                            ));
                        }
                        Err(error) => {
                            error!(%error, job_id = %id, "request active download cancellation");
                            let _ = events.send(UiEvent::Status(format!(
                                "Could not stop that active download: {error}"
                            )));
                        }
                    }
                }
                Err(error) => {
                    error!(%error, job_id = %id, "request download cancellation");
                    let _ = events
                        .send(UiEvent::Status(format!("Could not cancel that download: {error}")));
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
        if record.status == "Canceling" {
            record.status = "Failed".to_owned();
            record.error = Some(
                "The app closed before it could confirm process-tree cancellation. Partial files were kept; check that no media process is still using them before retrying.".to_owned(),
            );
            record.updated_at = now();
            store.save_download(record.clone()).await?;
            continue;
        }
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
    pending.sort_by_key(|record| record.queue_order);
    Ok(pending)
}

async fn run_download_job(
    pending: DownloadRecord,
    store: Store,
    engine: DownloadEngine,
    events: mpsc::UnboundedSender<UiEvent>,
    slots: Arc<Semaphore>,
    active_jobs: ActiveJobs,
) {
    let Ok(_slot) = slots.acquire_owned().await else {
        return;
    };
    let (cancel_sender, mut cancel_receiver) = watch::channel(false);
    let cancel_sender = Arc::new(cancel_sender);
    match active_jobs.lock() {
        Ok(mut jobs) => {
            jobs.insert(pending.id.clone(), cancel_sender.clone());
        }
        Err(_) => {
            let _ = events.send(UiEvent::Status(
                "Could not register controls for this queue item.".to_owned(),
            ));
            return;
        }
    }
    let _registration =
        ActiveJobRegistration { id: pending.id.clone(), jobs: active_jobs, sender: cancel_sender };

    let mut active = match store.claim_queued_download(&pending.id, now()).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            let _ = events.send(UiEvent::Refresh);
            return;
        }
        Err(error) => {
            error!(%error, job_id = %pending.id, "claim queued download");
            let _ = events.send(UiEvent::Status(format!("Could not start queued item: {error}")));
            return;
        }
    };
    let _ = events.send(UiEvent::Refresh);

    match engine
        .inspect_with_playlist_index_cancellable(
            &active.url,
            active.playlist_index,
            &mut cancel_receiver,
        )
        .await
    {
        Ok(EngineOutcome::Completed(metadata)) => {
            if *cancel_receiver.borrow() {
                finish_canceled_job(active, store, events).await;
                return;
            }
            active.title = metadata.title;
            active.source = metadata.source;
            active.status = "Downloading".to_owned();
            active.updated_at = now();
            active.error = None;
            match store.update_active_download(active.clone()).await {
                Ok(true) => {}
                Ok(false) => {
                    let _ = events.send(UiEvent::Refresh);
                    let canceling = match store.is_canceling(&active.id).await {
                        Ok(canceling) => canceling,
                        Err(error) => {
                            error!(%error, job_id = %active.id, "check cancellation after rejected phase update");
                            false
                        }
                    };
                    if *cancel_receiver.borrow() || canceling {
                        finish_canceled_job(active, store, events).await;
                    }
                    return;
                }
                Err(error) => {
                    error!(%error, job_id = %active.id, "persist download state");
                }
            }
            let _ = events.send(UiEvent::Refresh);
        }
        Ok(EngineOutcome::Canceled) => {
            finish_canceled_job(active, store, events).await;
            return;
        }
        Err(error) => {
            active.status = "Failed".to_owned();
            active.error = Some(error.to_string());
            active.updated_at = now();
            match finish_failed_job(active.clone(), &store).await {
                Ok(true) => {}
                Ok(false) => {
                    let _ = events.send(UiEvent::Refresh);
                    return;
                }
                Err(save_error) => {
                    error!(%save_error, job_id = %active.id, "persist analysis failure");
                }
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
                if let Err(error) =
                    progress_store.update_download_progress(progress_record.clone()).await
                {
                    error!(%error, job_id = %progress_record.id, "persist progress");
                }
                last_persist = tokio::time::Instant::now();
                let _ = progress_events.send(UiEvent::Refresh);
            }
        }
        progress_record
    });

    let download_result = engine
        .download_with_playlist_index_cancellable(
            &active.url,
            active.playlist_index,
            active.download_mode,
            active.audio_format,
            active.audio_quality,
            &mut cancel_receiver,
            move |progress| {
                let _ = progress_tx.send(progress);
            },
        )
        .await;
    let mut final_record = progress_task.await.unwrap_or_else(|_| active.clone());
    let (downloaded_successfully, mut canceled, mut status_message) = match download_result {
        Ok(EngineOutcome::Completed(result)) => {
            final_record.status = "Completed".to_owned();
            final_record.progress = 100.0;
            final_record.output_path = result.output_path;
            final_record.error = None;
            (true, false, "Download complete.".to_owned())
        }
        Ok(EngineOutcome::Canceled) => {
            final_record.status = "Canceled".to_owned();
            final_record.error = Some("Canceled by user; partial files were kept.".to_owned());
            (false, true, "Canceled the active download. Partial files were kept.".to_owned())
        }
        Err(error) => {
            final_record.status = "Failed".to_owned();
            final_record.error = Some(error.to_string());
            (false, false, error.to_string())
        }
    };
    final_record.updated_at = now();
    let persisted = if canceled {
        store.finish_canceled_download(final_record).await
    } else if downloaded_successfully {
        match store.finish_download(final_record.clone()).await {
            Ok(true) => Ok(true),
            Ok(false) => {
                final_record.status = "Canceled".to_owned();
                final_record.error =
                    Some("Canceled by user; the process had already finished.".to_owned());
                canceled = true;
                status_message =
                    "Canceled the active download. Any completed media file was kept.".to_owned();
                store.finish_canceled_download(final_record).await
            }
            Err(error) => Err(error),
        }
    } else {
        finish_failed_job(final_record, &store).await
    };
    match persisted {
        Ok(true) => {
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status(status_message));
        }
        Ok(false) => {
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status(
                "The queue item changed before its final state could be saved.".to_owned(),
            ));
        }
        Err(error) => {
            error!(%error, job_id = %active.id, "persist terminal download state");
            let message = if downloaded_successfully {
                "Media downloaded, but the app could not save its completed state to the library."
            } else if canceled {
                "The process stopped, but the app could not save the canceled state."
            } else {
                "The download failed, and the app could not save the final queue state."
            };
            let _ = events.send(UiEvent::Status(message.to_owned()));
        }
    }
}

async fn finish_failed_job(record: DownloadRecord, store: &Store) -> Result<bool> {
    if store.fail_canceling_download(record.clone()).await? {
        return Ok(true);
    }
    if store.finish_download(record.clone()).await? {
        return Ok(true);
    }
    store.fail_canceling_download(record).await
}

async fn finish_canceled_job(
    mut record: DownloadRecord,
    store: Store,
    events: mpsc::UnboundedSender<UiEvent>,
) {
    let job_id = record.id.clone();
    record.status = "Canceled".to_owned();
    record.error = Some("Canceled by user; partial files were kept.".to_owned());
    record.updated_at = now();
    match store.finish_canceled_download(record).await {
        Ok(true) => {
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status(
                "Canceled the active item. Partial files were kept.".to_owned(),
            ));
        }
        Ok(false) => {
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status(
                "The queue item changed before cancellation could be saved.".to_owned(),
            ));
        }
        Err(error) => {
            error!(%error, job_id = %job_id, "persist canceled download state");
            let _ = events.send(UiEvent::Status(
                "The process stopped, but the app could not save the canceled state.".to_owned(),
            ));
        }
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
    let details = match (
        record.status.as_str(),
        record.error.as_deref(),
        record.downloaded_bytes,
        record.total_bytes,
    ) {
        ("Failed" | "Canceled" | "Interrupted", Some(error), _, _) => error.to_owned(),
        (_, _, Some(downloaded), Some(total)) => {
            format!("{} of {}", format_bytes(Some(downloaded)), format_bytes(Some(total)))
        }
        (_, _, Some(downloaded), None) => format!("{} downloaded", format_bytes(Some(downloaded))),
        (_, Some(error), _, _) => error.to_owned(),
        _ => record.url.clone(),
    };
    let selection = match record.download_mode {
        DownloadMode::Video => "Video".to_owned(),
        DownloadMode::AudioOnly if record.audio_format.supports_quality_setting() => {
            format!("Audio · {} · {}", record.audio_format.label(), record.audio_quality.label())
        }
        DownloadMode::AudioOnly => format!("Audio · {}", record.audio_format.label()),
    };
    let details = format!("{selection} · {details}");
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
    fn row_view_shows_terminal_error_before_saved_progress() {
        let mut record = DownloadRecord::queued(
            "canceling".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        record.status = "Failed".to_owned();
        record.error = Some(
            "Check that no media process is still using the partial file before retrying."
                .to_owned(),
        );
        record.downloaded_bytes = Some(22);
        record.total_bytes = Some(100);

        let row = row_view(&record);
        assert!(row.detail.as_str().contains(record.error.as_deref().unwrap()));
        assert!(row.detail.as_str().starts_with("Video · "));
    }

    #[test]
    fn row_view_labels_audio_output_and_selected_format() {
        let mut record = DownloadRecord::queued(
            "audio-job".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        record.download_mode = DownloadMode::AudioOnly;
        record.audio_format = AudioFormat::Mp3;

        let row = row_view(&record);

        assert!(row.detail.as_str().starts_with("Audio · MP3 · Best · "));
    }

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
        active.playlist_index = Some(3);
        active.download_mode = DownloadMode::AudioOnly;
        active.audio_format = AudioFormat::Opus;
        active.audio_quality = AudioQuality::K192;
        active.downloaded_bytes = Some(75);
        active.total_bytes = Some(100);
        let mut later_playlist_item = DownloadRecord::queued(
            "selected-later".to_owned(),
            "https://example.test/playlist".to_owned(),
            30,
        );
        later_playlist_item.playlist_index = Some(9);
        let mut earlier_playlist_item = DownloadRecord::queued(
            "selected-earlier".to_owned(),
            "https://example.test/playlist".to_owned(),
            30,
        );
        earlier_playlist_item.playlist_index = Some(2);
        let mut canceling = DownloadRecord::queued(
            "canceling".to_owned(),
            "https://example.test/canceling".to_owned(),
            40,
        );
        canceling.status = "Canceling".to_owned();
        canceling.progress = 22.0;
        canceling.downloaded_bytes = Some(22);
        canceling.total_bytes = Some(100);
        store.save_download(queued.clone()).await.unwrap();
        store.save_download(active.clone()).await.unwrap();
        store.save_download(later_playlist_item).await.unwrap();
        store.save_download(earlier_playlist_item).await.unwrap();
        store.save_download(canceling).await.unwrap();

        let pending = recover_pending(&store).await.unwrap();
        assert_eq!(pending.len(), 4);
        assert!(pending.iter().all(|record| record.status == "Queued"));
        assert_eq!(
            pending.iter().map(|record| record.id.as_str()).collect::<Vec<_>>(),
            ["queued", "active", "selected-later", "selected-earlier"]
        );
        let rows = store.list_downloads().await.unwrap();
        let recovered_active = rows.iter().find(|record| record.id == "active").unwrap();
        assert_eq!(recovered_active.status, "Queued");
        assert_eq!(recovered_active.downloaded_bytes, Some(75));
        assert_eq!(recovered_active.playlist_index, Some(3));
        assert_eq!(recovered_active.download_mode, DownloadMode::AudioOnly);
        assert_eq!(recovered_active.audio_format, AudioFormat::Opus);
        assert_eq!(recovered_active.audio_quality, AudioQuality::K192);
        let recovered_canceling = rows.iter().find(|record| record.id == "canceling").unwrap();
        assert_eq!(recovered_canceling.status, "Failed");
        assert_eq!(recovered_canceling.progress, 22.0);
        assert_eq!(recovered_canceling.downloaded_bytes, Some(22));
        assert!(
            recovered_canceling.error.as_deref().unwrap().contains("process-tree cancellation")
        );
        assert!(!pending.iter().any(|record| record.id == "canceling"));
        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}
