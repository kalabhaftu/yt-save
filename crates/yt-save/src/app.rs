use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use directories::{ProjectDirs, UserDirs};
use slint::{CloseRequestResponse, ComponentHandle, ModelRc, SharedString, VecModel};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, mpsc, watch},
    task::JoinSet,
};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    AppTray, ClipboardPopup, DownloadRowData, LibraryRowData, MainWindow, PlaylistEntryRowData,
    clipboard_monitor::{ClipboardCandidate, ClipboardMonitor, WorkArea},
    dependencies::DependencyPaths,
    engine::{
        DownloadEngine, DownloadOptions, EngineOutcome, JobSignal, ProgressUpdate,
        is_retryable_engine_error,
    },
    history::download_history_csv,
    model::{
        DownloadMode, DownloadPresets, DownloadRecord, MediaMetadata, PlaylistEntry, format_bytes,
        format_duration,
    },
    persistence::{
        DEFAULT_AUTO_RETRY_ATTEMPTS, DEFAULT_CONCURRENT_FRAGMENTS, MAX_AUTO_RETRY_ATTEMPTS,
        QueueMove, Store, TrayPreferences,
    },
};

enum UiEvent {
    Refresh,
    FilterHistory(String),
    FilterLibrary(String),
    LibraryFilter(String),
    LibrarySort(String),
    LibraryView(bool),
    RefreshLibrary,
    LibraryScanComplete {
        generation: u64,
        signature: LibraryCacheSignature,
        result: std::result::Result<Vec<LibraryCacheEntry>, String>,
    },
    Status(String),
    ClipboardCandidate {
        generation: u64,
        candidate: ClipboardCandidate,
    },
    DependencyStatus(String),
    ClearPreview {
        request_id: u64,
    },
    Metadata {
        request_id: u64,
        source_url: String,
        metadata: MediaMetadata,
    },
}

type ActiveJobs = Arc<Mutex<std::collections::HashMap<String, Arc<watch::Sender<JobSignal>>>>>;

type LibraryCacheSignature =
    Vec<(String, String, String, String, Option<String>, String, Option<u64>, i64)>;

#[derive(Clone)]
struct LibraryCacheEntry {
    row: LibraryRowData,
    searchable_text: String,
    title_sort_key: String,
    downloaded_at: i64,
    size_for_sort: Option<u64>,
}

struct TrayContext {
    store: Store,
    handle: tokio::runtime::Handle,
    events: mpsc::UnboundedSender<UiEvent>,
    active_jobs: ActiveJobs,
    queue_wake: Arc<Notify>,
    preferences: Arc<Mutex<TrayPreferences>>,
    shutdown_started: Arc<AtomicBool>,
}

struct ActiveJobRegistration {
    id: String,
    jobs: ActiveJobs,
    sender: Arc<watch::Sender<JobSignal>>,
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

fn set_playlist_selection(selection: &mut PlaylistSelectionState, selected: bool) {
    for (entry, selected_entry) in selection.entries.iter().zip(selection.selected.iter_mut()) {
        *selected_entry = selected && entry.playlist_index.is_some();
    }
}

fn select_playlist_range(
    selection: &mut PlaylistSelectionState,
    first_index: i64,
    last_index: i64,
) -> bool {
    if first_index < 1 || last_index < first_index {
        return false;
    }
    let mut matched = false;
    for (entry, selected_entry) in selection.entries.iter().zip(selection.selected.iter_mut()) {
        if entry.playlist_index.is_some_and(|index| (first_index..=last_index).contains(&index)) {
            *selected_entry = true;
            matched = true;
        }
    }
    matched
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

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("yt-save-worker")
        .build()
        .context("create background runtime")?;
    let fallback_directory = validate_download_directory(&paths.downloads)?;
    let configured_directory =
        runtime.block_on(store.get_or_initialize_download_directory(
            fallback_directory.to_string_lossy().into_owned(),
        ))?;
    let download_directory = match validate_download_directory(Path::new(&configured_directory)) {
        Ok(directory) => directory,
        Err(error) => {
            error!(%error, "saved download destination is unavailable; restoring the default folder");
            runtime.block_on(store.set_default_download_directory(
                fallback_directory.to_string_lossy().into_owned(),
                now(),
            ))?;
            fallback_directory
        }
    };
    let download_directory_text = download_directory.to_string_lossy().into_owned();
    runtime
        .block_on(store.backfill_legacy_download_directories(download_directory_text.clone()))?;
    let download_presets = runtime.block_on(store.load_download_presets())?;
    let max_concurrent_downloads = runtime.block_on(store.load_max_concurrent_downloads())?;
    let concurrent_fragments = runtime.block_on(store.load_concurrent_fragments())?;
    let auto_retry_attempts = runtime.block_on(store.load_auto_retry_attempts())?;
    let clipboard_monitoring_enabled =
        runtime.block_on(store.load_clipboard_monitoring_enabled())?;
    let tray_preferences = runtime.block_on(store.load_tray_preferences())?;
    let tray_preferences_state = Arc::new(Mutex::new(tray_preferences));
    let engine = DownloadEngine::new(dependencies.clone(), download_directory.clone());
    let handle = runtime.handle().clone();
    let window = MainWindow::new().context("create main window")?;
    let tray = AppTray::new().context("create system tray icon")?;
    #[cfg(windows)]
    let popup = crate::window_activation::create_non_activating_window(ClipboardPopup::new)
        .context("create clipboard popup")?;
    #[cfg(not(windows))]
    let popup = ClipboardPopup::new().context("create clipboard popup")?;
    window.set_product_name(env!("YT_SAVE_PRODUCT_NAME").into());
    window.set_app_version(env!("YT_SAVE_PRODUCT_VERSION").into());
    window.set_download_directory(download_directory_text.into());
    window.set_audio_only(download_presets.mode == DownloadMode::AudioOnly);
    window.set_audio_format(download_presets.audio_format.as_database_value().into());
    window.set_audio_quality(download_presets.audio_quality.as_database_value().into());
    window.set_video_quality(download_presets.video_quality.label().into());
    window.set_max_concurrent_downloads(max_concurrent_downloads.to_string().into());
    window.set_concurrent_fragments(concurrent_fragments.to_string().into());
    window.set_auto_retry_attempts(
        if auto_retry_attempts == 0 { "Off".to_owned() } else { auto_retry_attempts.to_string() }
            .into(),
    );
    window.set_clipboard_monitoring_enabled(false);
    window.set_close_to_tray_enabled(tray_preferences.close_to_tray);
    window.set_minimize_to_tray_enabled(tray_preferences.minimize_to_tray);
    let saved_downloads = runtime.block_on(store.list_downloads())?;
    window.set_queue_summary(queue_summary_text(&saved_downloads).into());
    let saved_active_count =
        saved_downloads.iter().filter(|record| is_active_status(&record.status)).count();
    let saved_paused_count =
        saved_downloads.iter().filter(|record| record.status == "Paused").count();
    tray.set_active_count(saved_active_count as i32);
    tray.set_paused_count(saved_paused_count as i32);
    tray.set_tray_tooltip(tray_tooltip(saved_active_count, saved_paused_count).into());
    let saved_rows = row_views(&saved_downloads);
    window.set_downloads(ModelRc::new(VecModel::from(saved_rows)));
    let event_initial_records = saved_downloads;

    let (events_tx, mut events_rx) = mpsc::unbounded_channel::<UiEvent>();
    let clipboard_monitor_slot = Arc::new(Mutex::new(None::<ClipboardMonitor>));
    let clipboard_monitor_generation = Arc::new(AtomicU64::new(0));
    let clipboard_popup_generation = Arc::new(AtomicU64::new(0));
    let clipboard_popup_work_area = Arc::new(Mutex::new(None::<WorkArea>));
    #[cfg(windows)]
    configure_clipboard_popup_window(&popup, clipboard_popup_work_area.clone());
    let shutdown_started = Arc::new(AtomicBool::new(false));
    let playlist_selection = Arc::new(Mutex::new(None::<PlaylistSelectionState>));
    let weak_window = window.as_weak();
    let weak_tray = tray.as_weak();
    let event_store = store.clone();
    let event_playlist_selection = playlist_selection.clone();
    let event_clipboard_generation = clipboard_monitor_generation.clone();
    let event_popup = popup.as_weak();
    let event_popup_generation = clipboard_popup_generation.clone();
    let event_popup_work_area = clipboard_popup_work_area.clone();
    let event_library_events = events_tx.clone();
    runtime.spawn(async move {
        let mut latest_analysis_id = 0;
        let mut history_search = String::new();
        let mut library_search = String::new();
        let mut library_filter = "All files".to_owned();
        let mut library_sort = "Newest first".to_owned();
        let mut cached_downloads = event_initial_records;
        let mut library_visible = false;
        let mut library_cache: Vec<LibraryCacheEntry> = Vec::new();
        let mut library_cache_signature: LibraryCacheSignature = Vec::new();
        let mut pending_library_signature = None;
        let mut library_scan_generation = 0;
        let library_events = event_library_events;
        while let Some(event) = events_rx.recv().await {
            match event {
                UiEvent::Refresh => match event_store.list_downloads().await {
                    Ok(records) => {
                        let records_signature = library_cache_signature_for(&records);
                        cached_downloads = records;
                        let active_count = cached_downloads
                            .iter()
                            .filter(|record| is_active_status(&record.status))
                            .count();
                        let paused_count = cached_downloads
                            .iter()
                            .filter(|record| record.status == "Paused")
                            .count();
                        let rows = row_views_filtered(&cached_downloads, &history_search);
                        if library_visible
                            && records_signature != library_cache_signature
                            && pending_library_signature.as_ref() != Some(&records_signature)
                        {
                            request_library_scan(
                                library_events.clone(),
                                &mut library_scan_generation,
                                &mut pending_library_signature,
                                cached_downloads.clone(),
                                records_signature,
                            );
                        }
                        let summary = queue_summary_text(&cached_downloads);
                        let weak = weak_window.clone();
                        let weak_tray = weak_tray.clone();
                        let tooltip = tray_tooltip(active_count, paused_count);
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(window) = weak.upgrade() {
                                window.set_downloads(ModelRc::new(VecModel::from(rows)));
                                window.set_queue_summary(summary.into());
                            }
                            if let Some(tray) = weak_tray.upgrade() {
                                tray.set_active_count(active_count as i32);
                                tray.set_paused_count(paused_count as i32);
                                tray.set_tray_tooltip(tooltip.into());
                            }
                        });
                    }
                    Err(error) => error!(%error, "refresh download list"),
                },
                UiEvent::FilterHistory(query) => {
                    history_search = query;
                    let rows = row_views_filtered(&cached_downloads, &history_search);
                    let weak = weak_window.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_downloads(ModelRc::new(VecModel::from(rows)));
                        }
                    });
                }
                UiEvent::FilterLibrary(query) => {
                    library_search = query;
                    let rows = library_row_views_from_cache(
                        &library_cache,
                        &library_search,
                        &library_filter,
                        &library_sort,
                    );
                    let weak = weak_window.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_library_entries(ModelRc::new(VecModel::from(rows)));
                        }
                    });
                }
                UiEvent::LibraryFilter(filter) => {
                    library_filter = filter;
                    let rows = library_row_views_from_cache(
                        &library_cache,
                        &library_search,
                        &library_filter,
                        &library_sort,
                    );
                    let weak = weak_window.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_library_entries(ModelRc::new(VecModel::from(rows)));
                        }
                    });
                }
                UiEvent::LibrarySort(sort) => {
                    library_sort = sort;
                    let rows = library_row_views_from_cache(
                        &library_cache,
                        &library_search,
                        &library_filter,
                        &library_sort,
                    );
                    let weak = weak_window.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_library_entries(ModelRc::new(VecModel::from(rows)));
                        }
                    });
                }
                UiEvent::LibraryView(visible) => {
                    library_visible = visible;
                    if visible {
                        let signature = library_cache_signature_for(&cached_downloads);
                        request_library_scan(
                            library_events.clone(),
                            &mut library_scan_generation,
                            &mut pending_library_signature,
                            cached_downloads.clone(),
                            signature,
                        );
                    }
                }
                UiEvent::RefreshLibrary => {
                    if library_visible {
                        let signature = library_cache_signature_for(&cached_downloads);
                        request_library_scan(
                            library_events.clone(),
                            &mut library_scan_generation,
                            &mut pending_library_signature,
                            cached_downloads.clone(),
                            signature,
                        );
                    }
                }
                UiEvent::LibraryScanComplete { generation, signature, result } => {
                    if generation != library_scan_generation {
                        continue;
                    }
                    pending_library_signature = None;
                    if !library_visible {
                        continue;
                    }
                    match result {
                        Ok(entries) => {
                            library_cache_signature = signature;
                            library_cache = entries;
                            let rows = library_row_views_from_cache(
                                &library_cache,
                                &library_search,
                                &library_filter,
                                &library_sort,
                            );
                            let weak = weak_window.clone();
                            let _ = slint::invoke_from_event_loop(move || {
                                if let Some(window) = weak.upgrade() {
                                    window.set_library_entries(ModelRc::new(VecModel::from(rows)));
                                }
                            });
                        }
                        Err(message) => error!(%message, "scan completed media for library view"),
                    }
                }
                UiEvent::Status(message) => {
                    let weak = weak_window.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_status_message(message.into());
                        }
                    });
                }
                UiEvent::ClipboardCandidate { generation, candidate } => {
                    if event_clipboard_generation.load(Ordering::Relaxed) != generation {
                        continue;
                    }
                    let weak = weak_window.clone();
                    let popup = event_popup.clone();
                    let popup_generation = event_popup_generation.clone();
                    let popup_work_area = event_popup_work_area.clone();
                    let active_generation = event_clipboard_generation.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if active_generation.load(Ordering::Relaxed) == generation
                            && let Some(window) = weak.upgrade()
                            && window.get_clipboard_monitoring_enabled()
                        {
                            window.set_clipboard_candidate(candidate.url.clone().into());
                            window.set_status_message(
                                "A copied web link is ready for review. Nothing starts until you choose an action."
                                    .into(),
                            );
                            #[cfg(windows)]
                            let main_window_is_focused = main_window_has_focus(&window);
                            #[cfg(not(windows))]
                            let main_window_is_focused = false;
                            let popup_id = popup_generation.fetch_add(1, Ordering::Relaxed) + 1;
                            if candidate.notifications_allowed
                                && !main_window_is_focused
                                && let Some(area) = candidate.work_area
                                && let Ok(mut saved_area) = popup_work_area.lock()
                            {
                                *saved_area = Some(area);
                                if let Some(popup) = popup.upgrade() {
                                    popup.set_candidate_url(candidate.url.clone().into());
                                    popup.set_source_host(clipboard_source_host(&candidate.url).into());
                                    popup.window().set_size(slint::LogicalSize::new(440.0, 214.0));
                                    let scale = popup.window().scale_factor();
                                    let (x, y) = initial_popup_position(area, scale, 440, 214);
                                    popup.window().set_position(slint::PhysicalPosition::new(x, y));
                                    if let Err(error) = popup.show() {
                                        error!(%error, "show clipboard review popup");
                                    } else {
                                        let weak_popup = popup.as_weak();
                                        let expiry_generation = popup_generation.clone();
                                        slint::Timer::single_shot(Duration::from_secs(12), move || {
                                            if expiry_generation.load(Ordering::Relaxed) == popup_id
                                                && let Some(popup) = weak_popup.upgrade()
                                                && let Err(error) = popup.hide()
                                            {
                                                error!(%error, "hide expired clipboard review popup");
                                            }
                                        });
                                    }
                                }
                            } else if let Some(popup) = popup.upgrade() {
                                let _ = popup.hide();
                            }
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
                            window.set_show_playlist_range_controls(false);
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
                            window.set_show_playlist_range_controls(false);
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

    if clipboard_monitoring_enabled {
        match start_clipboard_monitor(
            &clipboard_monitor_slot,
            &clipboard_monitor_generation,
            events_tx.clone(),
        ) {
            Ok(()) => window.set_clipboard_monitoring_enabled(true),
            Err(error) => {
                error!(%error, "start saved clipboard monitoring preference");
                window.set_status_message(
                    format!("Clipboard monitoring could not start: {error}").into(),
                );
                let store = store.clone();
                handle.spawn(async move {
                    if let Err(save_error) =
                        store.set_clipboard_monitoring_enabled(false, now()).await
                    {
                        error!(%save_error, "disable unavailable clipboard monitoring preference");
                    }
                });
            }
        }
    }

    let clipboard_request_slot = clipboard_monitor_slot.clone();
    let clipboard_request_generation = clipboard_monitor_generation.clone();
    let clipboard_request_store = store.clone();
    let clipboard_request_handle = handle.clone();
    let clipboard_request_events = events_tx.clone();
    let clipboard_request_window = window.as_weak();
    let clipboard_request_popup = popup.as_weak();
    let clipboard_request_popup_generation = clipboard_popup_generation.clone();
    window.on_clipboard_monitoring_requested(move |enabled| {
        let weak = clipboard_request_window.clone();
        let events = clipboard_request_events.clone();
        if enabled {
            match start_clipboard_monitor(
                &clipboard_request_slot,
                &clipboard_request_generation,
                events.clone(),
            ) {
                Ok(()) => {
                    if let Some(window) = weak.upgrade() {
                        window.set_clipboard_monitoring_enabled(true);
                        window.set_status_message(
                            "Clipboard URL checks are on. Links stay local until you choose Analyze or Add to queue."
                                .into(),
                        );
                    }
                    let store = clipboard_request_store.clone();
                    clipboard_request_handle.spawn(async move {
                        if let Err(error) = store.set_clipboard_monitoring_enabled(true, now()).await
                        {
                            error!(%error, "save clipboard monitoring preference");
                            let _ = events.send(UiEvent::Status(format!(
                                "Clipboard checks are on for this session, but the setting could not be saved: {error}"
                            )));
                        }
                    });
                }
                Err(error) => {
                    if let Some(window) = weak.upgrade() {
                        window.set_clipboard_monitoring_enabled(false);
                        window.set_status_message(
                            format!("Clipboard monitoring could not start: {error}").into(),
                        );
                    }
                }
            }
        } else {
            clipboard_request_popup_generation.fetch_add(1, Ordering::Relaxed);
            if let Some(popup) = clipboard_request_popup.upgrade()
                && let Err(error) = popup.hide()
            {
                error!(%error, "hide clipboard popup after monitoring was disabled");
            }
            match stop_clipboard_monitor(&clipboard_request_slot, &clipboard_request_generation) {
                Ok(()) => {
                    if let Some(window) = weak.upgrade() {
                        window.set_clipboard_monitoring_enabled(false);
                        window.set_clipboard_candidate(SharedString::default());
                        window.set_status_message("Clipboard URL checks are off.".into());
                    }
                    let store = clipboard_request_store.clone();
                    clipboard_request_handle.spawn(async move {
                        if let Err(error) = store.set_clipboard_monitoring_enabled(false, now()).await
                        {
                            error!(%error, "save clipboard monitoring preference");
                            let _ = events.send(UiEvent::Status(format!(
                                "Clipboard checks are off for this session, but the setting could not be saved: {error}"
                            )));
                        }
                    });
                }
                Err(error) => {
                    if let Some(window) = weak.upgrade() {
                        window.set_status_message(
                            format!("Could not stop clipboard monitoring: {error}").into(),
                        );
                    }
                }
            }
        }
    });

    let clipboard_dismiss_window = window.as_weak();
    window.on_clipboard_candidate_dismissed(move || {
        if let Some(window) = clipboard_dismiss_window.upgrade() {
            window.set_clipboard_candidate(SharedString::default());
            window.set_status_message("Copied link dismissed.".into());
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
    let analyze_shutdown_started = shutdown_started.clone();
    window.on_analyze_requested(move |submitted_url| {
        if analyze_shutdown_started.load(Ordering::Acquire) {
            let _ = analyze_events
                .send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
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
    let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let queue_wake = Arc::new(Notify::new());
    let queue_handle = handle.clone();
    let enqueue_shutdown_started = shutdown_started.clone();
    let enqueue_analysis_generation = analysis_generation.clone();
    let enqueue_queue_wake = queue_wake.clone();
    let _ = runtime.block_on(recover_pending(&store))?;
    let dispatcher_shutdown_started = shutdown_started.clone();
    handle.spawn(run_queue_dispatcher(
        store.clone(),
        engine.clone(),
        events_tx.clone(),
        queue_wake.clone(),
        active_jobs.clone(),
        dispatcher_shutdown_started,
    ));
    let _ = events_tx.send(UiEvent::Refresh);

    let history_search_events = events_tx.clone();
    window.on_history_search_requested(move |query| {
        let _ = history_search_events.send(UiEvent::FilterHistory(query.to_string()));
    });

    let library_search_events = events_tx.clone();
    window.on_library_search_requested(move |query| {
        let _ = library_search_events.send(UiEvent::FilterLibrary(query.to_string()));
    });

    let library_filter_events = events_tx.clone();
    window.on_library_filter_requested(move |filter| {
        let _ = library_filter_events.send(UiEvent::LibraryFilter(filter.to_string()));
    });

    let library_sort_events = events_tx.clone();
    window.on_library_sort_requested(move |sort| {
        let _ = library_sort_events.send(UiEvent::LibrarySort(sort.to_string()));
    });

    let library_view_events = events_tx.clone();
    window.on_library_view_requested(move |visible| {
        let _ = library_view_events.send(UiEvent::LibraryView(visible));
    });

    let refresh_library_events = events_tx.clone();
    window.on_library_refresh_requested(move || {
        let _ = refresh_library_events.send(UiEvent::RefreshLibrary);
    });

    window.on_enqueue_requested(move |submitted_url| {
        if enqueue_shutdown_started.load(Ordering::Acquire) {
            let _ =
                queue_events.send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        let url = match validate_url(&submitted_url) {
            Ok(url) => url,
            Err(message) => {
                let _ = queue_events.send(UiEvent::Status(message));
                return;
            }
        };
        let request_id = enqueue_analysis_generation.fetch_add(1, Ordering::AcqRel) + 1;
        let _ = queue_events.send(UiEvent::ClearPreview { request_id });
        let _ = queue_events.send(UiEvent::Status(
            "Checking the link before adding it to the queue…".to_owned(),
        ));
        let (
            audio_only,
            selected_format,
            selected_quality,
            selected_video_quality,
            selected_directory,
        ) = enqueue_window
            .upgrade()
            .map(|window| {
                (
                    window.get_audio_only(),
                    window.get_audio_format().to_string(),
                    window.get_audio_quality().to_string(),
                    window.get_video_quality().to_string(),
                    window.get_download_directory().to_string(),
                )
            })
            .unwrap_or_default();
        let inspection_url = url.clone();
        let mut record = DownloadRecord::queued(Uuid::new_v4().to_string(), url, now());
        let download_presets = DownloadPresets::from_ui_values(
            audio_only,
            &selected_format,
            &selected_quality,
            &selected_video_quality,
        );
        record.download_mode = download_presets.mode;
        record.audio_format = download_presets.audio_format;
        record.audio_quality = download_presets.audio_quality;
        record.video_quality = download_presets.video_quality;
        let store = queue_store.clone();
        let engine = queue_engine.clone();
        let events = queue_events.clone();
        let queue_wake = enqueue_queue_wake.clone();
        let inspector = engine.clone();
        let analysis_generation = enqueue_analysis_generation.clone();
        queue_handle.spawn(async move {
            match inspector.inspect(&inspection_url).await {
                Ok(metadata) if metadata.is_playlist => {
                    if analysis_generation.load(Ordering::Acquire) == request_id {
                        let _ = events.send(UiEvent::Metadata {
                            request_id,
                            source_url: inspection_url,
                            metadata,
                        });
                        let _ = events.send(UiEvent::Status(
                            "This URL contains a playlist. Review its entries and select what to queue."
                                .to_owned(),
                        ));
                    } else {
                        let _ = events.send(UiEvent::Status(
                            "This link contains a playlist. Analyze it again to review its entries before queueing."
                                .to_owned(),
                        ));
                    }
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    error!(%error, "inspect media link before queueing");
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not check this link before queueing: {error}"
                    )));
                    return;
                }
            }

            let destination = match tokio::task::spawn_blocking(move || {
                validate_download_directory(Path::new(&selected_directory))
            })
            .await
            {
                Ok(Ok(path)) => path,
                Ok(Err(error)) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not use the selected save folder: {error}"
                    )));
                    return;
                }
                Err(error) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not validate the selected save folder: {error}"
                    )));
                    return;
                }
            };
            record.download_directory = destination.to_string_lossy().into_owned();
            if let Err(error) = store.save_download(record.clone()).await {
                let _ = events.send(UiEvent::Status(format!("Could not save queue item: {error}")));
                return;
            }
            let preset_status = match store.set_download_presets(download_presets, now()).await {
                Ok(()) => "Added to the download queue.".to_owned(),
                Err(error) => format!(
                    "Added to the queue, but could not save the New download presets: {error}"
                ),
            };
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status(preset_status));
            queue_wake.notify_one();
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

    let set_selection_state = playlist_selection.clone();
    let set_selection_window = window.as_weak();
    window.on_playlist_selection_set_requested(move |selected| {
        let Ok(mut state) = set_selection_state.lock() else { return };
        let Some(selection) = state.as_mut() else { return };
        set_playlist_selection(selection, selected);
        let rows = playlist_entry_rows(selection);
        let selected_count = selection.selected_count();
        let selectable_count = selection.selectable_count();
        if let Some(window) = set_selection_window.upgrade() {
            window.set_playlist_entries(ModelRc::new(VecModel::from(rows)));
            window.set_selected_playlist_count(selected_count as i32);
            window.set_selectable_playlist_count(selectable_count as i32);
            window.set_playlist_selection_summary(selection_summary(
                selected_count,
                selectable_count,
            ));
        }
    });

    let range_selection_state = playlist_selection.clone();
    let range_selection_window = window.as_weak();
    let range_selection_events = events_tx.clone();
    window.on_playlist_range_selection_requested(move |first, last| {
        let first_index = match first.as_str().parse::<i64>() {
            Ok(index) if index > 0 => index,
            _ => {
                let _ = range_selection_events
                    .send(UiEvent::Status("Enter a positive starting playlist index.".to_owned()));
                return;
            }
        };
        let last_index = match last.as_str().parse::<i64>() {
            Ok(index) if index >= first_index => index,
            _ => {
                let _ = range_selection_events.send(UiEvent::Status(
                    "The ending index must be a number at or after the starting index.".to_owned(),
                ));
                return;
            }
        };
        let Ok(mut state) = range_selection_state.lock() else { return };
        let Some(selection) = state.as_mut() else {
            let _ = range_selection_events
                .send(UiEvent::Status("Analyze a playlist before selecting a range.".to_owned()));
            return;
        };
        if !select_playlist_range(selection, first_index, last_index) {
            let _ = range_selection_events.send(UiEvent::Status(
                "That range contains no selectable entries in the visible preview.".to_owned(),
            ));
            return;
        }
        let rows = playlist_entry_rows(selection);
        let selected_count = selection.selected_count();
        let selectable_count = selection.selectable_count();
        if let Some(window) = range_selection_window.upgrade() {
            window.set_playlist_entries(ModelRc::new(VecModel::from(rows)));
            window.set_selected_playlist_count(selected_count as i32);
            window.set_selectable_playlist_count(selectable_count as i32);
            window.set_playlist_selection_summary(selection_summary(
                selected_count,
                selectable_count,
            ));
        }
        let _ = range_selection_events.send(UiEvent::Status(format!(
            "Selected available entries from playlist index {first_index} to {last_index}."
        )));
    });

    let selected_queue_store = store.clone();
    let selected_queue_events = events_tx.clone();
    let selected_queue_handle = handle.clone();
    let selected_queue_wake = queue_wake.clone();
    let selected_playlist = playlist_selection.clone();
    let selected_queue_window = window.as_weak();
    let selected_shutdown_started = shutdown_started.clone();
    window.on_enqueue_selected_playlist_entries_requested(move || {
        if selected_shutdown_started.load(Ordering::Acquire) {
            let _ = selected_queue_events
                .send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
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
        let (
            audio_only,
            selected_format,
            selected_quality,
            selected_video_quality,
            selected_directory,
        ) = selected_queue_window
            .upgrade()
            .map(|window| {
                (
                    window.get_audio_only(),
                    window.get_audio_format().to_string(),
                    window.get_audio_quality().to_string(),
                    window.get_video_quality().to_string(),
                    window.get_download_directory().to_string(),
                )
            })
            .unwrap_or_default();
        let download_presets = DownloadPresets::from_ui_values(
            audio_only,
            &selected_format,
            &selected_quality,
            &selected_video_quality,
        );
        let _ = selected_queue_events.send(UiEvent::Status(format!(
            "Adding {} selected playlist entries to the queue…",
            selected_entries.len()
        )));
        let store = selected_queue_store.clone();
        let events = selected_queue_events.clone();
        let queue_wake = selected_queue_wake.clone();
        selected_queue_handle.spawn(async move {
            let destination = match tokio::task::spawn_blocking(move || {
                validate_download_directory(Path::new(&selected_directory))
            })
            .await
            {
                Ok(Ok(path)) => path,
                Ok(Err(error)) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not use the selected save folder: {error}"
                    )));
                    return;
                }
                Err(error) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not validate the selected save folder: {error}"
                    )));
                    return;
                }
            };
            let destination = destination.to_string_lossy().into_owned();
            let mut records = Vec::with_capacity(selected_entries.len());
            for (title, playlist_index) in selected_entries {
                let mut record = DownloadRecord::queued(
                    Uuid::new_v4().to_string(),
                    selection.source_url.clone(),
                    now(),
                );
                record.title = title;
                record.source = selection.source.clone();
                record.playlist_index = Some(playlist_index);
                record.download_mode = download_presets.mode;
                record.audio_format = download_presets.audio_format;
                record.audio_quality = download_presets.audio_quality;
                record.video_quality = download_presets.video_quality;
                record.download_directory = destination.clone();
                records.push(record);
            }
            let selected_count = records.len();
            if let Err(error) = store.save_downloads(records).await {
                error!(%error, "persist selected playlist batch");
                let _ = events.send(UiEvent::Refresh);
                let _ = events.send(UiEvent::Status(format!(
                    "Could not queue the selected playlist entries: {error}"
                )));
                return;
            }
            let preset_save_error = store.set_download_presets(download_presets, now()).await.err();
            let _ = events.send(UiEvent::Refresh);
            let mut status = format!(
                "Queued {selected_count} selected playlist entries. Each item will download separately."
            );
            if let Some(error) = preset_save_error {
                status.push_str(&format!(" New download presets could not be saved: {error}"));
            }
            let _ = events.send(UiEvent::Status(status));
            queue_wake.notify_one();
        });
    });

    let retry_store = store.clone();
    let retry_events = events_tx.clone();
    let retry_wake = queue_wake.clone();
    let retry_handle = handle.clone();
    let retry_shutdown_started = shutdown_started.clone();
    window.on_retry_download_requested(move |id| {
        if retry_shutdown_started.load(Ordering::Acquire) {
            let _ =
                retry_events.send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        let id = id.to_string();
        let store = retry_store.clone();
        let events = retry_events.clone();
        let queue_wake = retry_wake.clone();
        retry_handle.spawn(async move {
            match store.retry_download(&id, now()).await {
                Ok(Some(_record)) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(
                        "Retry queued. yt-dlp will continue a compatible partial file.".to_owned(),
                    ));
                    queue_wake.notify_one();
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

    let resume_store = store.clone();
    let resume_events = events_tx.clone();
    let resume_wake = queue_wake.clone();
    let resume_handle = handle.clone();
    let resume_shutdown_started = shutdown_started.clone();
    window.on_resume_download_requested(move |id| {
        if resume_shutdown_started.load(Ordering::Acquire) {
            let _ =
                resume_events.send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        let id = id.to_string();
        let store = resume_store.clone();
        let events = resume_events.clone();
        let queue_wake = resume_wake.clone();
        resume_handle.spawn(async move {
            match store.resume_download(&id, now()).await {
                Ok(Some(_record)) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(
                        "Resumed. yt-dlp will continue a compatible partial file.".to_owned(),
                    ));
                    queue_wake.notify_one();
                }
                Ok(None) => {
                    let _ = events
                        .send(UiEvent::Status("Only paused downloads can be resumed.".to_owned()));
                }
                Err(error) => {
                    error!(%error, job_id = %id, "resume paused download");
                    let _ =
                        events.send(UiEvent::Status(format!("Could not resume download: {error}")));
                }
            }
        });
    });

    let move_store = store.clone();
    let move_events = events_tx.clone();
    let move_wake = queue_wake.clone();
    let move_handle = handle.clone();
    window.on_queue_move_requested(move |id, direction| {
        let movement = match direction.as_str() {
            "up" => QueueMove::Up,
            "down" => QueueMove::Down,
            "top" => QueueMove::Top,
            "bottom" => QueueMove::Bottom,
            _ => return,
        };
        let id = id.to_string();
        let store = move_store.clone();
        let events = move_events.clone();
        let queue_wake = move_wake.clone();
        move_handle.spawn(async move {
            match store.move_download(&id, movement).await {
                Ok(true) => {
                    let _ = events.send(UiEvent::Refresh);
                    queue_wake.notify_one();
                }
                Ok(false) => {}
                Err(error) => {
                    error!(%error, job_id = %id, "reorder queued download");
                    let _ =
                        events.send(UiEvent::Status(format!("Could not reorder item: {error}")));
                }
            }
        });
    });

    let priority_store = store.clone();
    let priority_events = events_tx.clone();
    let priority_wake = queue_wake.clone();
    let priority_handle = handle.clone();
    window.on_download_priority_requested(move |id, priority| {
        let id = id.to_string();
        let store = priority_store.clone();
        let events = priority_events.clone();
        let queue_wake = priority_wake.clone();
        priority_handle.spawn(async move {
            match store.set_download_priority(&id, priority, now()).await {
                Ok(true) => {
                    let _ = events.send(UiEvent::Refresh);
                    queue_wake.notify_one();
                }
                Ok(false) => {}
                Err(error) => {
                    error!(%error, job_id = %id, "change download priority");
                    let _ =
                        events.send(UiEvent::Status(format!("Could not change priority: {error}")));
                }
            }
        });
    });

    let concurrency_store = store.clone();
    let concurrency_events = events_tx.clone();
    let concurrency_wake = queue_wake.clone();
    let concurrency_handle = handle.clone();
    let concurrency_generation = Arc::new(AtomicU64::new(0));
    let concurrency_write_lock = Arc::new(AsyncMutex::new(()));
    let concurrency_window = window.as_weak();
    window.on_max_concurrent_downloads_requested(move |value| {
        let Ok(limit) = value.parse::<usize>() else {
            return;
        };
        if !(1..=8).contains(&limit) {
            return;
        }
        let store = concurrency_store.clone();
        let events = concurrency_events.clone();
        let queue_wake = concurrency_wake.clone();
        let generation_state = concurrency_generation.clone();
        let generation = generation_state.fetch_add(1, Ordering::AcqRel) + 1;
        let write_lock = concurrency_write_lock.clone();
        let weak_window = concurrency_window.clone();
        concurrency_handle.spawn(async move {
            let _write_guard = write_lock.lock().await;
            if generation_state.load(Ordering::Acquire) != generation {
                return;
            }
            let previous_limit = store.load_max_concurrent_downloads().await.unwrap_or(1);
            if generation_state.load(Ordering::Acquire) != generation {
                return;
            }
            match store.set_max_concurrent_downloads(limit, now()).await {
                Ok(()) => {
                    queue_wake.notify_one();
                    let _ = events.send(UiEvent::Status(format!(
                        "The queue can now run up to {limit} downloads at once."
                    )));
                }
                Err(error) => {
                    error!(%error, "save maximum concurrent downloads");
                    if generation_state.load(Ordering::Acquire) == generation {
                        let previous_value = previous_limit.to_string().into();
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(window) = weak_window.upgrade() {
                                window.set_max_concurrent_downloads(previous_value);
                            }
                        });
                    }
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not save the parallel download limit: {error}"
                    )));
                }
            }
        });
    });

    let fragments_store = store.clone();
    let fragments_events = events_tx.clone();
    let fragments_handle = handle.clone();
    let fragments_generation = Arc::new(AtomicU64::new(0));
    let fragments_write_lock = Arc::new(AsyncMutex::new(()));
    let fragments_window = window.as_weak();
    window.on_concurrent_fragments_requested(move |value| {
        let Ok(count) = value.parse::<usize>() else {
            return;
        };
        if !(1..=8).contains(&count) {
            return;
        }
        let store = fragments_store.clone();
        let events = fragments_events.clone();
        let generation_state = fragments_generation.clone();
        let generation = generation_state.fetch_add(1, Ordering::AcqRel) + 1;
        let write_lock = fragments_write_lock.clone();
        let weak_window = fragments_window.clone();
        fragments_handle.spawn(async move {
            let _write_guard = write_lock.lock().await;
            if generation_state.load(Ordering::Acquire) != generation {
                return;
            }
            let previous_count =
                store.load_concurrent_fragments().await.unwrap_or(DEFAULT_CONCURRENT_FRAGMENTS);
            if generation_state.load(Ordering::Acquire) != generation {
                return;
            }
            match store.set_concurrent_fragments(count, now()).await {
                Ok(()) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "New downloads can now use up to {count} fragments per download."
                    )));
                }
                Err(error) => {
                    error!(%error, "save concurrent fragments setting");
                    if generation_state.load(Ordering::Acquire) == generation {
                        let previous_value = previous_count.to_string().into();
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(window) = weak_window.upgrade() {
                                window.set_concurrent_fragments(previous_value);
                            }
                        });
                    }
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not save the fragment concurrency limit: {error}"
                    )));
                }
            }
        });
    });

    let retry_settings_store = store.clone();
    let retry_settings_events = events_tx.clone();
    let retry_settings_wake = queue_wake.clone();
    let retry_settings_handle = handle.clone();
    let retry_settings_generation = Arc::new(AtomicU64::new(0));
    let retry_settings_write_lock = Arc::new(AsyncMutex::new(()));
    let retry_settings_window = window.as_weak();
    window.on_auto_retry_attempts_requested(move |value| {
        let count = if value.as_str() == "Off" { Some(0) } else { value.parse::<usize>().ok() };
        let Some(count) = count.filter(|count| *count <= MAX_AUTO_RETRY_ATTEMPTS) else {
            return;
        };
        let store = retry_settings_store.clone();
        let events = retry_settings_events.clone();
        let queue_wake = retry_settings_wake.clone();
        let generation_state = retry_settings_generation.clone();
        let generation = generation_state.fetch_add(1, Ordering::AcqRel) + 1;
        let write_lock = retry_settings_write_lock.clone();
        let weak_window = retry_settings_window.clone();
        retry_settings_handle.spawn(async move {
            let _write_guard = write_lock.lock().await;
            if generation_state.load(Ordering::Acquire) != generation {
                return;
            }
            let previous_count =
                store.load_auto_retry_attempts().await.unwrap_or(DEFAULT_AUTO_RETRY_ATTEMPTS);
            if generation_state.load(Ordering::Acquire) != generation {
                return;
            }
            match store.set_auto_retry_attempts(count, now()).await {
                Ok(canceled_waits) => {
                    queue_wake.notify_one();
                    let setting_label =
                        if count == 0 { "off".to_owned() } else { count.to_string() };
                    let _ = events.send(UiEvent::Status(format!(
                        "Automatic transient-error retries set to {setting_label}; canceled {canceled_waits} scheduled retry(s)."
                    )));
                }
                Err(error) => {
                    error!(%error, "save automatic retry setting");
                    if generation_state.load(Ordering::Acquire) == generation {
                        let previous_value = if previous_count == 0 {
                            "Off".to_owned()
                        } else {
                            previous_count.to_string()
                        };
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(window) = weak_window.upgrade() {
                                window.set_auto_retry_attempts(previous_value.into());
                            }
                        });
                    }
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not save the automatic retry limit: {error}"
                    )));
                }
            }
        });
    });

    let pause_store = store.clone();
    let pause_events = events_tx.clone();
    let pause_handle = handle.clone();
    let pause_active_jobs = active_jobs.clone();
    window.on_pause_download_requested(move |id| {
        let id = id.to_string();
        let store = pause_store.clone();
        let events = pause_events.clone();
        let active_jobs = pause_active_jobs.clone();
        pause_handle.spawn(async move {
            match store.pause_queued_download(&id, now()).await {
                Ok(true) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status("Paused the queued item.".to_owned()));
                }
                Ok(false) => {
                    let control = active_jobs.lock().ok().and_then(|jobs| jobs.get(&id).cloned());
                    let Some(control) = control else {
                        let _ = events.send(UiEvent::Refresh);
                        let _ = events.send(UiEvent::Status(
                            "This item is no longer queued or active.".to_owned(),
                        ));
                        return;
                    };
                    match store.request_active_pause(&id, now()).await {
                        Ok(true) => {
                            control.send_replace(JobSignal::Pause);
                            let _ = events.send(UiEvent::Refresh);
                            let _ = events.send(UiEvent::Status(
                                "Pausing the active item and keeping partial files.".to_owned(),
                            ));
                        }
                        Ok(false) => {
                            let _ = events.send(UiEvent::Refresh);
                            let _ = events.send(UiEvent::Status(
                                "This item has already stopped or is being changed.".to_owned(),
                            ));
                        }
                        Err(error) => {
                            error!(%error, job_id = %id, "request active download pause");
                            let _ = events.send(UiEvent::Status(format!(
                                "Could not pause that download: {error}"
                            )));
                        }
                    }
                }
                Err(error) => {
                    error!(%error, job_id = %id, "pause queued download");
                    let _ = events
                        .send(UiEvent::Status(format!("Could not pause that download: {error}")));
                }
            }
        });
    });

    let folder_events = events_tx.clone();
    let folder_window = window.as_weak();
    let folder_handle = handle.clone();
    window.on_open_download_folder(move || {
        let Some(window) = folder_window.upgrade() else { return };
        let folder = PathBuf::from(window.get_download_directory().as_str());
        let events = folder_events.clone();
        folder_handle.spawn_blocking(move || {
            if let Err(error) = open_folder(&folder) {
                let _ = events
                    .send(UiEvent::Status(format!("Could not open download folder: {error}")));
            }
        });
    });

    let choose_store = store.clone();
    let choose_events = events_tx.clone();
    let choose_window = window.as_weak();
    let choose_handle = handle.clone();
    window.on_choose_download_directory_requested(move || {
        let store = choose_store.clone();
        let events = choose_events.clone();
        let weak_window = choose_window.clone();
        let handle = choose_handle.clone();
        let _ = events.send(UiEvent::Status("Choose an existing save folder…".to_owned()));
        handle.spawn(async move {
            let selected = match tokio::task::spawn_blocking(pick_download_directory).await {
                Ok(Ok(Some(path))) => path,
                Ok(Ok(None)) => {
                    let _ = events.send(UiEvent::Status("Folder selection canceled.".to_owned()));
                    return;
                }
                Ok(Err(error)) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not open the folder picker: {error}"
                    )));
                    return;
                }
                Err(error) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "The folder picker did not complete: {error}"
                    )));
                    return;
                }
            };
            let selected =
                match tokio::task::spawn_blocking(move || validate_download_directory(&selected))
                    .await
                {
                    Ok(Ok(path)) => path,
                    Ok(Err(error)) => {
                        let _ = events
                            .send(UiEvent::Status(format!("Choose an accessible folder: {error}")));
                        return;
                    }
                    Err(error) => {
                        let _ = events.send(UiEvent::Status(format!(
                            "Could not validate the selected folder: {error}"
                        )));
                        return;
                    }
                };
            let directory = selected.to_string_lossy().into_owned();
            if let Err(error) = store.set_default_download_directory(directory.clone(), now()).await
            {
                let _ = events
                    .send(UiEvent::Status(format!("Could not save the default folder: {error}")));
                return;
            }
            let weak = weak_window.clone();
            let _ = slint::invoke_from_event_loop(move || {
                if let Some(window) = weak.upgrade() {
                    window.set_download_directory(directory.into());
                }
            });
            let _ = events.send(UiEvent::Status(
                "Default save folder updated. Existing queue items keep their saved folder."
                    .to_owned(),
            ));
        });
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

    let export_store = store.clone();
    let export_events = events_tx.clone();
    let export_handle = handle.clone();
    window.on_export_history_requested(move || {
        let store = export_store.clone();
        let events = export_events.clone();
        let _ = events.send(UiEvent::Status("Choose a CSV file for download history…".to_owned()));
        export_handle.spawn(async move {
            let path = match tokio::task::spawn_blocking(pick_history_export_path).await {
                Ok(Ok(Some(path))) => path,
                Ok(Ok(None)) => {
                    let _ = events.send(UiEvent::Status("History export canceled.".to_owned()));
                    return;
                }
                Ok(Err(error)) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not open the CSV save dialog: {error}"
                    )));
                    return;
                }
                Err(error) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "The CSV save dialog did not complete: {error}"
                    )));
                    return;
                }
            };
            let records = match store.list_downloads().await {
                Ok(records) => records,
                Err(error) => {
                    error!(%error, "load history for CSV export");
                    let _ = events
                        .send(UiEvent::Status(format!("Could not load download history: {error}")));
                    return;
                }
            };
            let count = records.len();
            let csv = download_history_csv(&records);
            let write = tokio::task::spawn_blocking(move || {
                std::fs::write(&path, csv)
                    .with_context(|| format!("write download history to {}", path.display()))?;
                Ok::<_, anyhow::Error>(path)
            })
            .await;
            match write {
                Ok(Ok(path)) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "Exported {count} history {} to {}.",
                        if count == 1 { "entry" } else { "entries" },
                        path.display()
                    )));
                }
                Ok(Err(error)) => {
                    error!(%error, "write download history CSV");
                    let _ = events
                        .send(UiEvent::Status(format!("Could not write the history CSV: {error}")));
                }
                Err(error) => {
                    error!(%error, "join download history CSV writer");
                    let _ = events.send(UiEvent::Status(format!(
                        "The history CSV writer did not complete: {error}"
                    )));
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
                            cancel_sender.send_replace(JobSignal::Cancel);
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

    let reveal_events = events_tx.clone();
    let reveal_handle = handle.clone();
    window.on_reveal_output_requested(move |output_path| {
        let path = PathBuf::from(output_path.as_str());
        let events = reveal_events.clone();
        reveal_handle.spawn_blocking(move || {
            if let Err(error) = reveal_file(&path) {
                let _ = events.send(UiEvent::Status(format!(
                    "Could not show the downloaded file in its folder: {error}"
                )));
            }
        });
    });

    let locate_store = store.clone();
    let locate_events = events_tx.clone();
    let locate_handle = handle.clone();
    window.on_locate_library_file_requested(move |id| {
        let id = id.to_string();
        let store = locate_store.clone();
        let events = locate_events.clone();
        let handle = locate_handle.clone();
        let _ =
            events.send(UiEvent::Status("Choose the moved media file to relink it.".to_owned()));
        handle.spawn(async move {
            let selected = match tokio::task::spawn_blocking(pick_library_media_file).await {
                Ok(Ok(Some(path))) => path,
                Ok(Ok(None)) => {
                    let _ = events.send(UiEvent::Status("Library relink canceled.".to_owned()));
                    return;
                }
                Ok(Err(error)) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not open the media file picker: {error}"
                    )));
                    return;
                }
                Err(error) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "The media file picker did not complete: {error}"
                    )));
                    return;
                }
            };
            if !has_supported_media_extension(&selected) {
                let _ = events
                    .send(UiEvent::Status("Choose a supported audio or video file.".to_owned()));
                return;
            }
            let canonical = match tokio::task::spawn_blocking(move || {
                validate_output_file(&selected)?;
                std::fs::canonicalize(&selected)
                    .with_context(|| format!("resolve moved media file {}", selected.display()))
            })
            .await
            {
                Ok(Ok(path)) => path,
                Ok(Err(error)) => {
                    let _ = events
                        .send(UiEvent::Status(format!("Choose an existing media file: {error}")));
                    return;
                }
                Err(error) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not validate the selected media file: {error}"
                    )));
                    return;
                }
            };
            match store
                .update_completed_output_path(&id, canonical.to_string_lossy().into_owned())
                .await
            {
                Ok(true) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(
                        "Updated the library path. The selected file was left in place.".to_owned(),
                    ));
                }
                Ok(false) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(
                        "This item is no longer a completed download.".to_owned(),
                    ));
                }
                Err(error) => {
                    error!(%error, job_id = %id, "relink moved library file");
                    let _ = events
                        .send(UiEvent::Status(format!("Could not relink the media file: {error}")));
                }
            }
        });
    });

    configure_clipboard_popup(
        &popup,
        ClipboardPopupContext {
            main_window: window.as_weak(),
            popup_window: popup.as_weak(),
            popup_generation: clipboard_popup_generation.clone(),
            shutdown_started: shutdown_started.clone(),
        },
    );

    configure_tray(
        &window,
        &tray,
        TrayContext {
            store: store.clone(),
            handle: handle.clone(),
            events: events_tx.clone(),
            active_jobs: active_jobs.clone(),
            queue_wake: queue_wake.clone(),
            preferences: tray_preferences_state.clone(),
            shutdown_started,
        },
    );

    info!(product = env!("YT_SAVE_PRODUCT_NAME"), "application started");
    window.run().context("run Slint event loop")?;
    let _ = stop_clipboard_monitor(&clipboard_monitor_slot, &clipboard_monitor_generation);
    runtime.shutdown_timeout(Duration::from_secs(3));
    Ok(())
}

#[derive(Clone)]
struct ClipboardPopupContext {
    main_window: slint::Weak<MainWindow>,
    popup_window: slint::Weak<ClipboardPopup>,
    popup_generation: Arc<AtomicU64>,
    shutdown_started: Arc<AtomicBool>,
}

fn configure_clipboard_popup(popup: &ClipboardPopup, context: ClipboardPopupContext) {
    let analyze_context = context.clone();
    popup.on_analyze_requested(move |url| {
        dismiss_clipboard_popup(&analyze_context);
        if let Some(window) = analyze_context.main_window.upgrade() {
            let url = url.to_string();
            window.set_url_input(url.clone().into());
            window.set_status_message("Reading media details…".into());
            if let Err(error) = window.show() {
                error!(%error, "show window to analyze copied link");
                return;
            }
            window.invoke_analyze_requested(url.into());
        }
    });

    let video_context = context.clone();
    popup.on_add_video_requested(move |url| {
        queue_clipboard_candidate(url.to_string(), false, video_context.clone());
    });

    let audio_context = context.clone();
    popup.on_add_audio_requested(move |url| {
        queue_clipboard_candidate(url.to_string(), true, audio_context.clone());
    });

    let options_context = context.clone();
    popup.on_more_options_requested(move |url| {
        dismiss_clipboard_popup(&options_context);
        if let Some(window) = options_context.main_window.upgrade() {
            window.set_url_input(url);
            if let Err(error) = window.show() {
                error!(%error, "show window for copied-link options");
            }
        }
    });

    popup.on_dismiss_requested(move || dismiss_clipboard_popup(&context));
}

fn dismiss_clipboard_popup(context: &ClipboardPopupContext) {
    context.popup_generation.fetch_add(1, Ordering::Relaxed);
    if let Some(window) = context.main_window.upgrade() {
        window.set_clipboard_candidate(SharedString::default());
    }
    if let Some(popup) = context.popup_window.upgrade()
        && let Err(error) = popup.hide()
    {
        error!(%error, "hide clipboard review popup");
    }
}

fn queue_clipboard_candidate(url: String, audio_only: bool, context: ClipboardPopupContext) {
    dismiss_clipboard_popup(&context);
    if context.shutdown_started.load(Ordering::Acquire) {
        if let Some(window) = context.main_window.upgrade() {
            window.set_status_message("The application is shutting down.".into());
        }
        return;
    }
    let url = match validate_url(&url) {
        Ok(url) => url,
        Err(message) => {
            if let Some(window) = context.main_window.upgrade() {
                window.set_status_message(message.into());
            }
            return;
        }
    };

    if let Some(window) = context.main_window.upgrade() {
        window.set_url_input(url.clone().into());
        window.set_audio_only(audio_only);
        window.set_status_message("Checking the copied link before adding it to the queue…".into());
        if let Err(error) = window.show() {
            error!(%error, "show window to add copied link");
            return;
        }
        window.invoke_enqueue_requested(url.into());
    }
}

fn clipboard_source_host(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_else(|| "Web media".to_owned())
}

fn initial_popup_position(
    area: WorkArea,
    scale: f32,
    logical_width: i32,
    logical_height: i32,
) -> (i32, i32) {
    let scale = scale.max(1.0);
    let width = (logical_width as f32 * scale).round() as i32;
    let height = (logical_height as f32 * scale).round() as i32;
    (
        area.right.saturating_sub(width).saturating_sub(16).max(area.left.saturating_add(16)),
        area.bottom.saturating_sub(height).saturating_sub(16).max(area.top.saturating_add(16)),
    )
}

#[cfg(windows)]
fn configure_clipboard_popup_window(
    popup: &ClipboardPopup,
    work_area: Arc<Mutex<Option<WorkArea>>>,
) {
    use slint::winit_030::{EventResult, WinitWindowAccessor, winit};

    let native_style_ready = Arc::new(AtomicBool::new(false));
    popup.window().on_winit_window_event(move |slint_window, event| {
        if matches!(
            event,
            winit::event::WindowEvent::RedrawRequested
                | winit::event::WindowEvent::Resized(_)
                | winit::event::WindowEvent::ScaleFactorChanged { .. }
        ) {
            let work_area = work_area.lock().ok().and_then(|area| *area);
            let native_style_ready = native_style_ready.clone();
            slint_window.with_winit_window(|window| {
                if !native_style_ready.load(Ordering::Acquire) && set_window_no_activate(window) {
                    native_style_ready.store(true, Ordering::Release);
                }

                if let Some(area) = work_area {
                    let size = window.inner_size();
                    let width = i32::try_from(size.width).unwrap_or(i32::MAX);
                    let height = i32::try_from(size.height).unwrap_or(i32::MAX);
                    let x = area
                        .right
                        .saturating_sub(width)
                        .saturating_sub(16)
                        .max(area.left.saturating_add(16));
                    let y = area
                        .bottom
                        .saturating_sub(height)
                        .saturating_sub(16)
                        .max(area.top.saturating_add(16));
                    window.set_outer_position(winit::dpi::PhysicalPosition::new(x, y));
                }
            });
        }

        EventResult::Propagate
    });
}

#[cfg(windows)]
fn main_window_has_focus(window: &MainWindow) -> bool {
    use slint::winit_030::WinitWindowAccessor;
    window.window().with_winit_window(|window| window.has_focus()).unwrap_or(false)
}

#[cfg(windows)]
fn set_window_no_activate(window: &slint::winit_030::winit::window::Window) -> bool {
    use slint::winit_030::winit::raw_window_handle::{HasWindowHandle, RawWindowHandle};
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        GWL_EXSTYLE, GetWindowLongPtrW, SWP_FRAMECHANGED, SWP_NOACTIVATE, SWP_NOMOVE, SWP_NOSIZE,
        SWP_NOZORDER, SetWindowLongPtrW, SetWindowPos, WS_EX_NOACTIVATE, WS_EX_TOOLWINDOW,
    };

    let Ok(handle) = window.window_handle() else {
        return false;
    };
    let RawWindowHandle::Win32(handle) = handle.as_raw() else {
        return false;
    };
    let hwnd = handle.hwnd.get() as *mut std::ffi::c_void;
    unsafe {
        let style = GetWindowLongPtrW(hwnd, GWL_EXSTYLE);
        let required_style = (WS_EX_NOACTIVATE | WS_EX_TOOLWINDOW) as isize;
        SetWindowLongPtrW(hwnd, GWL_EXSTYLE, style | required_style);
        let updated_style = GetWindowLongPtrW(hwnd, GWL_EXSTYLE);
        if updated_style & required_style != required_style {
            return false;
        }
        SetWindowPos(
            hwnd,
            std::ptr::null_mut(),
            0,
            0,
            0,
            0,
            SWP_FRAMECHANGED | SWP_NOACTIVATE | SWP_NOMOVE | SWP_NOSIZE | SWP_NOZORDER,
        ) != 0
    }
}

fn start_clipboard_monitor(
    slot: &Arc<Mutex<Option<ClipboardMonitor>>>,
    generation: &AtomicU64,
    events: mpsc::UnboundedSender<UiEvent>,
) -> Result<()> {
    let mut monitor_slot =
        slot.lock().map_err(|_| anyhow::anyhow!("clipboard monitor state is unavailable"))?;
    if monitor_slot.is_some() {
        return Ok(());
    }

    let (monitor, candidates) = ClipboardMonitor::start()?;
    let generation = generation.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    let dispatcher = std::thread::Builder::new()
        .name("yt-save-clipboard-events".to_owned())
        .spawn(move || {
            while let Ok(candidate) = candidates.recv() {
                if events.send(UiEvent::ClipboardCandidate { generation, candidate }).is_err() {
                    break;
                }
            }
        })
        .context("start clipboard candidate dispatcher")?;
    drop(dispatcher);
    *monitor_slot = Some(monitor);
    Ok(())
}

fn stop_clipboard_monitor(
    slot: &Arc<Mutex<Option<ClipboardMonitor>>>,
    generation: &AtomicU64,
) -> Result<()> {
    let mut monitor_slot =
        slot.lock().map_err(|_| anyhow::anyhow!("clipboard monitor state is unavailable"))?;
    generation.fetch_add(1, Ordering::Relaxed);
    let monitor = monitor_slot.take();
    drop(monitor_slot);
    drop(monitor);
    Ok(())
}

fn configure_tray(window: &MainWindow, tray: &AppTray, context: TrayContext) {
    let TrayContext {
        store,
        handle,
        events,
        active_jobs,
        queue_wake,
        preferences,
        shutdown_started,
    } = context;
    let open_window = window.as_weak();
    tray.on_open_requested(move || {
        if let Some(window) = open_window.upgrade() {
            window.window().set_minimized(false);
            if let Err(error) = window.show() {
                error!(%error, "show window from system tray");
            }
        }
    });

    let hide_window = window.as_weak();
    tray.on_hide_requested(move || {
        if let Some(window) = hide_window.upgrade()
            && let Err(error) = window.hide()
        {
            error!(%error, "hide window to system tray");
        }
    });

    let open_folder_window = window.as_weak();
    tray.on_open_folder_requested(move || {
        if let Some(window) = open_folder_window.upgrade() {
            window.invoke_open_download_folder();
        }
    });

    let pause_store = store.clone();
    let pause_active_jobs = active_jobs.clone();
    let pause_events = events.clone();
    let pause_handle = handle.clone();
    let pause_shutdown_guard = shutdown_started.clone();
    tray.on_pause_all_requested(move || {
        if pause_shutdown_guard.load(Ordering::Acquire) {
            return;
        }
        let store = pause_store.clone();
        let active_jobs = pause_active_jobs.clone();
        let events = pause_events.clone();
        pause_handle.spawn(async move {
            match pause_all_downloads(&store, &active_jobs, true).await {
                Ok((pending_count, active_count)) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(format!(
                        "Paused {pending_count} pending item(s); requested a safe pause for {active_count} active item(s)."
                    )));
                }
                Err(error) => {
                    error!(%error, "pause all downloads from system tray");
                    let _ = events.send(UiEvent::Status(format!("Could not pause all downloads: {error}")));
                }
            }
        });
    });

    let resume_store = store.clone();
    let resume_events = events.clone();
    let resume_wake = queue_wake.clone();
    let resume_handle = handle.clone();
    let resume_shutdown_guard = shutdown_started.clone();
    tray.on_resume_all_requested(move || {
        if resume_shutdown_guard.load(Ordering::Acquire) {
            return;
        }
        let store = resume_store.clone();
        let events = resume_events.clone();
        let queue_wake = resume_wake.clone();
        resume_handle.spawn(async move {
            match resume_all_downloads(store).await {
                Ok(count) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ =
                        events.send(UiEvent::Status(format!("Resumed {count} paused item(s).")));
                    if count > 0 {
                        queue_wake.notify_one();
                    }
                }
                Err(error) => {
                    error!(%error, "resume all downloads from system tray");
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not resume paused downloads: {error}"
                    )));
                }
            }
        });
    });

    let quit_store = store.clone();
    let quit_handle = handle.clone();
    let quit_events = events.clone();
    let quit_active_jobs = active_jobs.clone();
    let quit_queue_wake = queue_wake.clone();
    let quit_window = window.as_weak();
    let quit_guard = shutdown_started.clone();
    tray.on_quit_requested(move || {
        begin_orderly_quit(
            quit_handle.clone(),
            quit_store.clone(),
            quit_active_jobs.clone(),
            quit_queue_wake.clone(),
            quit_events.clone(),
            quit_window.clone(),
            quit_guard.clone(),
        );
    });

    let preferences_state = preferences.clone();
    let preference_write_lock = Arc::new(AsyncMutex::new(()));
    let close_preference_write_lock = preference_write_lock.clone();
    let preference_store = store.clone();
    let preference_handle = handle.clone();
    let preference_events = events.clone();
    let preference_window = window.as_weak();
    window.on_close_to_tray_requested(move |enabled| {
        if let Some(window) = preference_window.upgrade() {
            window.set_close_to_tray_enabled(enabled);
        }
        save_tray_preference_update(
            &preferences_state,
            Some(enabled),
            None,
            close_preference_write_lock.clone(),
            preference_store.clone(),
            preference_handle.clone(),
            preference_events.clone(),
        );
    });

    let preferences_state = preferences.clone();
    let minimize_preference_write_lock = preference_write_lock.clone();
    let preference_store = store.clone();
    let preference_handle = handle.clone();
    let preference_events = events.clone();
    let preference_window = window.as_weak();
    window.on_minimize_to_tray_requested(move |enabled| {
        if let Some(window) = preference_window.upgrade() {
            window.set_minimize_to_tray_enabled(enabled);
        }
        save_tray_preference_update(
            &preferences_state,
            None,
            Some(enabled),
            minimize_preference_write_lock.clone(),
            preference_store.clone(),
            preference_handle.clone(),
            preference_events.clone(),
        );
    });

    let close_preferences = preferences.clone();
    let close_store = store;
    let close_handle = handle;
    let close_events = events;
    let close_active_jobs = active_jobs;
    let close_queue_wake = queue_wake;
    let close_window = window.as_weak();
    let close_guard = shutdown_started;
    window.window().on_close_requested(move || {
        let close_to_tray =
            close_preferences.lock().map(|preferences| preferences.close_to_tray).unwrap_or(false);
        if close_to_tray {
            CloseRequestResponse::HideWindow
        } else {
            begin_orderly_quit(
                close_handle.clone(),
                close_store.clone(),
                close_active_jobs.clone(),
                close_queue_wake.clone(),
                close_events.clone(),
                close_window.clone(),
                close_guard.clone(),
            );
            CloseRequestResponse::KeepWindowShown
        }
    });
}

fn save_tray_preference_update(
    state: &Arc<Mutex<TrayPreferences>>,
    close_to_tray: Option<bool>,
    minimize_to_tray: Option<bool>,
    write_lock: Arc<AsyncMutex<()>>,
    store: Store,
    handle: tokio::runtime::Handle,
    events: mpsc::UnboundedSender<UiEvent>,
) {
    match state.lock() {
        Ok(mut preferences) => {
            if let Some(enabled) = close_to_tray {
                preferences.close_to_tray = enabled;
            }
            if let Some(enabled) = minimize_to_tray {
                preferences.minimize_to_tray = enabled;
            }
        }
        Err(_) => {
            let _ = events.send(UiEvent::Status(
                "Could not update tray behavior because settings state is unavailable.".to_owned(),
            ));
            return;
        }
    };

    let state = state.clone();
    handle.spawn(async move {
        let _write_guard = write_lock.lock().await;
        let preferences = match state.lock() {
            Ok(preferences) => *preferences,
            Err(_) => {
                let _ = events.send(UiEvent::Status(
                    "Tray behavior changed for this session but could not be saved because settings state is unavailable.".to_owned(),
                ));
                return;
            }
        };
        if let Err(error) = store.set_tray_preferences(preferences, now()).await {
            error!(%error, "save tray behavior preferences");
            let _ = events.send(UiEvent::Status(format!(
                "Tray behavior changed for this session but could not be saved: {error}"
            )));
        }
    });
}

fn begin_orderly_quit(
    handle: tokio::runtime::Handle,
    store: Store,
    active_jobs: ActiveJobs,
    queue_wake: Arc<Notify>,
    events: mpsc::UnboundedSender<UiEvent>,
    window: slint::Weak<MainWindow>,
    shutdown_started: Arc<AtomicBool>,
) {
    if shutdown_started.swap(true, Ordering::AcqRel) {
        return;
    }
    queue_wake.notify_one();
    if let Some(window) = window.upgrade() {
        window.set_status_message(
            "Pausing queue work safely before quitting. Partial files are being preserved.".into(),
        );
    }

    handle.spawn(async move {
        let shutdown_result = async {
            pause_all_downloads(&store, &active_jobs, false).await?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                let active_count = active_jobs
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active download state is unavailable"))?
                    .len();
                if active_count == 0 {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    anyhow::bail!(
                        "active jobs did not stop within 30 seconds; the app is still open"
                    );
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        match shutdown_result {
            Ok(()) => {
                let _ = events.send(UiEvent::Status(
                    "Downloads are paused. The application is shutting down.".to_owned(),
                ));
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = window.upgrade()
                        && let Err(error) = window.hide()
                    {
                        error!(%error, "hide window during orderly shutdown");
                    }
                    if let Err(error) = slint::quit_event_loop() {
                        shutdown_started.store(false, Ordering::Release);
                        error!(%error, "quit Slint event loop during orderly shutdown");
                        if let Some(window) = window.upgrade() {
                            window.set_status_message(
                                format!("Could not finish shutting down: {error}").into(),
                            );
                            let _ = window.show();
                        }
                    }
                });
            }
            Err(error) => {
                shutdown_started.store(false, Ordering::Release);
                error!(%error, "orderly application shutdown");
                let message = format!(
                    "Could not safely quit: {error}. The window and tray remain available."
                );
                let _ = events.send(UiEvent::Status(message.clone()));
                if let Err(invoke_error) = slint::invoke_from_event_loop(move || {
                    if let Some(window) = window.upgrade() {
                        window.window().set_minimized(false);
                        window.set_status_message(message.into());
                        if let Err(show_error) = window.show() {
                            error!(%show_error, "show window after orderly shutdown failure");
                        }
                    }
                }) {
                    error!(%invoke_error, "restore window after orderly shutdown failure");
                }
            }
        }
    });
}

async fn pause_all_downloads(
    store: &Store,
    active_jobs: &ActiveJobs,
    pause_retry_waits: bool,
) -> Result<(usize, usize)> {
    let records = store.list_downloads().await?;
    let mut pending_count = 0;
    let mut active_count = 0;
    for record in records {
        if (record.status == "Queued"
            || (pause_retry_waits && record.status == "Waiting for retry"))
            && store.pause_queued_download(&record.id, now()).await?
        {
            pending_count += 1;
            continue;
        }
        if matches!(
            record.status.as_str(),
            "Queued"
                | "Waiting for retry"
                | "Analyzing"
                | "Downloading"
                | "Post-processing"
                | "Pausing"
                | "Canceling"
        ) && request_pause_for_active_record(store, active_jobs, &record).await?
        {
            active_count += 1;
        }
    }
    Ok((pending_count, active_count))
}

async fn request_pause_for_active_record(
    store: &Store,
    active_jobs: &ActiveJobs,
    record: &DownloadRecord,
) -> Result<bool> {
    let control = active_jobs
        .lock()
        .map_err(|_| anyhow::anyhow!("active download state is unavailable"))?
        .get(&record.id)
        .cloned();
    let Some(control) = control else {
        return Ok(false);
    };

    match record.status.as_str() {
        "Pausing" => {
            control.send_replace(JobSignal::Pause);
            return Ok(true);
        }
        "Canceling" => {
            control.send_replace(JobSignal::Cancel);
            return Ok(true);
        }
        _ => {}
    }

    if store.request_active_pause(&record.id, now()).await? {
        control.send_replace(JobSignal::Pause);
        return Ok(true);
    }
    if store.is_pausing(&record.id).await? {
        control.send_replace(JobSignal::Pause);
        return Ok(true);
    }
    if store.is_canceling(&record.id).await? {
        control.send_replace(JobSignal::Cancel);
        return Ok(true);
    }
    Ok(false)
}

async fn resume_all_downloads(store: Store) -> Result<usize> {
    let records = store.list_downloads().await?;
    let mut resumed_count = 0;
    for record in records.into_iter().filter(|record| record.status == "Paused") {
        if store.resume_download(&record.id, now()).await?.is_some() {
            resumed_count += 1;
        }
    }
    Ok(resumed_count)
}

fn is_active_status(status: &str) -> bool {
    matches!(status, "Analyzing" | "Downloading" | "Post-processing" | "Pausing" | "Canceling")
}

fn can_dispatch_more(active_workers: usize, max_concurrent_downloads: usize) -> bool {
    active_workers < max_concurrent_downloads
}

fn tray_tooltip(active_count: usize, paused_count: usize) -> String {
    if active_count == 0 && paused_count == 0 {
        "Yt Save".to_owned()
    } else {
        format!("Yt Save — {active_count} active, {paused_count} paused")
    }
}

async fn recover_pending(store: &Store) -> Result<Vec<DownloadRecord>> {
    let mut pending = Vec::new();
    for mut record in store.list_downloads().await? {
        if matches!(record.status.as_str(), "Canceling" | "Pausing") {
            let operation = if record.status == "Pausing" { "pause" } else { "cancellation" };
            record.status = "Failed".to_owned();
            record.error = Some(format!(
                "The app closed before it could confirm process-tree {operation}. Partial files were kept; check that no media process is still using them before retrying."
            ));
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
    pending.sort_by(|left, right| {
        right.priority.cmp(&left.priority).then_with(|| left.queue_order.cmp(&right.queue_order))
    });
    Ok(pending)
}

async fn run_queue_dispatcher(
    store: Store,
    engine: DownloadEngine,
    events: mpsc::UnboundedSender<UiEvent>,
    queue_wake: Arc<Notify>,
    active_jobs: ActiveJobs,
    shutdown_started: Arc<AtomicBool>,
) {
    let mut workers = JoinSet::new();
    loop {
        if shutdown_started.load(Ordering::Acquire) {
            while let Some(result) = workers.join_next().await {
                if let Err(error) = result {
                    error!(%error, "queue worker task failed while shutting down");
                }
            }
            if shutdown_started.load(Ordering::Acquire) {
                return;
            }
            continue;
        }

        let notified = queue_wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut retry_after_error = false;
        match store.promote_due_retries(now()).await {
            Ok(count) if count > 0 => {
                let _ = events.send(UiEvent::Refresh);
            }
            Ok(_) => {}
            Err(error) => {
                error!(%error, "promote due automatic retries");
                retry_after_error = true;
            }
        }
        loop {
            if shutdown_started.load(Ordering::Acquire) {
                break;
            }
            let limit = match store.load_max_concurrent_downloads().await {
                Ok(limit) => limit,
                Err(error) => {
                    error!(%error, "read maximum concurrent downloads; using one worker");
                    retry_after_error = true;
                    1
                }
            };
            if !can_dispatch_more(workers.len(), limit) {
                break;
            }
            let pending = match store.next_queued_download().await {
                Ok(Some(pending)) => pending,
                Ok(None) => break,
                Err(error) => {
                    error!(%error, "read next queue item");
                    retry_after_error = true;
                    break;
                }
            };
            let (signal_sender, signal_receiver) = watch::channel(JobSignal::Continue);
            let signal_sender = Arc::new(signal_sender);
            match active_jobs.lock() {
                Ok(mut jobs) => {
                    jobs.insert(pending.id.clone(), signal_sender.clone());
                }
                Err(_) => {
                    let _ = events.send(UiEvent::Status(
                        "Could not register controls for this queue item.".to_owned(),
                    ));
                    retry_after_error = true;
                    break;
                }
            }
            let registration = ActiveJobRegistration {
                id: pending.id.clone(),
                jobs: active_jobs.clone(),
                sender: signal_sender,
            };
            if shutdown_started.load(Ordering::Acquire) {
                drop(registration);
                break;
            }
            let active = match store.claim_queued_download_if_next(&pending.id, now()).await {
                Ok(Some(record)) => record,
                Ok(None) => {
                    drop(registration);
                    let _ = events.send(UiEvent::Refresh);
                    continue;
                }
                Err(error) => {
                    drop(registration);
                    error!(%error, job_id = %pending.id, "claim queued download");
                    let _ = events
                        .send(UiEvent::Status(format!("Could not start queued item: {error}")));
                    retry_after_error = true;
                    break;
                }
            };
            let job_store = store.clone();
            let job_engine = engine.clone();
            let job_events = events.clone();
            let job_wake = queue_wake.clone();
            workers.spawn(async move {
                run_download_job(
                    active,
                    job_store,
                    job_engine,
                    job_events,
                    signal_receiver,
                    registration,
                )
                .await;
                job_wake.notify_one();
            });
        }

        if shutdown_started.load(Ordering::Acquire) {
            continue;
        }
        let retry_wait = match store.next_retry_at().await {
            Ok(Some(deadline)) => Some(Duration::from_secs(
                u64::try_from(deadline.saturating_sub(now()).max(0)).unwrap_or_default(),
            )),
            Ok(None) => None,
            Err(error) => {
                error!(%error, "read next automatic retry deadline");
                retry_after_error = true;
                None
            }
        };
        let wait_duration = if retry_after_error {
            Some(
                retry_wait
                    .map_or(Duration::from_secs(5), |delay| delay.min(Duration::from_secs(5))),
            )
        } else {
            retry_wait
        };
        if workers.is_empty() {
            if let Some(delay) = wait_duration {
                tokio::select! {
                    _ = notified.as_mut() => {},
                    _ = tokio::time::sleep(delay) => {},
                }
            } else {
                notified.await;
            }
        } else if let Some(delay) = wait_duration {
            tokio::select! {
                _ = notified.as_mut() => {},
                result = workers.join_next() => {
                    if let Some(Err(error)) = result {
                        error!(%error, "queue worker task failed");
                    }
                },
                _ = tokio::time::sleep(delay) => {},
            }
        } else {
            tokio::select! {
                _ = notified.as_mut() => {},
                result = workers.join_next() => {
                    if let Some(Err(error)) = result {
                        error!(%error, "queue worker task failed");
                    }
                },
            }
        }
    }
}

async fn run_download_job(
    mut active: DownloadRecord,
    store: Store,
    engine: DownloadEngine,
    events: mpsc::UnboundedSender<UiEvent>,
    mut signal_receiver: watch::Receiver<JobSignal>,
    _registration: ActiveJobRegistration,
) {
    let _ = events.send(UiEvent::Refresh);

    match engine
        .inspect_with_playlist_index_cancellable(
            &active.url,
            active.playlist_index,
            &mut signal_receiver,
        )
        .await
    {
        Ok(EngineOutcome::Completed(metadata)) => {
            if *signal_receiver.borrow() != JobSignal::Continue {
                finish_requested_stop(active, store, events).await;
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
                    let pausing = match store.is_pausing(&active.id).await {
                        Ok(pausing) => pausing,
                        Err(error) => {
                            error!(%error, job_id = %active.id, "check pause after rejected phase update");
                            false
                        }
                    };
                    let canceling = match store.is_canceling(&active.id).await {
                        Ok(canceling) => canceling,
                        Err(error) => {
                            error!(%error, job_id = %active.id, "check cancellation after rejected phase update");
                            false
                        }
                    };
                    if *signal_receiver.borrow() != JobSignal::Continue || pausing || canceling {
                        finish_requested_stop(active, store, events).await;
                    }
                    return;
                }
                Err(error) => {
                    error!(%error, job_id = %active.id, "persist download state");
                }
            }
            let _ = events.send(UiEvent::Refresh);
        }
        Ok(EngineOutcome::Paused | EngineOutcome::Canceled) => {
            finish_requested_stop(active, store, events).await;
            return;
        }
        Err(error) => {
            match schedule_automatic_retry(active.clone(), &error, &store).await {
                Ok(Some(message)) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(message));
                    return;
                }
                Ok(None) => {}
                Err(schedule_error) => {
                    error!(%schedule_error, job_id = %active.id, "schedule automatic retry");
                }
            }
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

    let concurrent_fragments = match store.load_concurrent_fragments().await {
        Ok(count) => count,
        Err(error) => {
            error!(%error, job_id = %active.id, "read fragment concurrency setting; using one fragment");
            DEFAULT_CONCURRENT_FRAGMENTS
        }
    };
    let download_result = engine
        .download_with_playlist_index_cancellable(
            &active.url,
            active.playlist_index,
            DownloadOptions::new(active.download_mode, active.audio_format, active.audio_quality)
                .with_video_quality(active.video_quality)
                .with_concurrent_fragments(concurrent_fragments)
                .with_output_directory(PathBuf::from(&active.download_directory)),
            &mut signal_receiver,
            move |progress| {
                let _ = progress_tx.send(progress);
            },
        )
        .await;
    let mut final_record = progress_task.await.unwrap_or_else(|_| active.clone());
    let (downloaded_successfully, canceled, paused, status_message) = match download_result {
        Ok(EngineOutcome::Completed(result)) => {
            final_record.status = "Completed".to_owned();
            final_record.progress = 100.0;
            final_record.output_path = result.output_path;
            final_record.error = None;
            (true, false, false, "Download complete.".to_owned())
        }
        Ok(EngineOutcome::Paused) => {
            final_record.status = "Paused".to_owned();
            final_record.error = None;
            (false, false, true, "Paused the active download. Partial files were kept.".to_owned())
        }
        Ok(EngineOutcome::Canceled) => {
            final_record.status = "Canceled".to_owned();
            final_record.error = Some("Canceled by user; partial files were kept.".to_owned());
            (
                false,
                true,
                false,
                "Canceled the active download. Partial files were kept.".to_owned(),
            )
        }
        Err(error) => {
            match schedule_automatic_retry(final_record.clone(), &error, &store).await {
                Ok(Some(message)) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(message));
                    return;
                }
                Ok(None) => {}
                Err(schedule_error) => {
                    error!(%schedule_error, job_id = %final_record.id, "schedule automatic retry");
                }
            }
            final_record.status = "Failed".to_owned();
            final_record.error = Some(error.to_string());
            (false, false, false, error.to_string())
        }
    };
    final_record.updated_at = now();
    let persisted = if canceled {
        store.finish_canceled_download(final_record.clone()).await
    } else if paused {
        store.finish_paused_download(final_record.clone()).await
    } else if downloaded_successfully {
        store.finish_download(final_record.clone()).await
    } else {
        finish_failed_job(final_record.clone(), &store).await
    };
    match persisted {
        Ok(true) => {
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status(status_message));
        }
        Ok(false) => {
            if canceled || downloaded_successfully || paused {
                finish_requested_stop(final_record, store, events).await;
                return;
            }
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
    if store.fail_stopping_download(record.clone()).await? {
        return Ok(true);
    }
    if store.finish_download(record.clone()).await? {
        return Ok(true);
    }
    store.fail_stopping_download(record).await
}

const AUTO_RETRY_BASE_DELAY_SECS: u64 = 15;
const AUTO_RETRY_MAX_DELAY_SECS: u64 = 4 * 60;

fn automatic_retry_delay_secs(attempts_already_scheduled: u32) -> u64 {
    let multiplier = 1_u64.checked_shl(attempts_already_scheduled.min(63)).unwrap_or(u64::MAX);
    AUTO_RETRY_BASE_DELAY_SECS.saturating_mul(multiplier).min(AUTO_RETRY_MAX_DELAY_SECS)
}

async fn schedule_automatic_retry(
    mut record: DownloadRecord,
    error: &anyhow::Error,
    store: &Store,
) -> Result<Option<String>> {
    if !is_retryable_engine_error(error) {
        return Ok(None);
    }
    let delay_secs = automatic_retry_delay_secs(record.retry_attempts);
    let retry_number = record.retry_attempts.saturating_add(1);
    record.status = "Waiting for retry".to_owned();
    record.error = Some(error.to_string());
    record.updated_at = now();
    let Some(max_attempts) = store
        .schedule_automatic_retry(
            record.clone(),
            record.updated_at.saturating_add(delay_secs as i64),
        )
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(format!(
        "Temporary issue. Retry {retry_number} of {max_attempts} is scheduled in {delay_secs} seconds."
    )))
}

async fn finish_requested_stop(
    record: DownloadRecord,
    store: Store,
    events: mpsc::UnboundedSender<UiEvent>,
) {
    let canceling = match store.is_canceling(&record.id).await {
        Ok(canceling) => canceling,
        Err(error) => {
            error!(%error, job_id = %record.id, "check cancellation before stopping queue item");
            false
        }
    };
    if canceling {
        finish_canceled_job(record, store, events).await;
        return;
    }

    let pausing = match store.is_pausing(&record.id).await {
        Ok(pausing) => pausing,
        Err(error) => {
            error!(%error, job_id = %record.id, "check pause before stopping queue item");
            false
        }
    };
    if pausing {
        finish_paused_job(record, store, events).await;
        return;
    }

    let _ = events.send(UiEvent::Refresh);
    let _ = events.send(UiEvent::Status(
        "The queue item changed before its pause or cancellation could be saved.".to_owned(),
    ));
}

async fn finish_paused_job(
    mut record: DownloadRecord,
    store: Store,
    events: mpsc::UnboundedSender<UiEvent>,
) {
    record.status = "Paused".to_owned();
    record.error = None;
    record.speed_bytes_per_second = None;
    record.eta_seconds = None;
    record.updated_at = now();
    match store.finish_paused_download(record.clone()).await {
        Ok(true) => {
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status(
                "Paused the active download. Partial files were kept.".to_owned(),
            ));
        }
        Ok(false) => {
            let canceling = match store.is_canceling(&record.id).await {
                Ok(canceling) => canceling,
                Err(error) => {
                    error!(%error, job_id = %record.id, "check cancellation after rejected pause transition");
                    false
                }
            };
            if canceling {
                finish_canceled_job(record, store, events).await;
            } else {
                let _ = events.send(UiEvent::Refresh);
                let _ = events.send(UiEvent::Status(
                    "The queue item changed before the paused state could be saved.".to_owned(),
                ));
            }
        }
        Err(error) => {
            error!(%error, job_id = %record.id, "persist paused download state");
            let _ = events.send(UiEvent::Status(
                "The process stopped, but the app could not save the paused state.".to_owned(),
            ));
        }
    }
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

fn row_views(records: &[DownloadRecord]) -> Vec<DownloadRowData> {
    row_views_filtered(records, "")
}

fn row_views_filtered(records: &[DownloadRecord], query: &str) -> Vec<DownloadRowData> {
    let query = query.trim().to_lowercase();
    let mut position = 0usize;
    records
        .iter()
        .filter_map(|record| {
            let queue_position = if record.status == "Queued" {
                position += 1;
                format!("#{}", position)
            } else if record.status == "Waiting for retry" {
                "Retry".to_owned()
            } else if record.status == "Paused" {
                "Paused".to_owned()
            } else {
                String::new()
            };
            let matches_query = query.is_empty()
                || [
                    record.title.as_str(),
                    record.source.as_str(),
                    record.url.as_str(),
                    record.status.as_str(),
                    record.output_path.as_deref().unwrap_or_default(),
                    record.error.as_deref().unwrap_or_default(),
                    record.download_mode.as_database_value(),
                    record.audio_format.as_database_value(),
                    record.audio_quality.as_database_value(),
                    record.video_quality.as_database_value(),
                ]
                .iter()
                .any(|value| value.to_lowercase().contains(&query));
            matches_query.then(|| row_view_with_queue_position(record, &queue_position))
        })
        .collect()
}

#[cfg(test)]
fn library_row_views(
    records: &[DownloadRecord],
    query: &str,
    filter: &str,
    sort: &str,
) -> Vec<LibraryRowData> {
    let cache = scan_library_entries(records);
    library_row_views_from_cache(&cache, query, filter, sort)
}

fn library_cache_signature_for(records: &[DownloadRecord]) -> LibraryCacheSignature {
    records
        .iter()
        .filter(|record| record.status == "Completed")
        .map(|record| {
            (
                record.id.clone(),
                record.url.clone(),
                record.title.clone(),
                record.source.clone(),
                record.output_path.clone(),
                record.download_mode.as_database_value().to_owned(),
                record.total_bytes,
                record.updated_at,
            )
        })
        .collect()
}

fn request_library_scan(
    events: mpsc::UnboundedSender<UiEvent>,
    generation: &mut u64,
    pending_signature: &mut Option<LibraryCacheSignature>,
    records: Vec<DownloadRecord>,
    signature: LibraryCacheSignature,
) {
    *generation = generation.wrapping_add(1);
    let request_generation = *generation;
    *pending_signature = Some(signature.clone());
    tokio::spawn(async move {
        let result = match tokio::task::spawn_blocking(move || scan_library_entries(&records)).await
        {
            Ok(entries) => Ok(entries),
            Err(error) => Err(error.to_string()),
        };
        let _ = events.send(UiEvent::LibraryScanComplete {
            generation: request_generation,
            signature,
            result,
        });
    });
}

fn scan_library_entries(records: &[DownloadRecord]) -> Vec<LibraryCacheEntry> {
    records
        .iter()
        .filter(|record| record.status == "Completed")
        .map(|record| {
            let path = record.output_path.as_deref().unwrap_or_default();
            let metadata = if path.is_empty() {
                None
            } else {
                std::fs::metadata(path).ok().filter(|metadata| metadata.is_file())
            };
            let path_available = metadata.is_some();
            let kind = match record.download_mode {
                DownloadMode::Video => "Video",
                DownloadMode::AudioOnly => "Audio",
            };
            let file_format = Path::new(path)
                .extension()
                .and_then(|extension| extension.to_str())
                .filter(|extension| !extension.is_empty())
                .map(|extension| extension.to_ascii_uppercase())
                .unwrap_or_else(|| "UNKNOWN".to_owned());
            let source = if record.source.trim().is_empty() {
                url::Url::parse(&record.url)
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_owned))
                    .unwrap_or_else(|| "Web media".to_owned())
            } else {
                record.source.clone()
            };
            let actual_size = metadata.as_ref().map(std::fs::Metadata::len);
            let size_for_sort = actual_size.or(record.total_bytes);
            let size_label = match actual_size {
                Some(bytes) => format!("{} on disk", format_bytes(Some(bytes))),
                None => record.total_bytes.map_or_else(
                    || "Size unknown".to_owned(),
                    |bytes| format!("Last known {}", format_bytes(Some(bytes))),
                ),
            };
            let downloaded_at = chrono::DateTime::from_timestamp(record.updated_at, 0)
                .map(|timestamp| timestamp.format("%b %d, %Y").to_string())
                .unwrap_or_else(|| "Unknown date".to_owned());
            let file_status =
                if path_available { "Available" } else { "Unavailable · locate moved file" };
            let searchable_text = [
                record.title.as_str(),
                source.as_str(),
                kind,
                file_format.as_str(),
                path,
                record.url.as_str(),
            ]
            .iter()
            .map(|value| value.to_lowercase())
            .collect::<Vec<_>>()
            .join(" ");
            LibraryCacheEntry {
                row: LibraryRowData {
                    id: SharedString::from(record.id.as_str()),
                    title: SharedString::from(record.title.as_str()),
                    source: SharedString::from(source),
                    kind: SharedString::from(kind),
                    file_format: SharedString::from(file_format),
                    file_path: SharedString::from(path),
                    file_status: SharedString::from(file_status),
                    size_label: SharedString::from(size_label),
                    downloaded_at: SharedString::from(downloaded_at),
                    path_available,
                },
                searchable_text,
                title_sort_key: record.title.to_lowercase(),
                downloaded_at: record.updated_at,
                size_for_sort,
            }
        })
        .collect()
}

fn library_row_views_from_cache(
    cache: &[LibraryCacheEntry],
    query: &str,
    filter: &str,
    sort: &str,
) -> Vec<LibraryRowData> {
    let query = query.trim().to_lowercase();
    let mut rows = cache
        .iter()
        .filter(|entry| match filter {
            "Video" => entry.row.kind.as_str() == "Video",
            "Audio" => entry.row.kind.as_str() == "Audio",
            "Missing files" => !entry.row.path_available,
            _ => true,
        })
        .filter(|entry| query.is_empty() || entry.searchable_text.contains(&query))
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| match sort {
        "Oldest first" => left
            .downloaded_at
            .cmp(&right.downloaded_at)
            .then_with(|| left.row.id.as_str().cmp(right.row.id.as_str())),
        "Title A–Z" => left
            .title_sort_key
            .cmp(&right.title_sort_key)
            .then_with(|| left.row.id.as_str().cmp(right.row.id.as_str())),
        "Largest first" => right
            .size_for_sort
            .cmp(&left.size_for_sort)
            .then_with(|| right.downloaded_at.cmp(&left.downloaded_at))
            .then_with(|| left.row.id.as_str().cmp(right.row.id.as_str())),
        _ => right
            .downloaded_at
            .cmp(&left.downloaded_at)
            .then_with(|| left.row.id.as_str().cmp(right.row.id.as_str())),
    });
    rows.into_iter()
        .map(|entry| LibraryRowData {
            id: entry.row.id.clone(),
            title: entry.row.title.clone(),
            source: entry.row.source.clone(),
            kind: entry.row.kind.clone(),
            file_format: entry.row.file_format.clone(),
            file_path: entry.row.file_path.clone(),
            file_status: entry.row.file_status.clone(),
            size_label: entry.row.size_label.clone(),
            downloaded_at: entry.row.downloaded_at.clone(),
            path_available: entry.row.path_available,
        })
        .collect()
}

fn queue_summary_text(records: &[DownloadRecord]) -> String {
    let pending = records
        .iter()
        .filter(|record| {
            matches!(record.status.as_str(), "Queued" | "Waiting for retry" | "Paused")
        })
        .collect::<Vec<_>>();
    if pending.is_empty() {
        return "No pending work".to_owned();
    }

    let mut known_sizes = 0usize;
    let mut remaining_bytes = 0u64;
    let queued_count = pending.iter().filter(|record| record.status == "Queued").count();
    let retry_count = pending.iter().filter(|record| record.status == "Waiting for retry").count();
    let paused_count = pending.iter().filter(|record| record.status == "Paused").count();
    let mut queue_labels = Vec::new();
    if queued_count > 0 {
        queue_labels.push(format!("{queued_count} queued"));
    }
    if retry_count > 0 {
        queue_labels.push(format!("{retry_count} waiting for retry"));
    }
    if paused_count > 0 {
        queue_labels.push(format!("{paused_count} paused"));
    }
    let queue_label = queue_labels.join(" · ");
    for record in &pending {
        if let Some(total_bytes) = record.total_bytes {
            known_sizes += 1;
            remaining_bytes = remaining_bytes.saturating_add(
                total_bytes.saturating_sub(record.downloaded_bytes.unwrap_or_default()),
            );
        }
    }
    if known_sizes == pending.len() {
        format!("{queue_label} · {} remaining", format_bytes(Some(remaining_bytes)))
    } else if known_sizes == 0 {
        format!("{queue_label} · size unknown")
    } else {
        let unknown_sizes = pending.len() - known_sizes;
        format!(
            "{queue_label} · {} known; {unknown_sizes} size{} unknown",
            format_bytes(Some(remaining_bytes)),
            if unknown_sizes == 1 { "" } else { "s" },
        )
    }
}

fn row_view_with_queue_position(record: &DownloadRecord, queue_position: &str) -> DownloadRowData {
    let details = if record.status == "Waiting for retry" {
        let scheduled = record
            .next_retry_at
            .and_then(|timestamp| chrono::DateTime::from_timestamp(timestamp, 0))
            .map(|timestamp| timestamp.format("%H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "soon".to_owned());
        format!(
            "Retry {} scheduled for {scheduled} · {}",
            record.retry_attempts,
            record.error.as_deref().unwrap_or("temporary network issue"),
        )
    } else {
        match (
            record.status.as_str(),
            record.error.as_deref(),
            record.downloaded_bytes,
            record.total_bytes,
        ) {
            ("Failed" | "Canceled" | "Interrupted", Some(error), _, _) => error.to_owned(),
            (_, _, Some(downloaded), Some(total)) => {
                format!("{} of {}", format_bytes(Some(downloaded)), format_bytes(Some(total)))
            }
            (_, _, Some(downloaded), None) => {
                format!("{} downloaded", format_bytes(Some(downloaded)))
            }
            (_, Some(error), _, _) => error.to_owned(),
            _ => record.url.clone(),
        }
    };
    let selection = match record.download_mode {
        DownloadMode::Video => format!("Video · {}", record.video_quality.label()),
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
        url: SharedString::from(record.url.as_str()),
        status: SharedString::from(record.status.as_str()),
        progress: record.progress,
        detail: SharedString::from(details),
        output_path: SharedString::from(record.output_path.as_deref().unwrap_or_default()),
        queue_position: SharedString::from(queue_position),
        priority: record.priority,
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn validate_download_directory(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() || !path.is_absolute() {
        anyhow::bail!("choose an absolute save-folder path");
    }
    let metadata =
        std::fs::metadata(path).with_context(|| format!("read save folder {}", path.display()))?;
    if !metadata.is_dir() {
        anyhow::bail!("{} is not a folder", path.display());
    }
    std::fs::canonicalize(path).with_context(|| format!("resolve save folder {}", path.display()))
}

#[cfg(windows)]
fn pick_download_directory() -> Result<Option<PathBuf>> {
    use std::{ffi::OsString, os::windows::ffi::OsStringExt};
    use windows_sys::Win32::{
        System::Com::{COINIT_APARTMENTTHREADED, CoInitializeEx, CoTaskMemFree, CoUninitialize},
        UI::Shell::{
            BIF_NEWDIALOGSTYLE, BIF_RETURNONLYFSDIRS, BROWSEINFOW, SHBrowseForFolderW,
            SHGetPathFromIDListW,
        },
    };

    struct ComApartment;
    impl Drop for ComApartment {
        fn drop(&mut self) {
            unsafe { CoUninitialize() };
        }
    }

    let initialized = unsafe { CoInitializeEx(std::ptr::null(), COINIT_APARTMENTTHREADED as u32) };
    if initialized < 0 {
        anyhow::bail!("initialize the Windows folder picker (HRESULT {initialized:#x})");
    }
    let _apartment = ComApartment;
    let title: Vec<u16> =
        "Choose the default download folder".encode_utf16().chain(Some(0)).collect();
    let mut display_name = [0u16; 260];
    let info = BROWSEINFOW {
        hwndOwner: std::ptr::null_mut(),
        pidlRoot: std::ptr::null_mut(),
        pszDisplayName: display_name.as_mut_ptr(),
        lpszTitle: title.as_ptr(),
        ulFlags: BIF_RETURNONLYFSDIRS | BIF_NEWDIALOGSTYLE,
        ..Default::default()
    };
    let item = unsafe { SHBrowseForFolderW(&info) };
    if item.is_null() {
        return Ok(None);
    }
    let mut selected_path = [0u16; 260];
    let resolved = unsafe { SHGetPathFromIDListW(item, selected_path.as_mut_ptr()) } != 0;
    unsafe { CoTaskMemFree(item.cast()) };
    if !resolved {
        anyhow::bail!("Windows could not resolve the selected folder path");
    }
    let length = selected_path.iter().position(|unit| *unit == 0).unwrap_or(selected_path.len());
    if length == 0 {
        return Ok(None);
    }
    Ok(Some(PathBuf::from(OsString::from_wide(&selected_path[..length]))))
}

#[cfg(windows)]
fn pick_history_export_path() -> Result<Option<PathBuf>> {
    use std::{ffi::OsString, os::windows::ffi::OsStringExt};
    use windows_sys::Win32::UI::Controls::Dialogs::{
        CommDlgExtendedError, GetSaveFileNameW, OFN_EXPLORER, OFN_NOCHANGEDIR, OFN_OVERWRITEPROMPT,
        OFN_PATHMUSTEXIST, OPENFILENAMEW,
    };

    let filter: Vec<u16> =
        "CSV files (*.csv)\0*.csv\0All files (*.*)\0*.*\0\0".encode_utf16().collect();
    let title: Vec<u16> = "Export download history".encode_utf16().chain(Some(0)).collect();
    let default_extension: Vec<u16> = "csv".encode_utf16().chain(Some(0)).collect();
    let mut file: Vec<u16> = "yt-save-history.csv".encode_utf16().chain(Some(0)).collect();
    file.resize(32_768, 0);
    let mut dialog = OPENFILENAMEW {
        lStructSize: std::mem::size_of::<OPENFILENAMEW>() as u32,
        lpstrFilter: filter.as_ptr(),
        nFilterIndex: 1,
        lpstrFile: file.as_mut_ptr(),
        nMaxFile: file.len() as u32,
        lpstrTitle: title.as_ptr(),
        lpstrDefExt: default_extension.as_ptr(),
        Flags: OFN_EXPLORER | OFN_OVERWRITEPROMPT | OFN_PATHMUSTEXIST | OFN_NOCHANGEDIR,
        ..Default::default()
    };
    if unsafe { GetSaveFileNameW(&mut dialog) } == 0 {
        let error = unsafe { CommDlgExtendedError() };
        if error == 0 {
            return Ok(None);
        }
        anyhow::bail!("Windows save dialog error {error:#x}");
    }
    let length = file.iter().position(|unit| *unit == 0).unwrap_or(file.len());
    if length == 0 {
        return Ok(None);
    }
    let mut path = PathBuf::from(OsString::from_wide(&file[..length]));
    if path.extension().is_none() {
        path.set_extension("csv");
    }
    Ok(Some(path))
}

#[cfg(windows)]
fn pick_library_media_file() -> Result<Option<PathBuf>> {
    use std::{ffi::OsString, os::windows::ffi::OsStringExt};
    use windows_sys::Win32::UI::Controls::Dialogs::{
        CommDlgExtendedError, GetOpenFileNameW, OFN_EXPLORER, OFN_FILEMUSTEXIST, OFN_NOCHANGEDIR,
        OFN_PATHMUSTEXIST, OPENFILENAMEW,
    };

    let filter: Vec<u16> = concat!(
        "Media files\0",
        "*.3g2;*.3gp;*.aac;*.ac3;*.aif;*.aiff;*.alac;*.amr;*.ape;*.asf;*.au;*.avi;*.caf;*.flac;*.flv;*.m2ts;*.m4a;*.m4b;*.m4v;*.mka;*.mkv;*.mov;*.mp2;*.mp3;*.mp4;*.mpeg;*.mpg;*.oga;*.ogg;*.ogv;*.opus;*.ra;*.rm;*.ts;*.wav;*.webm;*.wma;*.wmv\0\0"
    )
    .encode_utf16()
    .collect();
    let title: Vec<u16> = "Locate moved media file".encode_utf16().chain(Some(0)).collect();
    let mut file = vec![0u16; 32_768];
    let mut dialog = OPENFILENAMEW {
        lStructSize: std::mem::size_of::<OPENFILENAMEW>() as u32,
        lpstrFilter: filter.as_ptr(),
        nFilterIndex: 1,
        lpstrFile: file.as_mut_ptr(),
        nMaxFile: file.len() as u32,
        lpstrTitle: title.as_ptr(),
        Flags: OFN_EXPLORER | OFN_FILEMUSTEXIST | OFN_PATHMUSTEXIST | OFN_NOCHANGEDIR,
        ..Default::default()
    };
    if unsafe { GetOpenFileNameW(&mut dialog) } == 0 {
        let error = unsafe { CommDlgExtendedError() };
        if error == 0 {
            return Ok(None);
        }
        anyhow::bail!("Windows media file dialog error {error:#x}");
    }
    let length = file.iter().position(|unit| *unit == 0).unwrap_or(file.len());
    if length == 0 {
        return Ok(None);
    }
    Ok(Some(PathBuf::from(OsString::from_wide(&file[..length]))))
}

#[cfg(not(windows))]
fn pick_library_media_file() -> Result<Option<PathBuf>> {
    anyhow::bail!("the native media file picker is available in Windows builds")
}

#[cfg(not(windows))]
fn pick_history_export_path() -> Result<Option<PathBuf>> {
    anyhow::bail!("the native CSV save dialog is available in Windows builds")
}

#[cfg(not(windows))]
fn pick_download_directory() -> Result<Option<PathBuf>> {
    anyhow::bail!("the native folder picker is available in Windows builds")
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

fn has_supported_media_extension(path: &Path) -> bool {
    let Some(extension) = path.extension().and_then(|extension| extension.to_str()) else {
        return false;
    };
    matches!(
        extension.to_ascii_lowercase().as_str(),
        "3g2"
            | "3gp"
            | "aac"
            | "ac3"
            | "aif"
            | "aiff"
            | "alac"
            | "amr"
            | "ape"
            | "asf"
            | "au"
            | "avi"
            | "caf"
            | "flac"
            | "flv"
            | "m2ts"
            | "m4a"
            | "m4b"
            | "m4v"
            | "mka"
            | "mkv"
            | "mov"
            | "mp2"
            | "mp3"
            | "mp4"
            | "mpeg"
            | "mpg"
            | "oga"
            | "ogg"
            | "ogv"
            | "opus"
            | "ra"
            | "rm"
            | "ts"
            | "wav"
            | "webm"
            | "wma"
            | "wmv"
    )
}

fn open_file(path: &Path) -> Result<()> {
    validate_output_file(path)?;

    open_with_platform_handler(path)
}

fn reveal_file(path: &Path) -> Result<()> {
    validate_output_file(path)?;
    reveal_with_platform_file_manager(path)
}

#[cfg(windows)]
fn reveal_with_platform_file_manager(path: &Path) -> Result<()> {
    let selection = format!("/select,{}", path.display());
    Command::new("explorer.exe")
        .arg(selection)
        .spawn()
        .context("launch Explorer for the selected media file")?;
    Ok(())
}

#[cfg(not(windows))]
fn reveal_with_platform_file_manager(path: &Path) -> Result<()> {
    let parent =
        path.parent().ok_or_else(|| anyhow::anyhow!("media file has no containing folder"))?;
    open_folder(&parent.to_path_buf())
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
    fn automatic_retry_backoff_doubles_and_caps_at_four_minutes() {
        assert_eq!(automatic_retry_delay_secs(0), 15);
        assert_eq!(automatic_retry_delay_secs(1), 30);
        assert_eq!(automatic_retry_delay_secs(2), 60);
        assert_eq!(automatic_retry_delay_secs(4), AUTO_RETRY_MAX_DELAY_SECS);
        assert_eq!(automatic_retry_delay_secs(20), AUTO_RETRY_MAX_DELAY_SECS);
        assert_eq!(automatic_retry_delay_secs(u32::MAX), AUTO_RETRY_MAX_DELAY_SECS);
    }

    #[test]
    fn playlist_select_all_and_none_ignore_entries_without_indexes() {
        let mut selection = PlaylistSelectionState {
            source_url: "https://example.test/playlist".to_owned(),
            source: "Example".to_owned(),
            entries: vec![
                PlaylistEntry { playlist_index: Some(1), title: "First".to_owned() },
                PlaylistEntry { playlist_index: None, title: "Unavailable".to_owned() },
                PlaylistEntry { playlist_index: Some(3), title: "Third".to_owned() },
            ],
            selected: vec![false, false, false],
        };

        set_playlist_selection(&mut selection, true);
        assert_eq!(selection.selected, [true, false, true]);
        assert_eq!(selection.selected_count(), 2);
        assert_eq!(selection.selectable_count(), 2);

        set_playlist_selection(&mut selection, false);
        assert_eq!(selection.selected, [false, false, false]);
        assert_eq!(selection.selected_count(), 0);
    }

    #[test]
    fn playlist_range_selection_adds_only_available_indexes_inclusive() {
        let mut selection = PlaylistSelectionState {
            source_url: "https://example.test/playlist".to_owned(),
            source: "Example".to_owned(),
            entries: vec![
                PlaylistEntry { playlist_index: Some(1), title: "First".to_owned() },
                PlaylistEntry { playlist_index: Some(2), title: "Second".to_owned() },
                PlaylistEntry { playlist_index: None, title: "Unavailable".to_owned() },
                PlaylistEntry { playlist_index: Some(4), title: "Fourth".to_owned() },
                PlaylistEntry { playlist_index: Some(5), title: "Fifth".to_owned() },
            ],
            selected: vec![true, false, false, false, false],
        };

        assert!(select_playlist_range(&mut selection, 2, 4));
        assert_eq!(selection.selected, [true, true, false, true, false]);
        assert!(!select_playlist_range(&mut selection, 0, 2));
        assert!(!select_playlist_range(&mut selection, 5, 4));
        assert!(!select_playlist_range(&mut selection, 7, 9));
    }

    #[tokio::test]
    async fn orderly_shutdown_keeps_retry_waits_but_pause_all_pauses_them() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut retry = DownloadRecord::queued(
            "retry-wait".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        retry.status = "Waiting for retry".to_owned();
        retry.retry_attempts = 1;
        retry.next_retry_at = Some(50);
        retry.error = Some("A temporary network error interrupted the request.".to_owned());
        store.save_download(retry).await.unwrap();
        let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::new()));

        assert_eq!(pause_all_downloads(&store, &active_jobs, false).await.unwrap().0, 0);
        let waiting = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(waiting.status, "Waiting for retry");
        assert_eq!(waiting.next_retry_at, Some(50));

        assert_eq!(pause_all_downloads(&store, &active_jobs, true).await.unwrap().0, 1);
        let paused = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(paused.status, "Paused");
        assert_eq!(paused.next_retry_at, None);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn dispatcher_capacity_tracks_raised_and_lowered_limits_without_stopping_workers() {
        assert!(can_dispatch_more(1, 3));
        assert!(!can_dispatch_more(3, 2));
        assert!(!can_dispatch_more(2, 2));
        assert!(can_dispatch_more(1, 2));
        assert!(!can_dispatch_more(1, 1));
    }
    use crate::model::{AudioFormat, AudioQuality, VideoQuality};

    #[test]
    fn tray_summary_counts_only_active_and_paused_states() {
        assert!(is_active_status("Analyzing"));
        assert!(is_active_status("Downloading"));
        assert!(is_active_status("Post-processing"));
        assert!(is_active_status("Pausing"));
        assert!(is_active_status("Canceling"));
        assert!(!is_active_status("Queued"));
        assert!(!is_active_status("Paused"));
        assert!(!is_active_status("Completed"));
        assert_eq!(tray_tooltip(0, 0), "Yt Save");
        assert_eq!(tray_tooltip(1, 2), "Yt Save — 1 active, 2 paused");
    }

    #[test]
    fn queue_rows_show_execution_positions_and_only_report_known_remaining_sizes() {
        let mut first =
            DownloadRecord::queued("first".to_owned(), "https://example.test/first".to_owned(), 10);
        first.total_bytes = Some(1_000);
        first.downloaded_bytes = Some(200);
        let mut paused = DownloadRecord::queued(
            "paused".to_owned(),
            "https://example.test/paused".to_owned(),
            11,
        );
        paused.status = "Paused".to_owned();
        let second = DownloadRecord::queued(
            "second".to_owned(),
            "https://example.test/second".to_owned(),
            12,
        );

        let rows = row_views(&[first.clone(), paused.clone(), second.clone()]);
        assert_eq!(rows[0].queue_position.as_str(), "#1");
        assert_eq!(rows[1].queue_position.as_str(), "Paused");
        assert_eq!(rows[2].queue_position.as_str(), "#2");
        assert_eq!(
            queue_summary_text(&[first, paused, second]),
            "2 queued · 1 paused · 800.0 B known; 2 sizes unknown"
        );
        assert_eq!(queue_summary_text(&[]), "No pending work");
    }

    #[test]
    fn history_search_is_case_insensitive_and_preserves_full_queue_positions() {
        let first =
            DownloadRecord::queued("first".to_owned(), "https://example.test/first".to_owned(), 10);
        let mut second = DownloadRecord::queued(
            "second".to_owned(),
            "https://media.example.test/featured".to_owned(),
            11,
        );
        second.title = "Featured clip".to_owned();
        second.source = "Example Site".to_owned();

        let rows = row_views_filtered(&[first, second], "MEDIA.EXAMPLE.TEST");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id.as_str(), "second");
        assert_eq!(rows[0].url.as_str(), "https://media.example.test/featured");
        assert_eq!(rows[0].queue_position.as_str(), "#2");
        assert!(row_views_filtered(&[], "no match").is_empty());
    }

    #[test]
    fn media_library_filters_real_files_and_surfaces_missing_paths() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let video_path = temp_dir.join("tutorial.mp4");
        let audio_path = temp_dir.join("track.mp3");
        std::fs::write(&video_path, b"video-bytes").unwrap();
        std::fs::write(&audio_path, b"audio").unwrap();

        let mut video = DownloadRecord::queued(
            "video".to_owned(),
            "https://video.example/watch/1".to_owned(),
            10,
        );
        video.title = "Rust tutorial".to_owned();
        video.source = "Video Example".to_owned();
        video.status = "Completed".to_owned();
        video.output_path = Some(video_path.to_string_lossy().into_owned());
        video.updated_at = 20;

        let mut audio = DownloadRecord::queued(
            "audio".to_owned(),
            "https://audio.example/track/2".to_owned(),
            11,
        );
        audio.title = "Quiet track".to_owned();
        audio.source = "Audio Example".to_owned();
        audio.status = "Completed".to_owned();
        audio.download_mode = DownloadMode::AudioOnly;
        audio.output_path = Some(audio_path.to_string_lossy().into_owned());
        audio.updated_at = 10;

        let mut missing = DownloadRecord::queued(
            "missing".to_owned(),
            "https://video.example/watch/3".to_owned(),
            12,
        );
        missing.title = "Moved tutorial".to_owned();
        missing.status = "Completed".to_owned();
        missing.output_path = Some(temp_dir.join("moved.mp4").to_string_lossy().into_owned());
        missing.updated_at = 30;

        let records = [video, audio, missing];
        let all = library_row_views(&records, "", "All files", "Newest first");
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id.as_str(), "missing");
        assert!(!all[0].path_available);
        assert_eq!(all[0].file_status.as_str(), "Unavailable · locate moved file");
        assert_eq!(all[1].id.as_str(), "video");
        assert!(all[1].path_available);
        assert!(all[1].size_label.as_str().contains("on disk"));

        let audio_only = library_row_views(&records, "audio.example", "Audio", "Title A–Z");
        assert_eq!(audio_only.len(), 1);
        assert_eq!(audio_only[0].id.as_str(), "audio");
        let missing_only = library_row_views(&records, "", "Missing files", "Newest first");
        assert_eq!(missing_only.len(), 1);
        assert_eq!(missing_only[0].id.as_str(), "missing");

        let largest_first = library_row_views(&records, "", "All files", "Largest first");
        assert_eq!(largest_first[0].id.as_str(), "video");
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn library_relink_accepts_media_extensions_and_rejects_unrelated_files() {
        assert!(has_supported_media_extension(Path::new("lesson.MP4")));
        assert!(has_supported_media_extension(Path::new("recording.opus")));
        assert!(!has_supported_media_extension(Path::new("notes.txt")));
        assert!(!has_supported_media_extension(Path::new("extensionless")));
    }

    #[test]
    fn library_cache_signature_ignores_queue_progress_but_tracks_completed_paths() {
        let mut completed = DownloadRecord::queued(
            "completed".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        completed.status = "Completed".to_owned();
        completed.output_path = Some("C:/Media/old.mp4".to_owned());
        let mut queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/queued".to_owned(),
            11,
        );
        let baseline = library_cache_signature_for(&[completed.clone(), queued.clone()]);

        queued.progress = 0.5;
        completed.progress = 1.0;
        assert_eq!(baseline, library_cache_signature_for(&[completed.clone(), queued.clone()]));

        completed.output_path = Some("C:/Media/moved.mp4".to_owned());
        assert_ne!(baseline, library_cache_signature_for(&[completed.clone(), queued.clone()]));
        completed.status = "Failed".to_owned();
        assert_ne!(baseline, library_cache_signature_for(&[completed, queued]));
    }

    #[test]
    fn download_destination_must_be_an_existing_directory_and_is_canonicalized() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let selected = temp_dir.join("selected");
        std::fs::create_dir_all(&selected).unwrap();
        let file = temp_dir.join("not-a-folder");
        std::fs::write(&file, b"x").unwrap();

        assert_eq!(
            validate_download_directory(&selected).unwrap(),
            selected.canonicalize().unwrap()
        );
        assert!(validate_download_directory(&file).is_err());
        assert!(validate_download_directory(&temp_dir.join("missing")).is_err());
        assert!(validate_download_directory(Path::new("")).is_err());
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

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

        let row = row_view_with_queue_position(&record, "");
        assert!(row.detail.as_str().contains(record.error.as_deref().unwrap()));
        assert!(row.detail.as_str().starts_with("Video · "));
    }

    #[test]
    fn retry_wait_has_a_visible_deadline_and_counts_as_pending_work() {
        let mut record = DownloadRecord::queued(
            "retrying".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        record.status = "Waiting for retry".to_owned();
        record.retry_attempts = 2;
        record.next_retry_at = Some(1_700_000_000);
        record.error = Some("A temporary network issue interrupted this media request.".to_owned());

        let row = row_view_with_queue_position(&record, "Retry");
        assert_eq!(row.status.as_str(), "Waiting for retry");
        assert_eq!(row.queue_position.as_str(), "Retry");
        assert!(row.detail.as_str().contains("Retry 2 scheduled for"));
        assert!(queue_summary_text(&[record]).contains("1 waiting for retry"));
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

        let row = row_view_with_queue_position(&record, "");

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
    fn clipboard_popup_position_accounts_for_dpi_and_monitor_origin() {
        let area = WorkArea { left: -1920, top: 80, right: 0, bottom: 1160 };

        assert_eq!(initial_popup_position(area, 1.5, 440, 214), (-676, 823));
    }

    #[test]
    fn clipboard_popup_displays_the_source_host_for_any_extractor_site() {
        assert_eq!(clipboard_source_host("https://media.example/watch/1"), "media.example");
        assert_eq!(clipboard_source_host("https://[invalid"), "Web media");
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
        let saved_presets = DownloadPresets {
            mode: DownloadMode::AudioOnly,
            audio_format: AudioFormat::Flac,
            audio_quality: AudioQuality::K320,
            video_quality: VideoQuality::P1440,
        };
        store.set_download_presets(saved_presets, 1).await.unwrap();
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
        active.video_quality = VideoQuality::P720;
        active.download_directory = "C:\\Media\\Archive".to_owned();
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
        let mut pausing = DownloadRecord::queued(
            "pausing".to_owned(),
            "https://example.test/pausing".to_owned(),
            41,
        );
        pausing.status = "Pausing".to_owned();
        pausing.downloaded_bytes = Some(15);
        let mut paused = DownloadRecord::queued(
            "paused".to_owned(),
            "https://example.test/paused".to_owned(),
            42,
        );
        paused.status = "Paused".to_owned();
        paused.progress = 51.0;
        paused.downloaded_bytes = Some(510);
        store.save_download(queued.clone()).await.unwrap();
        store.save_download(active.clone()).await.unwrap();
        store.save_download(later_playlist_item).await.unwrap();
        store.save_download(earlier_playlist_item).await.unwrap();
        store.save_download(canceling).await.unwrap();
        store.save_download(pausing).await.unwrap();
        store.save_download(paused).await.unwrap();

        let pending = recover_pending(&store).await.unwrap();
        assert_eq!(store.load_download_presets().await.unwrap(), saved_presets);
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
        assert_eq!(recovered_active.video_quality, VideoQuality::P720);
        assert_eq!(recovered_active.download_directory, "C:\\Media\\Archive");
        let recovered_canceling = rows.iter().find(|record| record.id == "canceling").unwrap();
        assert_eq!(recovered_canceling.status, "Failed");
        assert_eq!(recovered_canceling.progress, 22.0);
        assert_eq!(recovered_canceling.downloaded_bytes, Some(22));
        assert!(
            recovered_canceling.error.as_deref().unwrap().contains("process-tree cancellation")
        );
        assert!(!pending.iter().any(|record| record.id == "canceling"));
        let recovered_pausing = rows.iter().find(|record| record.id == "pausing").unwrap();
        assert_eq!(recovered_pausing.status, "Failed");
        assert_eq!(recovered_pausing.downloaded_bytes, Some(15));
        assert!(recovered_pausing.error.as_deref().unwrap().contains("process-tree pause"));
        let recovered_paused = rows.iter().find(|record| record.id == "paused").unwrap();
        assert_eq!(recovered_paused.status, "Paused");
        assert_eq!(recovered_paused.progress, 51.0);
        assert_eq!(recovered_paused.downloaded_bytes, Some(510));
        assert!(!pending.iter().any(|record| record.id == "paused"));
        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}
