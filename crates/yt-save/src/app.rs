use std::{
    collections::{HashMap, HashSet},
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
use slint::{ComponentHandle, ModelRc, SharedString, VecModel};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, mpsc, watch},
    task::JoinSet,
};
use tracing::{error, info, warn};
use uuid::Uuid;

const AUTOMATIC_FORMAT_CHOICE: &str = "Automatic (quality preset)";

mod clipboard_popup;
mod library;
#[cfg(windows)]
mod cookie_login;
mod tray;
use library::{
    LibraryCacheEntry, LibraryCacheSignature, library_cache_signature_for,
    library_row_views_from_cache, request_library_scan, scan_library_entries,
};
use tray::{ShutdownState, cancel_staged_update_helper};

use crate::{
    AppTray, ClipboardPopup, DownloadRowData, LibraryRowData, MainWindow, PlaylistEntryRowData,
    PlaylistSubscriptionEntryRowData, PlaylistSubscriptionRowData,
    app_update::{AppRelease, AppUpdateChannel, CheckOutcome, InstallerKind},
    clipboard_monitor::{ClipboardCandidate, ClipboardMonitor, WorkArea},
    cookie_auth::CookieStore,
    dependencies::DependencyPaths,
    engine::{
        DownloadEngine, DownloadOptions, EngineOutcome, JobSignal, PLAYLIST_PAGE_SIZE,
        ProgressUpdate, is_retryable_engine_error,
    },
    history::download_history_csv,
    model::{
        DownloadMode, DownloadPresets, DownloadRecord, MediaFormatOption, MediaMetadata,
        NamedDownloadPreset, PlaylistEntry, PlaylistMediaEntry, SubtitleFormat, download_speed_limit_label,
        format_bytes, format_duration, format_is_available, is_safe_format_id,
        normalize_subtitle_languages, normalized_host_key, subtitle_languages_available,
    },
    persistence::{
        DEFAULT_AUTO_RETRY_ATTEMPTS, DEFAULT_CONCURRENT_FRAGMENTS, DEFAULT_MAX_CONCURRENT_PER_HOST,
        MAX_AUTO_RETRY_ATTEMPTS, MAX_CONCURRENT_PER_HOST, PLAYLIST_CHECK_INTERVAL_1_HOUR,
        PLAYLIST_CHECK_INTERVAL_6_HOURS, PLAYLIST_CHECK_INTERVAL_7_DAYS,
        PLAYLIST_CHECK_INTERVAL_12_HOURS, PLAYLIST_CHECK_INTERVAL_24_HOURS,
        PLAYLIST_CHECK_INTERVAL_MANUAL, PlaylistSubscription, PlaylistSubscriptionEntry,
        PlaylistSubscriptionEntryInput, PlaylistSubscriptionQueueItem, QueueMove, Store,
        TrayPreferences,
    },
};

#[cfg(windows)]
use crate::CookieLoginWindow;

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
    CookieAuthState {
        generation: u64,
        url: String,
        has_saved: Option<bool>,
        message: Option<String>,
        close_login: bool,
        login_session: Option<u64>,
        refresh_current_url: bool,
    },
    ClipboardCandidate {
        generation: u64,
        candidate: ClipboardCandidate,
    },
    DependencyStatus(String),
    DependencyUpdateState(bool),
    AppUpdateView(AppUpdateUiState),
    ClearPreview {
        request_id: u64,
    },
    Metadata {
        request_id: u64,
        source_url: String,
        display_url: String,
        selection_mode: DownloadMode,
        metadata: Box<MediaMetadata>,
    },
    PlaylistPage {
        request_id: u64,
        start_index: usize,
        end_index: usize,
        select_range: Option<(i64, i64)>,
        result: std::result::Result<Box<MediaMetadata>, String>,
    },
}

#[derive(Clone)]
struct AppUpdateUiState {
    channel: AppUpdateChannel,
    status: String,
    release_notes: String,
    release_url: String,
    available: bool,
    checking: bool,
    downloading: bool,
    install_ready: bool,
    installing: bool,
}

impl AppUpdateUiState {
    fn idle(channel: AppUpdateChannel, status: impl Into<String>) -> Self {
        Self {
            channel,
            status: status.into(),
            release_notes: String::new(),
            release_url: String::new(),
            available: false,
            checking: false,
            downloading: false,
            install_ready: false,
            installing: false,
        }
    }
}

#[derive(Clone)]
struct StagedAppInstaller {
    path: PathBuf,
    kind: InstallerKind,
}

type ActiveJobs = Arc<Mutex<std::collections::HashMap<String, Arc<watch::Sender<JobSignal>>>>>;
type ActiveHostCounts = Arc<Mutex<HashMap<String, usize>>>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct QueueActionState {
    can_pause_all: bool,
    can_resume_all: bool,
    can_cancel_all: bool,
    can_retry_failed: bool,
}

struct ActiveJobRegistration {
    id: String,
    jobs: ActiveJobs,
    sender: Arc<watch::Sender<JobSignal>>,
}

struct ActiveHostRegistration {
    host_key: String,
    counts: ActiveHostCounts,
}

impl ActiveHostRegistration {
    fn register(host_key: String, counts: ActiveHostCounts) -> Self {
        let mut active = counts.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *active.entry(host_key.clone()).or_default() += 1;
        drop(active);
        Self { host_key, counts }
    }
}

impl Drop for ActiveHostRegistration {
    fn drop(&mut self) {
        let mut active = self.counts.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = active.get_mut(&self.host_key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.remove(&self.host_key);
            }
        }
    }
}

fn active_host_counts_snapshot(counts: &ActiveHostCounts) -> HashMap<String, usize> {
    counts.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
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
    request_id: u64,
    source_url: String,
    source: String,
    title: String,
    entries: Vec<PlaylistEntry>,
    selected: Vec<bool>,
    next_page_start: usize,
    has_more_entries: bool,
    loading_page: bool,
}

#[derive(Clone)]
struct PlaylistPageLoader {
    selection_state: Arc<Mutex<Option<PlaylistSelectionState>>>,
    engine: DownloadEngine,
    handle: tokio::runtime::Handle,
    events: mpsc::UnboundedSender<UiEvent>,
    weak_window: slint::Weak<MainWindow>,
}

#[derive(Clone, Default)]
struct PlaylistSubscriptionReviewState {
    subscription_id: String,
    subscription: Option<PlaylistSubscription>,
    entries: Vec<PlaylistSubscriptionEntry>,
    selected_media_keys: HashSet<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PlaylistSubscriptionCheckResult {
    new_entries: usize,
    auto_queued_entries: usize,
    auto_queue_skipped_for_subtitle_mode: bool,
}

struct PlaylistSubscriptionSchedulerState {
    queue_wake: Arc<Notify>,
    dispatch_gate: Arc<AsyncMutex<()>>,
    current_download_presets: Arc<Mutex<DownloadPresets>>,
    review_state: Arc<Mutex<PlaylistSubscriptionReviewState>>,
    check_active: Arc<AtomicBool>,
    weak_window: slint::Weak<MainWindow>,
    events: mpsc::UnboundedSender<UiEvent>,
    shutdown_started: Arc<AtomicBool>,
}

struct PlaylistSubscriptionCheckGuard {
    active: Arc<AtomicBool>,
    window: slint::Weak<MainWindow>,
}

struct BatchEnqueueGuard {
    active: Arc<AtomicBool>,
    window: slint::Weak<MainWindow>,
}

impl PlaylistSubscriptionCheckGuard {
    fn start(active: &Arc<AtomicBool>, window: slint::Weak<MainWindow>) -> Option<Self> {
        active.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).ok()?;
        let weak = window.clone();
        let _ = slint::invoke_from_event_loop(move || {
            if let Some(window) = weak.upgrade() {
                window.set_subscription_checking(true);
            }
        });
        Some(Self { active: active.clone(), window })
    }
}

impl Drop for PlaylistSubscriptionCheckGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        let weak = self.window.clone();
        let _ = slint::invoke_from_event_loop(move || {
            if let Some(window) = weak.upgrade() {
                window.set_subscription_checking(false);
            }
        });
    }
}

impl BatchEnqueueGuard {
    fn start(active: &Arc<AtomicBool>, window: slint::Weak<MainWindow>) -> Option<Self> {
        active.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).ok()?;
        let weak = window.clone();
        let _ = slint::invoke_from_event_loop(move || {
            if let Some(window) = weak.upgrade() {
                window.set_batch_enqueue_in_progress(true);
            }
        });
        Some(Self { active: active.clone(), window })
    }
}

impl Drop for BatchEnqueueGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        let weak = self.window.clone();
        let _ = slint::invoke_from_event_loop(move || {
            if let Some(window) = weak.upgrade() {
                window.set_batch_enqueue_in_progress(false);
            }
        });
    }
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

fn playlist_selection_summary(selection: &PlaylistSelectionState) -> SharedString {
    if selection.selectable_count() == 0 {
        SharedString::from("No entries with valid indexes have been loaded.")
    } else {
        SharedString::from(format!(
            "Selected {} of {} loaded entries.",
            selection.selected_count(),
            selection.selectable_count()
        ))
    }
}

fn merge_playlist_page(selection: &mut PlaylistSelectionState, entries: Vec<PlaylistEntry>) {
    for entry in entries {
        let duplicate = entry.playlist_index.is_some_and(|index| {
            selection.entries.iter().any(|loaded| loaded.playlist_index == Some(index))
        });
        if !duplicate {
            selection.entries.push(entry);
            selection.selected.push(false);
        }
    }

    let mut combined = selection
        .entries
        .drain(..)
        .zip(selection.selected.drain(..))
        .enumerate()
        .collect::<Vec<_>>();
    combined.sort_by_key(|(order, (entry, _))| match entry.playlist_index {
        Some(index) => (0_u8, index, *order),
        None => (1_u8, i64::MAX, *order),
    });
    for (_, (entry, selected)) in combined {
        selection.entries.push(entry);
        selection.selected.push(selected);
    }
}

fn playlist_subscription_entry_inputs(
    source: &str,
    entries: &[PlaylistMediaEntry],
) -> Vec<PlaylistSubscriptionEntryInput> {
    let source_key = source.trim().to_ascii_lowercase();
    entries
        .iter()
        .filter_map(|entry| {
            let media_id = entry.media_id.as_deref().map(str::trim).filter(|id| !id.is_empty());
            let media_url = entry.media_url.trim();
            let media_key = media_id.map_or_else(
                || (!media_url.is_empty()).then(|| format!("url:{media_url}")),
                |id| Some(format!("{source_key}:{id}")),
            )?;
            Some(PlaylistSubscriptionEntryInput {
                media_key,
                media_id: media_id.map(str::to_owned),
                media_url: media_url.to_owned(),
                title: entry.title.clone(),
                playlist_index: entry.playlist_index,
                available: entry.available
                    && !media_url.is_empty()
                    && validate_url(media_url).is_ok(),
            })
        })
        .collect()
}

fn playlist_subscription_auto_queue_items(
    source: &str,
    playlist_title: &str,
    entries: &[PlaylistSubscriptionEntryInput],
    presets: DownloadPresets,
    download_directory: &str,
    created_at: i64,
) -> Vec<PlaylistSubscriptionQueueItem> {
    let group_id = Uuid::new_v4().to_string();
    entries
        .iter()
        .filter(|entry| entry.available && !entry.media_url.trim().is_empty())
        .map(|entry| {
            let mut record = DownloadRecord::queued(
                Uuid::new_v4().to_string(),
                entry.media_url.clone(),
                created_at,
            );
            record.title = entry.title.clone();
            record.source = source.to_owned();
            record.playlist_group_id = Some(group_id.clone());
            record.playlist_group_title = Some(playlist_title.to_owned());
            record.download_mode = presets.mode;
            record.audio_format = presets.audio_format;
            record.audio_quality = presets.audio_quality;
            record.video_quality = presets.video_quality;
            record.download_speed_limit_bytes_per_second =
                presets.download_speed_limit_bytes_per_second;
            record.download_directory = download_directory.to_owned();
            PlaylistSubscriptionQueueItem { media_key: entry.media_key.clone(), record }
        })
        .collect()
}

fn playlist_subscription_check_status(result: PlaylistSubscriptionCheckResult) -> String {
    if result.new_entries == 0 {
        return if result.auto_queue_skipped_for_subtitle_mode {
            "Playlist checked. No new entries were found. Auto-queue was skipped because subtitle-only mode does not support playlist batches.".to_owned()
        } else {
            "Playlist checked. No new entries were found.".to_owned()
        };
    }
    if result.auto_queue_skipped_for_subtitle_mode {
        return format!(
            "Found {} new playlist {}. They remain in review because subtitle-only mode does not support playlist batches. Choose Video + audio, Video only, or Audio only to auto-queue future entries.",
            result.new_entries,
            plural_word(result.new_entries, "entry", "entries")
        );
    }
    if result.auto_queued_entries == 0 {
        return format!(
            "Found {} new playlist {}. Review them before queueing.",
            result.new_entries,
            plural_word(result.new_entries, "entry", "entries")
        );
    }
    format!(
        "Found {} new playlist {}; auto-queued {} available {} using your current New Download choices and save folder. Unavailable entries remain for review.",
        result.new_entries,
        plural_word(result.new_entries, "entry", "entries"),
        result.auto_queued_entries,
        plural_word(result.auto_queued_entries, "entry", "entries")
    )
}

fn playlist_subscription_rows(
    subscriptions: &[PlaylistSubscription],
    selected_id: &str,
) -> Vec<PlaylistSubscriptionRowData> {
    subscriptions
        .iter()
        .map(|subscription| {
            let summary = if subscription.last_check_error.is_some() {
                "check failed".to_owned()
            } else if subscription.last_checked_at.is_none() {
                "not checked".to_owned()
            } else if subscription.pending_count == 0 {
                "up to date".to_owned()
            } else {
                format!("{} new", subscription.pending_count)
            };
            PlaylistSubscriptionRowData {
                id: SharedString::from(subscription.id.as_str()),
                title: SharedString::from(subscription.title.as_str()),
                source: SharedString::from(subscription.source.as_str()),
                url: SharedString::from(subscription.url.as_str()),
                summary: SharedString::from(summary),
                check_frequency: SharedString::from(subscription_check_frequency_label(
                    subscription.check_interval_seconds,
                )),
                auto_queue_new_entries: subscription.auto_queue_new_entries,
                selected: subscription.id == selected_id,
            }
        })
        .collect()
}

fn playlist_subscription_entry_rows(
    entries: &[PlaylistSubscriptionEntry],
    selected_media_keys: &HashSet<String>,
) -> Vec<PlaylistSubscriptionEntryRowData> {
    entries
        .iter()
        .map(|entry| {
            let state = match entry.state.as_str() {
                "new" if entry.available => "New",
                "new" => "Unavailable",
                "queued" => "Queued",
                "downloaded" => "Downloaded",
                "ignored" => "Dismissed",
                _ => "Archived",
            };
            let detail = if !entry.available {
                "yt-dlp did not provide a directly queueable media URL".to_owned()
            } else if let Some(index) = entry.playlist_index {
                format!("Playlist index {index}")
            } else {
                "Playlist item".to_owned()
            };
            PlaylistSubscriptionEntryRowData {
                media_key: SharedString::from(entry.media_key.as_str()),
                title: SharedString::from(entry.title.as_str()),
                detail: SharedString::from(detail),
                state: SharedString::from(state),
                selectable: entry.state == "new" && entry.available,
                selected: selected_media_keys.contains(&entry.media_key),
            }
        })
        .collect()
}

fn subscription_last_checked(subscription: Option<&PlaylistSubscription>) -> String {
    let Some(subscription) = subscription else {
        return "Choose a saved playlist to review its archive.".to_owned();
    };
    let last_check = if let Some(error) = &subscription.last_check_error {
        format!("Last check failed: {error}")
    } else if let Some(timestamp) = subscription.last_checked_at {
        let checked = local_timestamp_label(timestamp);
        format!("Last checked {checked} · {} new", subscription.pending_count)
    } else {
        "Not checked yet.".to_owned()
    };
    let frequency = subscription_check_frequency_label(subscription.check_interval_seconds);
    let next_check = subscription
        .next_check_at
        .map(|timestamp| format!("Next check {}", local_timestamp_label(timestamp)))
        .unwrap_or_else(|| "No scheduled check".to_owned());
    format!("{last_check} · {frequency} · {next_check}")
}

fn local_timestamp_label(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|value| value.with_timezone(&chrono::Local).format("%b %d, %Y %H:%M").to_string())
        .unwrap_or_else(|| "unknown time".to_owned())
}

fn subscription_check_frequency_label(interval_seconds: i64) -> &'static str {
    match interval_seconds {
        PLAYLIST_CHECK_INTERVAL_1_HOUR => "Every hour",
        PLAYLIST_CHECK_INTERVAL_6_HOURS => "Every 6 hours",
        PLAYLIST_CHECK_INTERVAL_12_HOURS => "Every 12 hours",
        PLAYLIST_CHECK_INTERVAL_24_HOURS => "Daily",
        PLAYLIST_CHECK_INTERVAL_7_DAYS => "Weekly",
        _ => "Manual",
    }
}

fn request_playlist_page(
    loader: &PlaylistPageLoader,
    request_id: u64,
    start_index: usize,
    end_index: usize,
    select_range: Option<(i64, i64)>,
) -> std::result::Result<(), String> {
    let requested_count = end_index.saturating_sub(start_index).saturating_add(1);
    if start_index == 0 || end_index < start_index || requested_count > PLAYLIST_PAGE_SIZE {
        return Err(format!(
            "Load no more than {PLAYLIST_PAGE_SIZE} playlist entries per request."
        ));
    }
    let source_url = {
        let mut state = loader
            .selection_state
            .lock()
            .map_err(|_| "Playlist selection state is unavailable.".to_owned())?;
        let selection = state
            .as_mut()
            .filter(|selection| selection.request_id == request_id)
            .ok_or_else(|| "Analyze a playlist before loading more entries.".to_owned())?;
        if selection.loading_page {
            return Err("A playlist page is already loading.".to_owned());
        }
        selection.loading_page = true;
        selection.source_url.clone()
    };

    if let Some(window) = loader.weak_window.upgrade() {
        window.set_playlist_page_loading(true);
    }
    let _ = loader
        .events
        .send(UiEvent::Status(format!("Loading playlist indexes {start_index}–{end_index}…")));
    let engine = loader.engine.clone();
    let events = loader.events.clone();
    loader.handle.spawn(async move {
        let result = engine
            .inspect_playlist_page(&source_url, start_index, end_index)
            .await
            .map(Box::new)
            .map_err(|error| error.to_string());
        let _ = events.send(UiEvent::PlaylistPage {
            request_id,
            start_index,
            end_index,
            select_range,
            result,
        });
    });
    Ok(())
}

fn subscription_check_frequency_seconds(label: &str) -> Option<i64> {
    match label {
        "Manual" => Some(PLAYLIST_CHECK_INTERVAL_MANUAL),
        "Every hour" => Some(PLAYLIST_CHECK_INTERVAL_1_HOUR),
        "Every 6 hours" => Some(PLAYLIST_CHECK_INTERVAL_6_HOURS),
        "Every 12 hours" => Some(PLAYLIST_CHECK_INTERVAL_12_HOURS),
        "Daily" => Some(PLAYLIST_CHECK_INTERVAL_24_HOURS),
        "Weekly" => Some(PLAYLIST_CHECK_INTERVAL_7_DAYS),
        _ => None,
    }
}

async fn refresh_playlist_subscription_view(
    store: &Store,
    review_state: &Arc<Mutex<PlaylistSubscriptionReviewState>>,
    weak_window: slint::Weak<MainWindow>,
    requested_id: Option<String>,
) -> Result<()> {
    let subscriptions = store.list_playlist_subscriptions().await?;
    let current_id =
        review_state.lock().map(|review| review.subscription_id.clone()).unwrap_or_default();
    let selected_id = requested_id
        .filter(|requested| subscriptions.iter().any(|subscription| &subscription.id == requested))
        .or_else(|| {
            subscriptions
                .iter()
                .any(|subscription| subscription.id == current_id)
                .then_some(current_id)
        })
        .or_else(|| subscriptions.first().map(|subscription| subscription.id.clone()))
        .unwrap_or_default();
    let selected_subscription =
        subscriptions.iter().find(|subscription| subscription.id == selected_id).cloned();
    let entries = if selected_id.is_empty() {
        Vec::new()
    } else {
        store.list_playlist_subscription_entries(selected_id.clone()).await?
    };
    let review_snapshot = {
        let mut review = review_state
            .lock()
            .map_err(|_| anyhow::anyhow!("playlist subscription review state is unavailable"))?;
        if review.subscription_id != selected_id {
            review.selected_media_keys.clear();
        }
        review.subscription_id = selected_id.clone();
        review.subscription = selected_subscription.clone();
        review.entries = entries.clone();
        review.selected_media_keys.retain(|media_key| {
            entries.iter().any(|entry| {
                entry.media_key == *media_key && entry.state == "new" && entry.available
            })
        });
        review.clone()
    };
    let subscription_rows = playlist_subscription_rows(&subscriptions, &selected_id);
    let entry_rows = playlist_subscription_entry_rows(
        &review_snapshot.entries,
        &review_snapshot.selected_media_keys,
    );
    let summary = if review_snapshot.selected_media_keys.is_empty() {
        "Select new entries to queue or dismiss them.".to_owned()
    } else {
        format!(
            "{} new {} selected",
            review_snapshot.selected_media_keys.len(),
            plural_word(review_snapshot.selected_media_keys.len(), "entry", "entries")
        )
    };
    let title = review_snapshot
        .subscription
        .as_ref()
        .map_or_else(String::new, |subscription| subscription.title.clone());
    let details = subscription_last_checked(review_snapshot.subscription.as_ref());
    let selected_count =
        i32::try_from(review_snapshot.selected_media_keys.len()).unwrap_or(i32::MAX);
    slint::invoke_from_event_loop(move || {
        if let Some(window) = weak_window.upgrade() {
            window.set_playlist_subscriptions(ModelRc::new(VecModel::from(subscription_rows)));
            window.set_playlist_subscription_entries(ModelRc::new(VecModel::from(entry_rows)));
            window.set_selected_subscription_id(selected_id.into());
            window.set_subscription_check_frequency(
                selected_subscription
                    .as_ref()
                    .map_or("Manual", |subscription| {
                        subscription_check_frequency_label(subscription.check_interval_seconds)
                    })
                    .into(),
            );
            window.set_subscription_auto_queue_new_entries(
                selected_subscription
                    .as_ref()
                    .is_some_and(|subscription| subscription.auto_queue_new_entries),
            );
            window.set_selected_subscription_title(title.into());
            window.set_selected_subscription_details(details.into());
            window.set_subscription_selection_summary(summary.into());
            window.set_playlist_subscription_selected_count(selected_count);
        }
    })?;
    Ok(())
}

fn plural_word<'a>(count: usize, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 { singular } else { plural }
}

async fn perform_playlist_subscription_check(
    store: &Store,
    engine: &DownloadEngine,
    subscription_id: &str,
    current_download_presets: &Mutex<DownloadPresets>,
    dispatch_gate: &AsyncMutex<()>,
    shutdown_started: &AtomicBool,
) -> Result<PlaylistSubscriptionCheckResult> {
    let subscription = store
        .list_playlist_subscriptions()
        .await?
        .into_iter()
        .find(|subscription| subscription.id == subscription_id)
        .context("playlist subscription no longer exists")?;
    let metadata = engine.inspect_playlist_for_subscription(&subscription.url).await?;
    if !metadata.is_playlist {
        anyhow::bail!("yt-dlp no longer identifies this URL as a playlist.");
    }
    let entries =
        playlist_subscription_entry_inputs(&metadata.source, &metadata.subscription_entries);
    let check_time = now();
    let mut auto_queue_skipped_for_subtitle_mode = false;
    let queue_items = if subscription.auto_queue_new_entries {
        let presets = *current_download_presets
            .lock()
            .map_err(|_| anyhow::anyhow!("current New Download choices are unavailable"))?;
        if presets.mode == DownloadMode::SubtitlesOnly {
            auto_queue_skipped_for_subtitle_mode = true;
            Vec::new()
        } else {
            let fallback_directory =
                engine.default_output_directory().to_string_lossy().into_owned();
            let configured_directory =
                store.get_or_initialize_download_directory(fallback_directory).await?;
            let directory_title = metadata.title.clone();
            let directory_subscription_id = subscription.id.clone();
            let directory = tokio::task::spawn_blocking(move || {
                create_playlist_subscription_directory(
                    Path::new(&configured_directory),
                    &directory_title,
                    &directory_subscription_id,
                )
            })
            .await
            .context("join playlist subscription destination creation")??;
            playlist_subscription_auto_queue_items(
                &metadata.source,
                &metadata.title,
                &entries,
                presets,
                &directory.to_string_lossy(),
                check_time,
            )
        }
    } else {
        Vec::new()
    };
    let _dispatch_guard = dispatch_gate.lock().await;
    anyhow::ensure!(
        !shutdown_started.load(Ordering::Acquire),
        "Playlist check stopped because Yt Save is preparing to close or update."
    );
    let (new_entries, auto_queued_entries) = if auto_queue_skipped_for_subtitle_mode {
        let new_entries = store
            .record_playlist_subscription_check(
                subscription_id.to_owned(),
                metadata.title,
                metadata.source,
                check_time,
                entries,
            )
            .await?;
        (new_entries, 0)
    } else {
        store
            .record_playlist_subscription_check_with_queue_items(
                subscription_id.to_owned(),
                metadata.title,
                metadata.source,
                check_time,
                entries,
                queue_items,
            )
            .await?
    };
    Ok(PlaylistSubscriptionCheckResult {
        new_entries,
        auto_queued_entries,
        auto_queue_skipped_for_subtitle_mode,
    })
}

async fn run_playlist_subscription_scheduler(
    store: Store,
    engine: DownloadEngine,
    state: PlaylistSubscriptionSchedulerState,
) {
    let PlaylistSubscriptionSchedulerState {
        queue_wake,
        dispatch_gate,
        current_download_presets,
        review_state,
        check_active,
        weak_window,
        events,
        shutdown_started,
    } = state;
    let mut timer = tokio::time::interval(Duration::from_secs(30));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        if shutdown_started.load(Ordering::Acquire) {
            break;
        }
        let due_subscriptions = match store.list_due_playlist_subscriptions(now(), 8).await {
            Ok(subscriptions) => subscriptions,
            Err(error) => {
                error!(%error, "load scheduled playlist checks");
                continue;
            }
        };
        if due_subscriptions.is_empty() {
            continue;
        }
        let Some(check_guard) =
            PlaylistSubscriptionCheckGuard::start(&check_active, weak_window.clone())
        else {
            continue;
        };

        let mut checked = 0usize;
        let mut failed = 0usize;
        let mut new_entries = 0usize;
        let mut auto_queued_entries = 0usize;
        let mut auto_queue_skipped_count = 0usize;
        for subscription in due_subscriptions {
            if shutdown_started.load(Ordering::Acquire) {
                break;
            }
            match perform_playlist_subscription_check(
                &store,
                &engine,
                &subscription.id,
                &current_download_presets,
                &dispatch_gate,
                &shutdown_started,
            )
            .await
            {
                Ok(result) => {
                    checked += 1;
                    new_entries = new_entries.saturating_add(result.new_entries);
                    auto_queued_entries =
                        auto_queued_entries.saturating_add(result.auto_queued_entries);
                    auto_queue_skipped_count +=
                        usize::from(result.auto_queue_skipped_for_subtitle_mode);
                }
                Err(error) => {
                    if shutdown_started.load(Ordering::Acquire) {
                        break;
                    }
                    failed += 1;
                    if let Err(save_error) = store
                        .record_playlist_subscription_error(
                            subscription.id.clone(),
                            error.to_string(),
                            now(),
                        )
                        .await
                    {
                        error!(%save_error, subscription_id = %subscription.id, "save scheduled playlist check failure");
                    }
                    error!(%error, subscription_id = %subscription.id, "scheduled playlist check failed");
                }
            }
        }
        drop(check_guard);

        if auto_queued_entries > 0 {
            queue_wake.notify_one();
        }

        if checked > 0 || failed > 0 {
            let mut message = format!(
                "Scheduled playlist checks: {checked} completed, {failed} failed, {new_entries} new {}.",
                plural_word(new_entries, "entry", "entries")
            );
            if auto_queued_entries > 0 {
                message.push_str(&format!(
                    " Auto-queued {auto_queued_entries} available {} using saved New Download choices and folders. Unavailable entries remain for review.",
                    plural_word(auto_queued_entries, "entry", "entries")
                ));
            }
            if auto_queue_skipped_count > 0 {
                message.push_str(&format!(
                    " Auto-queue was skipped for {auto_queue_skipped_count} playlist {} because subtitle-only mode does not support playlist batches; new entries remain in review.",
                    plural_word(auto_queue_skipped_count, "subscription", "subscriptions")
                ));
            } else if auto_queued_entries == 0 {
                message.push_str(" New entries wait for your review.");
            }
            let _ = events.send(UiEvent::Status(message));
            let selected_id = review_state.lock().ok().map(|review| review.subscription_id.clone());
            if let Err(error) = refresh_playlist_subscription_view(
                &store,
                &review_state,
                weak_window.clone(),
                selected_id,
            )
            .await
            {
                error!(%error, "refresh scheduled playlist checks");
            }
        }
    }
}

#[cfg(windows)]
async fn run_cookie_lease_sweeper(cookie_store: CookieStore) {
    let mut timer = tokio::time::interval(Duration::from_secs(60));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    timer.tick().await;
    loop {
        timer.tick().await;
        let store = cookie_store.clone();
        match tokio::task::spawn_blocking(move || store.cleanup_stale_leases()).await {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => warn!("could not remove stale generated cookie staging files"),
            Err(_) => warn!("could not inspect stale generated cookie staging files"),
        }
    }
}

pub fn run() -> Result<()> {
    #[cfg(windows)]
    let _instance_guard = match crate::single_instance::SingleInstanceGuard::acquire()? {
        Some(guard) => guard,
        None => {
            crate::single_instance::show_already_running_message();
            return Ok(());
        }
    };

    let paths = AppPaths::discover()?;
    let cookie_store = CookieStore::new(paths.data_local.join("cookies"));
    #[cfg(windows)]
    if cookie_store.cleanup_stale_leases().is_err() {
        error!("could not remove stale generated cookie staging files");
    }
    std::fs::create_dir_all(&paths.downloads)
        .with_context(|| format!("create download directory {}", paths.downloads.display()))?;
    let store = Store::open(paths.database.clone())?;
    let mut dependencies = DependencyPaths::discover()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("yt-save-worker")
        .build()
        .context("create background runtime")?;
    #[cfg(windows)]
    runtime.spawn(run_cookie_lease_sweeper(cookie_store.clone()));
    let user_engine_directory = DependencyPaths::user_yt_dlp_directory()?;
    let packaged_yt_dlp = dependencies.directory.join("yt-dlp.exe");
    dependencies.yt_dlp = runtime.block_on(crate::yt_dlp_update::recover_startup_engine(
        &user_engine_directory,
        &packaged_yt_dlp,
    ));
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
    let current_download_presets = Arc::new(Mutex::new(download_presets));
    let saved_download_presets = runtime
        .block_on(store.load_saved_download_presets())
        .unwrap_or_else(|error| {
            error!(%error, "load saved download presets; continuing with no saved presets");
            Vec::new()
        });
    let saved_download_presets_state = Arc::new(Mutex::new(saved_download_presets.clone()));
    let applying_named_download_preset = Arc::new(AtomicBool::new(false));
    let max_concurrent_downloads = runtime.block_on(store.load_max_concurrent_downloads())?;
    let max_concurrent_per_host = runtime.block_on(store.load_max_concurrent_per_host())?;
    let concurrent_fragments = runtime.block_on(store.load_concurrent_fragments())?;
    let auto_retry_attempts = runtime.block_on(store.load_auto_retry_attempts())?;
    let clipboard_monitoring_enabled =
        runtime.block_on(store.load_clipboard_monitoring_enabled())?;
    let app_update_channel = runtime.block_on(store.load_app_update_channel())?;
    let app_update_channel_state = Arc::new(Mutex::new(app_update_channel));
    let tray_preferences = runtime.block_on(store.load_tray_preferences())?;
    let tray_preferences_state = Arc::new(Mutex::new(tray_preferences));
    let engine = DownloadEngine::new(dependencies.clone(), download_directory.clone())
        .with_cookie_store(cookie_store.clone());
    let handle = runtime.handle().clone();
    let window = MainWindow::new().context("create main window")?;
    #[cfg(windows)]
    let (cookie_support_enabled, cookie_support_status) =
        cookie_login::cookie_webview_runtime_status();
    #[cfg(not(windows))]
    let (cookie_support_enabled, cookie_support_status) =
        (false, "Site sign-in is available in the Windows version.".to_owned());
    #[cfg(windows)]
    let cookie_login_window = CookieLoginWindow::new().context("create sign-in window")?;
    let tray = AppTray::new().context("create system tray icon")?;
    #[cfg(windows)]
    let popup = crate::window_activation::create_non_activating_window(ClipboardPopup::new)
        .context("create clipboard popup")?;
    #[cfg(not(windows))]
    let popup = ClipboardPopup::new().context("create clipboard popup")?;
    window.set_product_name(env!("YT_SAVE_PRODUCT_NAME").into());
    window.set_cookie_support_enabled(cookie_support_enabled);
    window.set_cookie_support_status(cookie_support_status.into());
    window.set_app_version(env!("YT_SAVE_PRODUCT_VERSION").into());
    window.set_download_directory(download_directory_text.into());
    window.set_audio_only(download_presets.mode == DownloadMode::AudioOnly);
    window.set_video_only(download_presets.mode == DownloadMode::VideoOnly);
    window.set_subtitle_only(download_presets.mode == DownloadMode::SubtitlesOnly);
    window.set_metadata_only(download_presets.mode == DownloadMode::MetadataOnly);
    window.set_thumbnail_only(download_presets.mode == DownloadMode::ThumbnailOnly);
    window.set_audio_format(download_presets.audio_format.as_database_value().into());
    window.set_audio_quality(download_presets.audio_quality.as_database_value().into());
    window.set_video_quality(download_presets.video_quality.label().into());
    window.set_max_concurrent_downloads(max_concurrent_downloads.to_string().into());
    window.set_max_concurrent_per_host(max_concurrent_per_host.to_string().into());
    window.set_concurrent_fragments(concurrent_fragments.to_string().into());
    window.set_download_speed_limit(
        download_speed_limit_label(download_presets.download_speed_limit_bytes_per_second).into(),
    );
    window.set_saved_download_preset_names(saved_download_preset_model(&saved_download_presets));
    window.set_auto_retry_attempts(
        if auto_retry_attempts == 0 { "Off".to_owned() } else { auto_retry_attempts.to_string() }
            .into(),
    );
    window.set_clipboard_monitoring_enabled(false);
    window.set_app_update_channel(app_update_channel.label().into());
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
    let saved_queue_actions = queue_action_state(&saved_downloads);
    window.set_can_pause_all(saved_queue_actions.can_pause_all);
    window.set_can_resume_all(saved_queue_actions.can_resume_all);
    window.set_can_cancel_all(saved_queue_actions.can_cancel_all);
    window.set_can_retry_failed(saved_queue_actions.can_retry_failed);
    window.set_downloads(ModelRc::new(VecModel::from(saved_rows)));
    let event_initial_records = saved_downloads;

    let (events_tx, mut events_rx) = mpsc::unbounded_channel::<UiEvent>();
    let cookie_vault_generation = Arc::new(AtomicU64::new(0));
    #[cfg(windows)]
    let cookie_login_session = Arc::new(AtomicU64::new(0));
    let clipboard_monitor_slot = Arc::new(Mutex::new(None::<ClipboardMonitor>));
    let clipboard_monitor_generation = Arc::new(AtomicU64::new(0));
    let clipboard_popup_generation = Arc::new(AtomicU64::new(0));
    let clipboard_popup_work_area = Arc::new(Mutex::new(None::<WorkArea>));
    #[cfg(windows)]
    clipboard_popup::configure_clipboard_popup_window(&popup, clipboard_popup_work_area.clone());
    let shutdown_started = Arc::new(AtomicBool::new(false));
    let shutdown_committed = Arc::new(AtomicBool::new(false));
    let playlist_selection = Arc::new(Mutex::new(None::<PlaylistSelectionState>));
    let playlist_subscription_review =
        Arc::new(Mutex::new(PlaylistSubscriptionReviewState::default()));
    let playlist_subscription_check_active = Arc::new(AtomicBool::new(false));
    let weak_window = window.as_weak();
    let weak_tray = tray.as_weak();
    let event_store = store.clone();
    let event_playlist_selection = playlist_selection.clone();
    let event_clipboard_generation = clipboard_monitor_generation.clone();
    let event_popup = popup.as_weak();
    let event_popup_generation = clipboard_popup_generation.clone();
    let event_popup_work_area = clipboard_popup_work_area.clone();
    let event_library_events = events_tx.clone();
    let event_cookie_vault_generation = cookie_vault_generation.clone();
    #[cfg(windows)]
    let event_cookie_login_window = cookie_login_window.as_weak();
    #[cfg(windows)]
    let event_cookie_login_session = cookie_login_session.clone();
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
                        let queue_actions = queue_action_state(&cached_downloads);
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
                                window.set_can_pause_all(queue_actions.can_pause_all);
                                window.set_can_resume_all(queue_actions.can_resume_all);
                                window.set_can_cancel_all(queue_actions.can_cancel_all);
                                window.set_can_retry_failed(queue_actions.can_retry_failed);
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
                UiEvent::CookieAuthState {
                    generation,
                    url,
                    has_saved,
                    message,
                    close_login,
                    login_session,
                    refresh_current_url,
                } => {
                    let weak = weak_window.clone();
                    let active_generation = event_cookie_vault_generation.clone();
                    #[cfg(windows)]
                    let weak_login = event_cookie_login_window.clone();
                    #[cfg(windows)]
                    let active_login_session = event_cookie_login_session.clone();
                    #[cfg(not(windows))]
                    let _ = login_session;
                    let _ = slint::invoke_from_event_loop(move || {
                        #[cfg(windows)]
                        if close_login
                            && login_session.is_some_and(|session| {
                                active_login_session.load(Ordering::Acquire) == session
                            })
                            && let Some(login) = weak_login.upgrade()
                        {
                            login.set_saving(false);
                            if let Err(error) = login.hide() {
                                error!(%error, "close embedded sign-in window");
                            }
                        }

                        if active_generation.load(Ordering::Acquire) != generation {
                            return;
                        }
                        if let Some(window) = weak.upgrade()
                            && window.get_url_input().as_str() == url
                        {
                            if let Some(has_saved) = has_saved {
                                window.set_has_saved_cookies(has_saved);
                            }
                            if let Some(message) = message {
                                window.set_status_message(message.into());
                            }
                            if refresh_current_url {
                                let current_url = window.get_url_input();
                                window.invoke_cookie_vault_state_requested(current_url);
                            }
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
                            let main_window_is_focused = clipboard_popup::main_window_has_focus(&window);
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
                                    popup.set_source_host(clipboard_popup::clipboard_source_host(&candidate.url).into());
                                    popup.window().set_size(slint::LogicalSize::new(440.0, 214.0));
                                    let scale = popup.window().scale_factor();
                                    let (x, y) = clipboard_popup::initial_popup_position(area, scale, 440, 214);
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
                UiEvent::DependencyUpdateState(in_progress) => {
                    let weak = weak_window.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_yt_dlp_update_in_progress(in_progress);
                        }
                    });
                }
                UiEvent::AppUpdateView(state) => {
                    let weak = weak_window.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_app_update_channel(state.channel.label().into());
                            window.set_app_update_status(state.status.into());
                            window.set_app_update_release_notes(state.release_notes.into());
                            window.set_app_update_release_url(state.release_url.into());
                            window.set_app_update_available(state.available);
                            window.set_app_update_checking(state.checking);
                            window.set_app_update_downloading(state.downloading);
                            window.set_app_update_install_ready(state.install_ready);
                            window.set_app_update_installing(state.installing);
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
                            window.set_analyzed_description(SharedString::default());
                            window.set_analyzed_chapters(SharedString::default());
                            window.set_analyzed_is_playlist(false);
                            window.set_format_choices(ModelRc::new(VecModel::from(vec![
                                SharedString::from(AUTOMATIC_FORMAT_CHOICE),
                            ])));
                            window.set_selected_format(SharedString::from(AUTOMATIC_FORMAT_CHOICE));
                            window.set_format_choice_mode(SharedString::default());
                            window.set_format_choice_source_url(SharedString::default());
                            window.set_format_choice_display_url(SharedString::default());
                            window.set_format_choice_list_truncated(false);
                            window.set_playlist_entries(ModelRc::new(VecModel::from(Vec::new())));
                            window.set_selected_playlist_count(0);
                            window.set_selectable_playlist_count(0);
                            window.set_playlist_selection_summary(SharedString::default());
                            window.set_show_playlist_range_controls(false);
                            window.set_playlist_page_loading(false);
                            window.set_playlist_has_more_entries(false);
                        }
                    });
                }
                UiEvent::Metadata {
                    request_id,
                    source_url,
                    display_url,
                    selection_mode,
                    metadata,
                } => {
                    if request_id != latest_analysis_id {
                        continue;
                    }
                    let metadata = *metadata;
                    let format_choices = media_format_choice_labels(&metadata.formats, selection_mode);
                    let format_choice_mode = selection_mode.as_database_value().to_owned();
                    let format_choice_source_url = source_url.clone();
                    let format_choice_display_url = display_url.clone();
                    let format_choice_list_truncated = metadata.format_count > 100;
                    let format_summary = if metadata.is_playlist {
                        String::new()
                    } else {
                        format!(" · {} formats", metadata.format_count)
                    };
                    let playlist_summary = metadata.playlist_count.map_or(String::new(), |count| {
                        format!(" · {count} entries loaded")
                    });
                    let subtitle_summary = if metadata.is_playlist {
                        String::new()
                    } else {
                        let manual = if metadata.manual_subtitle_languages.is_empty() {
                            "none".to_owned()
                        } else {
                            metadata.manual_subtitle_languages.join(", ")
                        };
                        let automatic = if metadata.automatic_subtitle_languages.is_empty() {
                            "none".to_owned()
                        } else {
                            metadata.automatic_subtitle_languages.join(", ")
                        };
                        format!(" · captions: manual {manual}; auto {automatic}")
                    };
                    let creator_summary = metadata
                        .creator
                        .as_ref()
                        .map(|creator| format!(" · {creator}"))
                        .unwrap_or_default();
                    let upload_date_summary = metadata
                        .upload_date
                        .as_ref()
                        .map(|date| format!(" · Uploaded {date}"))
                        .unwrap_or_default();
                    let details = format!(
                        "{}{} · {}{}{}{}{}",
                        metadata.source,
                        creator_summary,
                        format_duration(metadata.duration_seconds),
                        upload_date_summary,
                        format_summary,
                        playlist_summary,
                        subtitle_summary,
                    );
                    let description = metadata
                        .description
                        .as_ref()
                        .map(|description| format!("Description · {description}"))
                        .unwrap_or_default();
                    let mut chapter_summaries = metadata
                        .chapters
                        .iter()
                        .take(6)
                        .map(|chapter| {
                            format!(
                                "{} · {}",
                                format_duration(Some(chapter.start_time)),
                                chapter.title
                            )
                        })
                        .collect::<Vec<_>>();
                    if metadata.chapters.len() > chapter_summaries.len() {
                        chapter_summaries.push(format!(
                            "{} more chapters",
                            metadata.chapters.len() - chapter_summaries.len()
                        ));
                    }
                    let chapters = if chapter_summaries.is_empty() {
                        String::new()
                    } else {
                        format!("Chapters · {}", chapter_summaries.join("  •  "))
                    };
                    let weak = weak_window.clone();
                    let title = metadata.title;
                    let is_playlist = metadata.is_playlist;
                    let preview_state = if is_playlist {
                        let entries = metadata.playlist_entries;
                        let loaded_count = metadata.playlist_count.unwrap_or(entries.len());
                        Some(PlaylistSelectionState {
                            request_id,
                            source_url,
                            source: metadata.source,
                            title: title.clone(),
                            selected: vec![false; entries.len()],
                            entries,
                            next_page_start: PLAYLIST_PAGE_SIZE.saturating_add(1),
                            has_more_entries: loaded_count >= PLAYLIST_PAGE_SIZE,
                            loading_page: false,
                        })
                    } else {
                        None
                    };
                    let preview_rows =
                        preview_state.as_ref().map(playlist_entry_rows).unwrap_or_default();
                    let selectable_count =
                        preview_state.as_ref().map_or(0, PlaylistSelectionState::selectable_count);
                    let preview_summary =
                        preview_state.as_ref().map(playlist_selection_summary).unwrap_or_default();
                    let has_more_entries =
                        preview_state.as_ref().is_some_and(|state| state.has_more_entries);
                    if let Ok(mut state) = event_playlist_selection.lock() {
                        *state = preview_state;
                    }
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak.upgrade() {
                            window.set_analyzed_title(title.into());
                            window.set_analyzed_details(details.into());
                            window.set_analyzed_description(description.into());
                            window.set_analyzed_chapters(chapters.into());
                            window.set_analyzed_is_playlist(is_playlist);
                            window.set_format_choices(ModelRc::new(VecModel::from(format_choices)));
                            window.set_selected_format(SharedString::from(AUTOMATIC_FORMAT_CHOICE));
                            window.set_format_choice_mode(format_choice_mode.into());
                            window.set_format_choice_source_url(format_choice_source_url.into());
                            window.set_format_choice_display_url(format_choice_display_url.into());
                            window.set_format_choice_list_truncated(format_choice_list_truncated);
                            window.set_playlist_entries(ModelRc::new(VecModel::from(preview_rows)));
                            window.set_selected_playlist_count(0);
                            window.set_selectable_playlist_count(selectable_count as i32);
                            window.set_show_playlist_range_controls(false);
                            window.set_playlist_selection_summary(preview_summary);
                            window.set_playlist_page_loading(false);
                            window.set_playlist_has_more_entries(has_more_entries);
                        }
                    });
                }
                UiEvent::PlaylistPage {
                    request_id,
                    start_index,
                    end_index,
                    select_range,
                    result,
                } => {
                    if request_id != latest_analysis_id {
                        continue;
                    }
                    let Ok(mut state) = event_playlist_selection.lock() else { continue };
                    let Some(selection) = state.as_mut() else { continue };
                    if selection.request_id != request_id {
                        continue;
                    }
                    selection.loading_page = false;
                    let status = match result {
                        Ok(metadata) => {
                            let metadata = *metadata;
                            let entry_count =
                                metadata.playlist_count.unwrap_or(metadata.playlist_entries.len());
                            merge_playlist_page(selection, metadata.playlist_entries);
                            if let Some((first, last)) = select_range {
                                let _ = select_playlist_range(selection, first, last);
                                let selected_in_range = selection
                                    .entries
                                    .iter()
                                    .zip(selection.selected.iter())
                                    .filter(|(entry, selected)| {
                                        **selected
                                            && entry.playlist_index.is_some_and(|index| {
                                                (first..=last).contains(&index)
                                            })
                                    })
                                    .count();
                                if start_index == selection.next_page_start {
                                    selection.next_page_start = end_index.saturating_add(1);
                                    selection.has_more_entries = entry_count
                                        >= end_index.saturating_sub(start_index).saturating_add(1);
                                }
                                if selected_in_range == 0 {
                                    format!(
                                        "No selectable playlist entries were returned for indexes {start_index}–{end_index}."
                                    )
                                } else {
                                    format!(
                                        "Selected {selected_in_range} playlist entries from indexes {start_index}–{end_index}."
                                    )
                                }
                            } else {
                                if start_index == selection.next_page_start {
                                    selection.next_page_start = end_index.saturating_add(1);
                                    selection.has_more_entries = entry_count
                                        >= end_index.saturating_sub(start_index).saturating_add(1);
                                }
                                format!(
                                    "Loaded playlist indexes {start_index}–{end_index}; {} selectable entries are loaded.",
                                    selection.selectable_count()
                                )
                            }
                        }
                        Err(error) => {
                            error!(start_index, end_index, %error, "load playlist entries");
                            format!("Could not load playlist entries: {error}")
                        }
                    };
                    let rows = playlist_entry_rows(selection);
                    let selected_count = selection.selected_count();
                    let selectable_count = selection.selectable_count();
                    let summary = playlist_selection_summary(selection);
                    let has_more = selection.has_more_entries;
                    drop(state);
                    if let Some(window) = weak_window.upgrade() {
                        window.set_playlist_entries(ModelRc::new(VecModel::from(rows)));
                        window.set_selected_playlist_count(selected_count as i32);
                        window.set_selectable_playlist_count(selectable_count as i32);
                        window.set_playlist_selection_summary(summary);
                        window.set_playlist_page_loading(false);
                        window.set_playlist_has_more_entries(has_more);
                        window.set_status_message(status.into());
                    }
                }
            }
        }
    });

    if clipboard_monitoring_enabled {
        match clipboard_popup::start_clipboard_monitor(
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
            match clipboard_popup::start_clipboard_monitor(
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
            match clipboard_popup::stop_clipboard_monitor(&clipboard_request_slot, &clipboard_request_generation) {
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

    #[cfg(windows)]
    cookie_login::configure_cookie_auth(CookieAuthContext {
        main_window: &window,
        login_window: &cookie_login_window,
        cookie_store: cookie_store.clone(),
        handle: handle.clone(),
        events: events_tx.clone(),
        generation: cookie_vault_generation.clone(),
        login_session: cookie_login_session.clone(),
        shutdown_started: shutdown_started.clone(),
    });

    let dependency_events = events_tx.clone();
    let mut dependency_paths = dependencies.clone();
    let dependency_engine = engine.clone();
    handle.spawn(async move {
        let _process_guard = match dependency_engine.lock_for_yt_dlp_process().await {
            Ok(guard) => guard,
            Err(error) => {
                error!(%error, "skip startup dependency probe while application update is starting");
                return;
            }
        };
        dependency_paths.yt_dlp = dependency_engine.current_yt_dlp_path();
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
    let analyze_window = window.as_weak();
    window.on_analyze_requested(move |submitted_url| {
        if analyze_shutdown_started.load(Ordering::Acquire) {
            let _ = analyze_events
                .send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        let request_id = analyze_generation.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = analyze_events.send(UiEvent::ClearPreview { request_id });
        let display_url = submitted_url.to_string();
        let url = match validate_url(&submitted_url) {
            Ok(url) => url,
            Err(message) => {
                let _ = analyze_events.send(UiEvent::Status(message));
                return;
            }
        };
        let selection_mode = analyze_window
            .upgrade()
            .map(|window| {
                if window.get_subtitle_only() {
                    DownloadMode::SubtitlesOnly
                } else if window.get_metadata_only() {
                    DownloadMode::MetadataOnly
                } else if window.get_thumbnail_only() {
                    DownloadMode::ThumbnailOnly
                } else if window.get_video_only() {
                    DownloadMode::VideoOnly
                } else if window.get_audio_only() {
                    DownloadMode::AudioOnly
                } else {
                    DownloadMode::Video
                }
            })
            .unwrap_or_default();
        let _ = analyze_events.send(UiEvent::Status("Reading media details…".to_owned()));
        let engine = analyze_engine.clone();
        let events = analyze_events.clone();
        analyze_handle.spawn(async move {
            match engine.inspect(&url).await {
                Ok(metadata) => {
                    let _ = events.send(UiEvent::Status("Media details loaded.".to_owned()));
                    let _ = events.send(UiEvent::Metadata {
                        request_id,
                        source_url: url,
                        display_url,
                        selection_mode,
                        metadata: Box::new(metadata),
                    });
                }
                Err(error) => {
                    error!(%error, "analyze media link");
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
    let update_dispatch_gate = Arc::new(AsyncMutex::new(()));
    let enqueue_dispatch_gate = update_dispatch_gate.clone();
    let batch_dispatch_gate = enqueue_dispatch_gate.clone();
    let batch_shutdown = enqueue_shutdown_started.clone();
    let _ = runtime.block_on(recover_pending(&store))?;
    let dispatcher_shutdown_started = shutdown_started.clone();
    let dispatcher_shutdown_committed = shutdown_committed.clone();
    handle.spawn(run_queue_dispatcher(
        store.clone(),
        engine.clone(),
        events_tx.clone(),
        queue_wake.clone(),
        active_jobs.clone(),
        update_dispatch_gate.clone(),
        ShutdownState {
            started: dispatcher_shutdown_started,
            committed: dispatcher_shutdown_committed,
        },
    ));

    let updater_running = Arc::new(AtomicBool::new(false));
    let update_events = events_tx.clone();
    let update_handle = handle.clone();
    let update_engine = engine.clone();
    let update_active_jobs = active_jobs.clone();
    let update_gate = update_dispatch_gate.clone();
    let update_shutdown_started = shutdown_started.clone();
    let update_dependencies = dependencies.clone();
    let update_running = updater_running.clone();
    window.on_yt_dlp_update_requested(move || {
        if update_shutdown_started.load(Ordering::Acquire) {
            let _ = update_events.send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        if update_running.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = update_events.send(UiEvent::Status("Checking the official yt-dlp stable release…".to_owned()));
        let events = update_events.clone();
        let engine = update_engine.clone();
        let active_jobs = update_active_jobs.clone();
        let dispatch_gate = update_gate.clone();
        let dependencies = update_dependencies.clone();
        let running = update_running.clone();
        let shutdown = update_shutdown_started.clone();
        update_handle.spawn(async move {
            let _ = events.send(UiEvent::DependencyUpdateState(true));
            let result = async {
                let _dispatch_guard = dispatch_gate.lock().await;
                if shutdown.load(Ordering::Acquire) {
                    anyhow::bail!("The application is shutting down before the yt-dlp update can start.");
                }
                let active_count = active_jobs
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active download state is unavailable"))?
                    .len();
                if active_count != 0 {
                    anyhow::bail!("Wait for active downloads to finish before updating yt-dlp.");
                }
                let _process_guard = engine.lock_for_yt_dlp_update().await;
                let current_executable = engine.current_yt_dlp_path();
                let user_directory = DependencyPaths::user_yt_dlp_directory()?;
                let outcome = crate::yt_dlp_update::check_and_install(
                    &current_executable,
                    &user_directory,
                )
                .await?;
                if let crate::yt_dlp_update::UpdateOutcome::Updated { installed_version, .. } =
                    &outcome
                {
                    let installed_path = user_directory
                        .join("versions")
                        .join(installed_version)
                        .join("yt-dlp.exe");
                    engine.use_yt_dlp_path(installed_path.clone());
                    let mut updated_dependencies = dependencies;
                    updated_dependencies.yt_dlp = installed_path;
                    let readiness = updated_dependencies.preflight().await;
                    let _ = events.send(UiEvent::DependencyStatus(readiness.summary()));
                }
                Ok::<_, anyhow::Error>(outcome)
            }
            .await;
            match result {
                Ok(crate::yt_dlp_update::UpdateOutcome::AlreadyCurrent {
                    installed_version,
                    latest_version,
                }) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "yt-dlp {installed_version} is current (latest stable: {latest_version})."
                    )));
                }
                Ok(crate::yt_dlp_update::UpdateOutcome::Updated {
                    previous_version,
                    installed_version,
                }) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "Updated yt-dlp from {previous_version} to {installed_version}. The previous per-user engine is retained for rollback."
                    )));
                    let _ = events.send(UiEvent::Refresh);
                }
                Err(error) => {
                    error!(%error, "check or install yt-dlp update");
                    let _ = events.send(UiEvent::Status(format!("yt-dlp update was not installed: {error}")));
                }
            }
            running.store(false, Ordering::Release);
            let _ = events.send(UiEvent::DependencyUpdateState(false));
        });
    });

    let app_update_release = Arc::new(Mutex::new(None::<AppRelease>));
    let app_update_installer = Arc::new(Mutex::new(None::<StagedAppInstaller>));
    let app_update_busy = Arc::new(AtomicBool::new(false));
    let app_update_helper = Arc::new(Mutex::new(None::<crate::app_update::UpdateHelper>));

    let channel_state = app_update_channel_state.clone();
    let channel_release = app_update_release.clone();
    let channel_installer = app_update_installer.clone();
    let channel_store = store.clone();
    let channel_events = events_tx.clone();
    let channel_handle = handle.clone();
    let channel_busy = app_update_busy.clone();
    window.on_app_update_channel_requested(move |selected| {
        if channel_busy.load(Ordering::Acquire) {
            return;
        }
        let next = AppUpdateChannel::from_database_value(Some(if selected.as_str() == "Preview" {
            "preview"
        } else {
            "stable"
        }));
        let previous = match channel_state.lock() {
            Ok(mut state) => {
                let previous = *state;
                *state = next;
                previous
            }
            Err(_) => {
                let _ = channel_events.send(UiEvent::Status(
                    "Application update settings are unavailable.".to_owned(),
                ));
                return;
            }
        };
        if previous == next {
            return;
        }
        if let Ok(mut release) = channel_release.lock() {
            *release = None;
        }
        if let Ok(mut installer) = channel_installer.lock() {
            *installer = None;
        }
        let mut view = AppUpdateUiState::idle(
            next,
            format!(
                "{} channel selected. Check for updates to see available releases.",
                next.label()
            ),
        );
        view.available = false;
        let _ = channel_events.send(UiEvent::AppUpdateView(view));
        let store = channel_store.clone();
        let events = channel_events.clone();
        channel_handle.spawn(async move {
            if let Err(error) = store.set_app_update_channel(next, now()).await {
                error!(%error, "save application update channel");
                let mut view = AppUpdateUiState::idle(
                    next,
                    format!("Channel selected for this session but could not be saved: {error}"),
                );
                view.available = false;
                let _ = events.send(UiEvent::AppUpdateView(view));
            }
        });
    });

    let check_channel_state = app_update_channel_state.clone();
    let check_release_state = app_update_release.clone();
    let check_installer_state = app_update_installer.clone();
    let check_busy = app_update_busy.clone();
    let check_handle = handle.clone();
    let check_events = events_tx.clone();
    window.on_app_update_check_requested(move || {
        if check_busy.swap(true, Ordering::AcqRel) {
            return;
        }
        let channel = match check_channel_state.lock() {
            Ok(channel) => *channel,
            Err(_) => {
                check_busy.store(false, Ordering::Release);
                let _ = check_events.send(UiEvent::Status(
                    "Application update settings are unavailable.".to_owned(),
                ));
                return;
            }
        };
        if let Ok(mut release) = check_release_state.lock() {
            *release = None;
        }
        if let Ok(mut installer) = check_installer_state.lock() {
            *installer = None;
        }
        let mut view = AppUpdateUiState::idle(channel, "Checking the official Yt Save releases…");
        view.checking = true;
        let _ = check_events.send(UiEvent::AppUpdateView(view));
        let events = check_events.clone();
        let release_state = check_release_state.clone();
        let running = check_busy.clone();
        check_handle.spawn(async move {
            let result = crate::app_update::check_latest(
                channel,
                env!("YT_SAVE_PRODUCT_VERSION"),
            )
            .await;
            let view = match result {
                Ok(CheckOutcome::UpToDate { current_version, release }) => {
                    let version = release.version.to_string();
                    let mut view = AppUpdateUiState::idle(
                        channel,
                        format!("Yt Save {current_version} is current on the {} channel.", channel.label()),
                    );
                    view.release_notes = app_update_notes_preview(&release.body);
                    view.release_url = release.html_url.to_string();
                    view.available = false;
                    if version != current_version.to_string() {
                        view.status = format!("Latest {} release is {version}; this app is {current_version}.", channel.label());
                    }
                    if let Ok(mut state) = release_state.lock() {
                        *state = Some(release);
                    }
                    view
                }
                Ok(CheckOutcome::UpdateAvailable { current_version, release }) => {
                    let mut view = AppUpdateUiState::idle(
                        channel,
                        format!("Yt Save {} is available (installed: {current_version}). Review the notes, then download the matching installer.", release.version),
                    );
                    view.release_notes = app_update_notes_preview(&release.body);
                    view.release_url = release.html_url.to_string();
                    view.available = true;
                    if let Ok(mut state) = release_state.lock() {
                        *state = Some(release);
                    }
                    view
                }
                Err(error) => {
                    error!(%error, "check application update");
                    AppUpdateUiState::idle(channel, format!("Could not check for application updates: {error}"))
                }
            };
            running.store(false, Ordering::Release);
            let _ = events.send(UiEvent::AppUpdateView(view));
        });
    });

    let download_release_state = app_update_release.clone();
    let download_installer_state = app_update_installer.clone();
    let download_channel_state = app_update_channel_state.clone();
    let download_busy = app_update_busy.clone();
    let download_handle = handle.clone();
    let download_events = events_tx.clone();
    window.on_app_update_download_requested(move || {
        if download_busy.swap(true, Ordering::AcqRel) {
            return;
        }
        let channel = download_channel_state
            .lock()
            .map(|channel| *channel)
            .unwrap_or(AppUpdateChannel::Stable);
        let release = download_release_state.lock().ok().and_then(|state| state.clone());
        let Some(release) = release else {
            download_busy.store(false, Ordering::Release);
            let _ = download_events.send(UiEvent::AppUpdateView(AppUpdateUiState::idle(
                channel,
                "Check for updates again before downloading an installer.",
            )));
            return;
        };
        let mut view = AppUpdateUiState::idle(
            channel,
            format!("Preparing and downloading the matching {} installer…", release.version),
        );
        view.available = true;
        view.release_notes = app_update_notes_preview(&release.body);
        view.release_url = release.html_url.to_string();
        view.downloading = true;
        let _ = download_events.send(UiEvent::AppUpdateView(view));
        let events = download_events.clone();
        let running = download_busy.clone();
        let staged_state = download_installer_state.clone();
        download_handle.spawn(async move {
            let result = async {
                let kind = tokio::task::spawn_blocking(crate::app_update::detect_installed_installer)
                    .await
                    .context("join installed-package detection")??;
                let cache_dir = app_update_cache_directory()?;
                let path = crate::app_update::download_verified_installer(&release, kind, &cache_dir).await?;
                crate::app_update::verify_cached_installer(&release, kind, &path)?;
                Ok::<_, anyhow::Error>((kind, path))
            }
            .await;
            let view = match result {
                Ok((kind, path)) => {
                    let label = installer_kind_label(kind);
                    if let Ok(mut staged) = staged_state.lock() {
                        *staged = Some(StagedAppInstaller { path, kind });
                    }
                    let mut view = AppUpdateUiState::idle(
                        channel,
                        format!("Verified the {} {} installer. Click Install verified update when ready; downloads will be checked again before handoff.", release.version, label),
                    );
                    view.available = true;
                    view.install_ready = true;
                    view.release_notes = app_update_notes_preview(&release.body);
                    view.release_url = release.html_url.to_string();
                    view
                }
                Err(error) => {
                    error!(%error, "stage verified application installer");
                    let mut view = AppUpdateUiState::idle(
                        channel,
                        format!("The installer was not staged: {error}"),
                    );
                    view.available = true;
                    view.release_notes = app_update_notes_preview(&release.body);
                    view.release_url = release.html_url.to_string();
                    view
                }
            };
            running.store(false, Ordering::Release);
            let _ = events.send(UiEvent::AppUpdateView(view));
        });
    });

    let install_release_state = app_update_release.clone();
    let install_installer_state = app_update_installer.clone();
    let install_channel_state = app_update_channel_state.clone();
    let install_busy = app_update_busy.clone();
    let install_handle = handle.clone();
    let install_events = events_tx.clone();
    let install_gate = update_dispatch_gate.clone();
    let install_helper_state = app_update_helper.clone();
    let install_active_jobs = active_jobs.clone();
    let install_shutdown = shutdown_started.clone();
    let install_shutdown_committed = shutdown_committed.clone();
    let install_queue_wake = queue_wake.clone();
    let install_store = store.clone();
    let install_engine = engine.clone();
    let install_window = window.as_weak();
    window.on_app_update_install_requested(move || {
        if install_busy.swap(true, Ordering::AcqRel) {
            return;
        }
        let channel = install_channel_state
            .lock()
            .map(|channel| *channel)
            .unwrap_or(AppUpdateChannel::Stable);
        if install_shutdown.load(Ordering::Acquire) {
            let has_helper = install_helper_state
                .lock()
                .is_ok_and(|state| state.is_some());
            if !has_helper {
                install_busy.store(false, Ordering::Release);
                let _ = install_events.send(UiEvent::AppUpdateView(AppUpdateUiState::idle(
                    channel,
                    "Yt Save is already shutting down and cannot start another update.",
                )));
                return;
            }
            let helper_state = install_helper_state.clone();
            let shutdown = install_shutdown.clone();
            let shutdown_committed = install_shutdown_committed.clone();
            let queue_wake = install_queue_wake.clone();
            let engine = install_engine.clone();
            let running = install_busy.clone();
            let events = install_events.clone();
            install_handle.spawn(async move {
                let result = tokio::task::spawn_blocking(move || {
                    cancel_staged_update_helper(&helper_state)
                })
                .await
                .context("join updater helper cancellation retry")
                .and_then(|result| result);
                match result {
                    Ok(()) => {
                        shutdown_committed.store(false, Ordering::Release);
                        shutdown.store(false, Ordering::Release);
                        queue_wake.notify_one();
                        engine.resume_accepting_yt_dlp_processes();
                        running.store(false, Ordering::Release);
                        let mut view = AppUpdateUiState::idle(
                            channel,
                            "The updater helper is confirmed stopped. Downloads can resume; retry the installation when ready.",
                        );
                        view.available = true;
                        view.install_ready = true;
                        let _ = events.send(UiEvent::AppUpdateView(view));
                    }
                    Err(error) => {
                        error!(%error, "retry updater helper cancellation");
                        running.store(false, Ordering::Release);
                        let mut view = AppUpdateUiState::idle(
                            channel,
                            format!("Safe shutdown mode remains active because the updater helper could not be confirmed stopped: {error}. Keep Yt Save open and retry this action."),
                        );
                        view.available = true;
                        view.install_ready = true;
                        let _ = events.send(UiEvent::AppUpdateView(view));
                    }
                }
            });
            return;
        }
        let release = install_release_state.lock().ok().and_then(|state| state.clone());
        let staged = install_installer_state.lock().ok().and_then(|state| state.clone());
        let (Some(release), Some(staged)) = (release, staged) else {
            install_busy.store(false, Ordering::Release);
            let _ = install_events.send(UiEvent::AppUpdateView(AppUpdateUiState::idle(
                channel,
                "Download and verify the update before installing it.",
            )));
            return;
        };
        if install_shutdown.swap(true, Ordering::AcqRel) {
            install_busy.store(false, Ordering::Release);
            let _ = install_events.send(UiEvent::AppUpdateView(AppUpdateUiState::idle(
                channel,
                "Yt Save is already shutting down and cannot start another update.",
            )));
            return;
        }
        let mut view = AppUpdateUiState::idle(channel, "Preparing a safe application update handoff…");
        view.available = true;
        view.install_ready = true;
        view.installing = true;
        view.release_notes = app_update_notes_preview(&release.body);
        view.release_url = release.html_url.to_string();
        let _ = install_events.send(UiEvent::AppUpdateView(view));
        let events = install_events.clone();
        let running = install_busy.clone();
        let gate = install_gate.clone();
        let active_jobs = install_active_jobs.clone();
        let shutdown = install_shutdown.clone();
        let shutdown_committed = install_shutdown_committed.clone();
        let queue_wake = install_queue_wake.clone();
        let helper_state = install_helper_state.clone();
        let helper_stop_failed = Arc::new(AtomicBool::new(false));
        let prepare_helper_stop_failed = helper_stop_failed.clone();
        let store = install_store.clone();
        let engine = install_engine.clone();
        let weak_window = install_window.clone();
        let event_handle = install_handle.clone();
        install_handle.spawn(async move {
            let result = async {
                let _dispatch_guard = gate.lock().await;
                let active_count = active_jobs
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active download state is unavailable"))?
                    .len();
                if active_count != 0 {
                    anyhow::bail!("Finish or safely pause all active downloads before installing an application update.");
                }
                let saved_records = store.list_downloads().await?;
                if saved_records.iter().any(|record| is_active_status(&record.status)) {
                    anyhow::bail!("The saved queue still contains an active download; wait for it to finish before updating.");
                }
                engine.stop_accepting_yt_dlp_processes();
                let _process_guard = tokio::time::timeout(
                    Duration::from_secs(120),
                    engine.lock_for_yt_dlp_update(),
                )
                .await
                .context("yt-dlp operations did not finish within two minutes; the update was canceled")?;
                let verify_release = release.clone();
                let verify_path = staged.path.clone();
                tokio::task::spawn_blocking(move || {
                    crate::app_update::verify_cached_installer(
                        &verify_release,
                        staged.kind,
                        &verify_path,
                    )
                })
                .await
                .context("join cached application installer verification")??;
                let app_path = std::env::current_exe().context("locate the installed application")?;
                let cache_dir = app_update_cache_directory()?;
                let installer = staged.path.clone();
                let helper = tokio::task::spawn_blocking(move || {
                    crate::app_update::launch_update_helper(
                        &installer,
                        staged.kind,
                        &app_path,
                        &cache_dir,
                    )
                })
                .await
                .context("join application updater helper startup")??;
                let helper_ready = helper.is_ready();
                *helper_state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("updater helper state is unavailable"))? =
                    Some(helper);
                if !helper_ready {
                    prepare_helper_stop_failed.store(true, Ordering::Release);
                    queue_wake.notify_one();
                    anyhow::bail!("the updater helper did not become ready and could not be confirmed stopped; retry cancellation while keeping Yt Save open");
                }
                queue_wake.notify_one();
                if let Err(error) = pause_all_downloads(&store, &active_jobs, false).await {
                    let cancel_state = helper_state.clone();
                    let helper_stop_error =
                        tokio::task::spawn_blocking(move || cancel_staged_update_helper(&cancel_state))
                            .await
                            .context("join updater helper cancellation")?
                            .err();
                    if let Some(helper_stop_error) = helper_stop_error {
                        prepare_helper_stop_failed.store(true, Ordering::Release);
                        return Err(error).context(format!(
                            "preserve the queue before application update; updater helper stop failed: {helper_stop_error:#}"
                        ));
                    }
                    shutdown.store(false, Ordering::Release);
                    shutdown_committed.store(false, Ordering::Release);
                    return Err(error).context("preserve the queue before application update");
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            match result {
                Ok(()) => {
                    let event_helper_state = helper_state.clone();
                    let event_shutdown = shutdown.clone();
                    let event_shutdown_committed = shutdown_committed.clone();
                    let event_running = running.clone();
                    let event_events = events.clone();
                    let event_engine = engine.clone();
                    let event_queue_wake = queue_wake.clone();
                    let event_handle = event_handle.clone();
                    let invoke_result = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak_window.upgrade()
                            && let Err(error) = window.hide()
                        {
                            error!(%error, "hide window for application update");
                        }
                        if let Err(error) = slint::quit_event_loop() {
                            error!(%error, "hand off application update");
                            let helper_state = event_helper_state.clone();
                            let shutdown = event_shutdown.clone();
                            let shutdown_committed = event_shutdown_committed.clone();
                            let queue_wake = event_queue_wake.clone();
                            let engine = event_engine.clone();
                            let running = event_running.clone();
                            let events = event_events.clone();
                            let window = weak_window.clone();
                            event_handle.spawn(async move {
                                let stop_result = tokio::task::spawn_blocking(move || {
                                    cancel_staged_update_helper(&helper_state)
                                })
                                .await
                                .context("join updater helper cancellation after failed handoff")
                                .and_then(|result| result);
                                if stop_result.is_err() {
                                } else {
                                    shutdown.store(false, Ordering::Release);
                                    shutdown_committed.store(false, Ordering::Release);
                                    queue_wake.notify_one();
                                    engine.resume_accepting_yt_dlp_processes();
                                }
                                running.store(false, Ordering::Release);
                                let message = match stop_result {
                                    Ok(()) => format!("The update could not be started: {error}. The installed application is still available."),
                                    Err(stop_error) => format!("Safe shutdown mode remains active because the update handoff failed ({error}) and the updater helper could not be confirmed stopped: {stop_error}. Keep Yt Save open and retry this action."),
                                };
                                let mut view = AppUpdateUiState::idle(channel, message);
                                view.available = true;
                                view.install_ready = true;
                                let _ = events.send(UiEvent::AppUpdateView(view));
                                if let Err(show_error) = slint::invoke_from_event_loop(move || {
                                    if let Some(window) = window.upgrade()
                                        && let Err(show_error) = window.show()
                                    {
                                        error!(%show_error, "restore window after update handoff failure");
                                    }
                                }) {
                                    error!(%show_error, "schedule window restore after update handoff failure");
                                }
                            });
                            return;
                        }
                        event_shutdown_committed.store(true, Ordering::Release);
                        event_queue_wake.notify_one();
                    });
                    if let Err(error) = invoke_result {
                        let cancel_state = helper_state.clone();
                        let helper_stop_error = tokio::task::spawn_blocking(move || {
                            cancel_staged_update_helper(&cancel_state)
                        })
                        .await
                        .context("join updater helper cancellation after event-loop dispatch failure")
                        .and_then(|result| result)
                        .err();
                        if let Some(stop_error) = &helper_stop_error
                        {
                            error!(%stop_error, "stop updater helper after failed event-loop handoff");
                        }
                        if helper_stop_error.is_none() {
                            shutdown.store(false, Ordering::Release);
                            shutdown_committed.store(false, Ordering::Release);
                            queue_wake.notify_one();
                            engine.resume_accepting_yt_dlp_processes();
                        }
                        running.store(false, Ordering::Release);
                        let status = helper_stop_error.map_or_else(
                            || format!("Could not hand off to the installer: {error}"),
                            |stop_error| format!("Safe shutdown mode remains active because the installer handoff failed ({error}) and the updater helper could not be confirmed stopped: {stop_error}. Keep Yt Save open and retry this action."),
                        );
                        let _ = events.send(UiEvent::AppUpdateView(AppUpdateUiState::idle(
                            channel,
                            status,
                        )));
                    }
                }
                Err(error) => {
                    error!(%error, "prepare application update handoff");
                    let helper_stop_failed = prepare_helper_stop_failed.load(Ordering::Acquire);
                    if !helper_stop_failed {
                        shutdown.store(false, Ordering::Release);
                        shutdown_committed.store(false, Ordering::Release);
                        queue_wake.notify_one();
                        engine.resume_accepting_yt_dlp_processes();
                    }
                    running.store(false, Ordering::Release);
                    let mut view = AppUpdateUiState::idle(
                        channel,
                        if helper_stop_failed {
                            format!("Safe shutdown mode remains active because the update was not started ({error}) and the updater helper could not be confirmed stopped. Keep Yt Save open and retry this action.")
                        } else {
                            format!("The update was not started: {error}")
                        },
                    );
                    view.available = true;
                    view.install_ready = true;
                    view.release_notes = app_update_notes_preview(&release.body);
                    view.release_url = release.html_url.to_string();
                    let _ = events.send(UiEvent::AppUpdateView(view));
                }
            }
        });
    });

    let notes_release_state = app_update_release.clone();
    let notes_events = events_tx.clone();
    window.on_app_update_notes_requested(move || {
        let url = notes_release_state
            .lock()
            .ok()
            .and_then(|state| state.as_ref().map(|release| release.html_url.to_string()));
        if let Some(url) = url
            && let Err(error) = open_release_page(&url)
        {
            error!(%error, "open application release notes");
            let _ = notes_events
                .send(UiEvent::Status(format!("Could not open the release page: {error}")));
        }
    });

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

    let subscription_view_store = store.clone();
    let subscription_view_review = playlist_subscription_review.clone();
    let subscription_view_window = window.as_weak();
    let subscription_view_handle = handle.clone();
    let subscription_view_events = events_tx.clone();
    window.on_subscriptions_view_requested(move |visible| {
        if !visible {
            return;
        }
        let store = subscription_view_store.clone();
        let review = subscription_view_review.clone();
        let weak_window = subscription_view_window.clone();
        let events = subscription_view_events.clone();
        let current_id =
            weak_window.upgrade().map(|window| window.get_selected_subscription_id().to_string());
        subscription_view_handle.spawn(async move {
            if let Err(error) =
                refresh_playlist_subscription_view(&store, &review, weak_window, current_id).await
            {
                error!(%error, "refresh playlist subscriptions");
                let _ = events.send(UiEvent::Status(format!(
                    "Could not load playlist subscriptions: {error}"
                )));
            }
        });
    });

    let refresh_library_events = events_tx.clone();
    window.on_library_refresh_requested(move || {
        let _ = refresh_library_events.send(UiEvent::RefreshLibrary);
    });

    let save_subscription_selection = playlist_selection.clone();
    let save_subscription_store = store.clone();
    let save_subscription_engine = engine.clone();
    let save_subscription_review = playlist_subscription_review.clone();
    let save_subscription_window = window.as_weak();
    let save_subscription_events = events_tx.clone();
    let save_subscription_wake = queue_wake.clone();
    let save_subscription_handle = handle.clone();
    let save_subscription_shutdown = shutdown_started.clone();
    let save_subscription_dispatch_gate = update_dispatch_gate.clone();
    let save_subscription_presets = current_download_presets.clone();
    let save_subscription_check_active = playlist_subscription_check_active.clone();
    window.on_subscribe_playlist_requested(move || {
        if save_subscription_shutdown.load(Ordering::Acquire) {
            let _ = save_subscription_events
                .send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        let selection = save_subscription_selection.lock().ok().and_then(|state| state.clone());
        let Some(selection) = selection else {
            let _ = save_subscription_events
                .send(UiEvent::Status("Analyze a playlist before saving a subscription.".to_owned()));
            return;
        };
        let Some(check_guard) = PlaylistSubscriptionCheckGuard::start(
            &save_subscription_check_active,
            save_subscription_window.clone(),
        ) else {
            let _ = save_subscription_events.send(UiEvent::Status(
                "A playlist check is already running. Wait for it to finish before saving another.".to_owned(),
            ));
            return;
        };
        let store = save_subscription_store.clone();
        let engine = save_subscription_engine.clone();
        let review = save_subscription_review.clone();
        let weak_window = save_subscription_window.clone();
        let events = save_subscription_events.clone();
        let queue_wake = save_subscription_wake.clone();
        let shutdown_started = save_subscription_shutdown.clone();
        let dispatch_gate = save_subscription_dispatch_gate.clone();
        let current_download_presets = save_subscription_presets.clone();
        save_subscription_handle.spawn(async move {
            let _check_guard = check_guard;
            match store.list_playlist_subscriptions().await {
                Ok(subscriptions) => {
                    if let Some(existing) = subscriptions
                        .iter()
                        .find(|subscription| subscription.url == selection.source_url)
                    {
                        let _ = events.send(UiEvent::Status(
                            "This playlist is already saved as a subscription.".to_owned(),
                        ));
                        if let Err(error) = refresh_playlist_subscription_view(
                            &store,
                            &review,
                            weak_window.clone(),
                            Some(existing.id.clone()),
                        )
                        .await
                        {
                            error!(%error, "refresh existing playlist subscription");
                        }
                        return;
                    }
                }
                Err(error) => {
                    error!(%error, "check existing playlist subscriptions");
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not check saved playlists: {error}"
                    )));
                    return;
                }
            }
            let subscription_id = Uuid::new_v4().to_string();
            if let Err(error) = store
                .create_playlist_subscription(
                    subscription_id.clone(),
                    selection.source_url,
                    selection.title,
                    selection.source,
                    now(),
                )
                .await
            {
                error!(%error, "save playlist subscription");
                let _ = events.send(UiEvent::Status(format!(
                    "Could not save this playlist subscription: {error}"
                )));
                return;
            }
            let weak = weak_window.clone();
            let selected_subscription_id = subscription_id.clone();
            let _ = slint::invoke_from_event_loop(move || {
                if let Some(window) = weak.upgrade() {
                    window.set_show_library(false);
                    window.set_selected_subscription_id(selected_subscription_id.into());
                    window.set_show_subscriptions(true);
                }
            });
            let _ = events.send(UiEvent::Status(
                "Saved the playlist. Checking its entries for your review…".to_owned(),
            ));
            match perform_playlist_subscription_check(
                &store,
                &engine,
                &subscription_id,
                &current_download_presets,
                &dispatch_gate,
                &shutdown_started,
            )
            .await
            {
                Ok(result) => {
                    let _ = events.send(UiEvent::Status(playlist_subscription_check_status(result)));
                    if result.auto_queued_entries > 0 {
                        queue_wake.notify_one();
                    }
                }
                Err(error) => {
                    if shutdown_started.load(Ordering::Acquire) {
                        return;
                    }
                    let _ = store
                        .record_playlist_subscription_error(subscription_id.clone(), error.to_string(), now())
                        .await;
                    error!(%error, subscription_id = %subscription_id, "check new playlist subscription");
                    let _ = events.send(UiEvent::Status(format!(
                        "Playlist saved, but its first check failed: {error}"
                    )));
                }
            }
            if let Err(error) = refresh_playlist_subscription_view(
                &store,
                &review,
                weak_window,
                Some(subscription_id),
            )
            .await
            {
                error!(%error, "refresh newly saved playlist subscription");
            }
        });
    });

    let selected_subscription_store = store.clone();
    let selected_subscription_review = playlist_subscription_review.clone();
    let selected_subscription_window = window.as_weak();
    let selected_subscription_events = events_tx.clone();
    let selected_subscription_handle = handle.clone();
    window.on_playlist_subscription_selected_requested(move |id| {
        let store = selected_subscription_store.clone();
        let review = selected_subscription_review.clone();
        let weak_window = selected_subscription_window.clone();
        let events = selected_subscription_events.clone();
        let id = id.to_string();
        selected_subscription_handle.spawn(async move {
            if let Err(error) =
                refresh_playlist_subscription_view(&store, &review, weak_window, Some(id)).await
            {
                error!(%error, "select playlist subscription");
                let _ = events.send(UiEvent::Status(format!(
                    "Could not load this playlist's archive: {error}"
                )));
            }
        });
    });

    let check_subscription_store = store.clone();
    let check_subscription_engine = engine.clone();
    let check_subscription_review = playlist_subscription_review.clone();
    let check_subscription_window = window.as_weak();
    let check_subscription_events = events_tx.clone();
    let check_subscription_wake = queue_wake.clone();
    let check_subscription_handle = handle.clone();
    let check_subscription_active = playlist_subscription_check_active.clone();
    let check_subscription_dispatch_gate = update_dispatch_gate.clone();
    let check_subscription_shutdown = shutdown_started.clone();
    let check_subscription_presets = current_download_presets.clone();
    window.on_playlist_subscription_check_requested(move |id| {
        if check_subscription_shutdown.load(Ordering::Acquire) {
            let _ = check_subscription_events
                .send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        let id = id.to_string();
        let store = check_subscription_store.clone();
        let engine = check_subscription_engine.clone();
        let review = check_subscription_review.clone();
        let weak_window = check_subscription_window.clone();
        let events = check_subscription_events.clone();
        let queue_wake = check_subscription_wake.clone();
        let dispatch_gate = check_subscription_dispatch_gate.clone();
        let shutdown_started = check_subscription_shutdown.clone();
        let current_download_presets = check_subscription_presets.clone();
        let Some(check_guard) = PlaylistSubscriptionCheckGuard::start(
            &check_subscription_active,
            check_subscription_window.clone(),
        ) else {
            let _ = events.send(UiEvent::Status(
                "A playlist check is already running. Wait for it to finish before checking again."
                    .to_owned(),
            ));
            return;
        };
        check_subscription_handle.spawn(async move {
            let _check_guard = check_guard;
            let _ =
                events.send(UiEvent::Status("Checking the playlist for new entries…".to_owned()));
            match perform_playlist_subscription_check(
                &store,
                &engine,
                &id,
                &current_download_presets,
                &dispatch_gate,
                &shutdown_started,
            )
            .await
            {
                Ok(result) => {
                    let _ =
                        events.send(UiEvent::Status(playlist_subscription_check_status(result)));
                    if result.auto_queued_entries > 0 {
                        queue_wake.notify_one();
                    }
                }
                Err(error) => {
                    if shutdown_started.load(Ordering::Acquire) {
                        return;
                    }
                    let _ = store
                        .record_playlist_subscription_error(id.clone(), error.to_string(), now())
                        .await;
                    error!(%error, subscription_id = %id, "check playlist subscription");
                    let _ = events
                        .send(UiEvent::Status(format!("Could not check this playlist: {error}")));
                }
            }
            if let Err(error) =
                refresh_playlist_subscription_view(&store, &review, weak_window, Some(id)).await
            {
                error!(%error, "refresh checked playlist subscription");
            }
        });
    });

    let frequency_subscription_store = store.clone();
    let frequency_subscription_review = playlist_subscription_review.clone();
    let frequency_subscription_window = window.as_weak();
    let frequency_subscription_events = events_tx.clone();
    let frequency_subscription_handle = handle.clone();
    window.on_playlist_subscription_frequency_requested(move |id, label| {
        let id = id.to_string();
        if id.is_empty() {
            return;
        }
        let label = label.to_string();
        let Some(interval_seconds) = subscription_check_frequency_seconds(&label) else {
            let _ = frequency_subscription_events
                .send(UiEvent::Status("Choose a supported playlist check frequency.".to_owned()));
            return;
        };
        let store = frequency_subscription_store.clone();
        let review = frequency_subscription_review.clone();
        let weak_window = frequency_subscription_window.clone();
        let events = frequency_subscription_events.clone();
        frequency_subscription_handle.spawn(async move {
            match store
                .set_playlist_subscription_check_interval(id.clone(), interval_seconds, now())
                .await
            {
                Ok(()) => {
                    let message = if interval_seconds == 0 {
                        "Automatic checks are off for this playlist.".to_owned()
                    } else {
                        format!(
                            "This playlist will be checked {} while Yt Save is running.",
                            label.to_lowercase()
                        )
                    };
                    let _ = events.send(UiEvent::Status(message));
                }
                Err(error) => {
                    error!(%error, subscription_id = %id, "update playlist check frequency");
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not update the playlist check frequency: {error}"
                    )));
                }
            }
            if let Err(error) =
                refresh_playlist_subscription_view(&store, &review, weak_window, Some(id)).await
            {
                error!(%error, "refresh playlist after changing its check frequency");
            }
        });
    });

    let auto_queue_subscription_store = store.clone();
    let auto_queue_subscription_review = playlist_subscription_review.clone();
    let auto_queue_subscription_window = window.as_weak();
    let auto_queue_subscription_events = events_tx.clone();
    let auto_queue_subscription_handle = handle.clone();
    window.on_playlist_subscription_auto_queue_requested(move |id, enabled| {
        let id = id.to_string();
        if id.is_empty() {
            return;
        }
        let store = auto_queue_subscription_store.clone();
        let review = auto_queue_subscription_review.clone();
        let weak_window = auto_queue_subscription_window.clone();
        let events = auto_queue_subscription_events.clone();
        auto_queue_subscription_handle.spawn(async move {
            match store
                .set_playlist_subscription_auto_queue_new_entries(id.clone(), enabled)
                .await
            {
                Ok(()) => {
                    let message = if enabled {
                        "New available entries for this playlist will be auto-queued using the current New Download choices and save folder."
                    } else {
                        "New entries for this playlist will wait for your review."
                    };
                    let _ = events.send(UiEvent::Status(message.to_owned()));
                }
                Err(error) => {
                    error!(%error, subscription_id = %id, "update playlist auto-queue preference");
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not update the playlist auto-queue setting: {error}"
                    )));
                }
            }
            if let Err(error) =
                refresh_playlist_subscription_view(&store, &review, weak_window, Some(id)).await
            {
                error!(%error, "refresh playlist after changing auto-queue setting");
            }
        });
    });

    let delete_subscription_store = store.clone();
    let delete_subscription_review = playlist_subscription_review.clone();
    let delete_subscription_window = window.as_weak();
    let delete_subscription_events = events_tx.clone();
    let delete_subscription_handle = handle.clone();
    window.on_playlist_subscription_delete_requested(move |id| {
        let id = id.to_string();
        let store = delete_subscription_store.clone();
        let review = delete_subscription_review.clone();
        let weak_window = delete_subscription_window.clone();
        let events = delete_subscription_events.clone();
        delete_subscription_handle.spawn(async move {
            match store.delete_playlist_subscription(id.clone()).await {
                Ok(true) => {
                    let _ = events.send(UiEvent::Status(
                        "Removed the saved playlist and its tracking archive. Downloaded files and queue history were kept.".to_owned(),
                    ));
                    if let Err(error) = refresh_playlist_subscription_view(&store, &review, weak_window, None).await {
                        error!(%error, "refresh subscriptions after removal");
                    }
                }
                Ok(false) => {
                    let _ = events.send(UiEvent::Status("That playlist subscription no longer exists.".to_owned()));
                }
                Err(error) => {
                    error!(%error, subscription_id = %id, "remove playlist subscription");
                    let _ = events.send(UiEvent::Status(format!("Could not remove the subscription: {error}")));
                }
            }
        });
    });

    let toggle_subscription_review = playlist_subscription_review.clone();
    let toggle_subscription_window = window.as_weak();
    let toggle_subscription_events = events_tx.clone();
    window.on_playlist_subscription_entry_toggle_requested(move |media_key| {
        let media_key = media_key.to_string();
        let snapshot = match toggle_subscription_review.lock() {
            Ok(mut review) => {
                if review.entries.iter().any(|entry| {
                    entry.media_key == media_key && entry.state == "new" && entry.available
                }) && !review.selected_media_keys.remove(&media_key)
                {
                    review.selected_media_keys.insert(media_key.clone());
                }
                Some(review.clone())
            }
            Err(_) => None,
        };
        let Some(snapshot) = snapshot else {
            let _ = toggle_subscription_events
                .send(UiEvent::Status("Playlist review state is unavailable.".to_owned()));
            return;
        };
        let rows =
            playlist_subscription_entry_rows(&snapshot.entries, &snapshot.selected_media_keys);
        let summary = if snapshot.selected_media_keys.is_empty() {
            "Select new entries to queue or dismiss them.".to_owned()
        } else {
            format!(
                "{} new {} selected",
                snapshot.selected_media_keys.len(),
                plural_word(snapshot.selected_media_keys.len(), "entry", "entries")
            )
        };
        let selected_count = i32::try_from(snapshot.selected_media_keys.len()).unwrap_or(i32::MAX);
        let weak = toggle_subscription_window.clone();
        let _ = slint::invoke_from_event_loop(move || {
            if let Some(window) = weak.upgrade() {
                window.set_playlist_subscription_entries(ModelRc::new(VecModel::from(rows)));
                window.set_subscription_selection_summary(summary.into());
                window.set_playlist_subscription_selected_count(selected_count);
            }
        });
    });

    let dismiss_subscription_store = store.clone();
    let dismiss_subscription_review = playlist_subscription_review.clone();
    let dismiss_subscription_window = window.as_weak();
    let dismiss_subscription_events = events_tx.clone();
    let dismiss_subscription_handle = handle.clone();
    window.on_playlist_subscription_dismiss_selected_requested(move || {
        let snapshot = dismiss_subscription_review.lock().ok().map(|review| review.clone());
        let Some(snapshot) = snapshot else {
            return;
        };
        if snapshot.subscription_id.is_empty() || snapshot.selected_media_keys.is_empty() {
            return;
        }
        let store = dismiss_subscription_store.clone();
        let review = dismiss_subscription_review.clone();
        let weak_window = dismiss_subscription_window.clone();
        let events = dismiss_subscription_events.clone();
        let keys = snapshot.selected_media_keys.into_iter().collect::<Vec<_>>();
        let id = snapshot.subscription_id;
        dismiss_subscription_handle.spawn(async move {
            match store.dismiss_playlist_subscription_entries(id.clone(), keys, now()).await {
                Ok(count) => {
                    let _ = events.send(UiEvent::Status(format!(
                        "Dismissed {count} new playlist {}.",
                        plural_word(count, "entry", "entries")
                    )));
                    if let Err(error) =
                        refresh_playlist_subscription_view(&store, &review, weak_window, Some(id))
                            .await
                    {
                        error!(%error, "refresh dismissed playlist entries");
                    }
                }
                Err(error) => {
                    error!(%error, "dismiss new playlist entries");
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not dismiss selected entries: {error}"
                    )));
                }
            }
        });
    });

    let queue_subscription_store = store.clone();
    let queue_subscription_review = playlist_subscription_review.clone();
    let queue_subscription_window = window.as_weak();
    let queue_subscription_events = events_tx.clone();
    let queue_subscription_handle = handle.clone();
    let queue_subscription_wake = queue_wake.clone();
    let queue_subscription_dispatch_gate = update_dispatch_gate.clone();
    let queue_subscription_shutdown = shutdown_started.clone();
    window.on_playlist_subscription_queue_selected_requested(move || {
        if queue_subscription_shutdown.load(Ordering::Acquire) {
            let _ = queue_subscription_events
                .send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        let snapshot = queue_subscription_review.lock().ok().map(|review| review.clone());
        let Some(snapshot) = snapshot else {
            return;
        };
        let Some(subscription) = snapshot.subscription.clone() else {
            return;
        };
        let selected_entries = snapshot
            .entries
            .iter()
            .filter(|entry| snapshot.selected_media_keys.contains(&entry.media_key))
            .cloned()
            .collect::<Vec<_>>();
        if selected_entries.is_empty() {
            return;
        }
        let (
            audio_only,
            video_only,
            subtitle_only,
            metadata_only,
            thumbnail_only,
            audio_format,
            audio_quality,
            video_quality,
            speed_limit_label,
            directory,
            scheduled,
            local_time,
        ) = queue_subscription_window
            .upgrade()
            .map(|window| {
                (
                    window.get_audio_only(),
                    window.get_video_only(),
                    window.get_subtitle_only(),
                    window.get_metadata_only(),
                    window.get_thumbnail_only(),
                    window.get_audio_format().to_string(),
                    window.get_audio_quality().to_string(),
                    window.get_video_quality().to_string(),
                    window.get_download_speed_limit().to_string(),
                    window.get_download_directory().to_string(),
                    window.get_schedule_download(),
                    window.get_schedule_local_time().to_string(),
                )
            })
            .unwrap_or_default();
        if subtitle_only {
            let _ = queue_subscription_events.send(UiEvent::Status(
                "Subtitle-only playlist batches are not available yet. Choose Video + audio, Video only, or Audio only."
                    .to_owned(),
            ));
            return;
        }
        let scheduled_at = if scheduled {
            match parse_local_schedule(&local_time, now()) {
                Ok(timestamp) => Some(timestamp),
                Err(error) => {
                    let _ = queue_subscription_events.send(UiEvent::Status(error.to_string()));
                    return;
                }
            }
        } else {
            None
        };
        let mut presets = DownloadPresets::from_ui_values(
            audio_only,
            video_only,
            &audio_format,
            &audio_quality,
            &video_quality,
        )
        .with_download_speed_limit_label(&speed_limit_label);
        if metadata_only {
            presets.mode = DownloadMode::MetadataOnly;
        } else if thumbnail_only {
            presets.mode = DownloadMode::ThumbnailOnly;
        }
        let store = queue_subscription_store.clone();
        let review = queue_subscription_review.clone();
        let weak_window = queue_subscription_window.clone();
        let events = queue_subscription_events.clone();
        let queue_wake = queue_subscription_wake.clone();
        let handle = queue_subscription_handle.clone();
        let dispatch_gate = queue_subscription_dispatch_gate.clone();
        let shutdown = queue_subscription_shutdown.clone();
        handle.spawn(async move {
            let playlist_title = subscription.title.clone();
            let subscription_id = subscription.id.clone();
            let destination = match tokio::task::spawn_blocking(move || {
                create_playlist_subscription_directory(
                    Path::new(&directory),
                    &playlist_title,
                    &subscription_id,
                )
            })
            .await
            {
                Ok(Ok(path)) => path.to_string_lossy().into_owned(),
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
            let group_id = Uuid::new_v4().to_string();
            let items = selected_entries
                .into_iter()
                .map(|entry| {
                    let mut record = DownloadRecord::queued(
                        Uuid::new_v4().to_string(),
                        entry.media_url.clone(),
                        now(),
                    );
                    record.title = entry.title;
                    record.source = subscription.source.clone();
                    record.playlist_group_id = Some(group_id.clone());
                    record.playlist_group_title = Some(subscription.title.clone());
                    record.download_mode = presets.mode;
                    record.audio_format = presets.audio_format;
                    record.audio_quality = presets.audio_quality;
                    record.video_quality = presets.video_quality;
                    record.download_speed_limit_bytes_per_second =
                        presets.download_speed_limit_bytes_per_second;
                    record.scheduled_at = scheduled_at;
                    if scheduled_at.is_some() {
                        record.status = "Waiting for schedule".to_owned();
                    }
                    record.download_directory = destination.clone();
                    PlaylistSubscriptionQueueItem { media_key: entry.media_key, record }
                })
                .collect::<Vec<_>>();
            let _dispatch_guard = dispatch_gate.lock().await;
            if shutdown.load(Ordering::Acquire) {
                let _ = events.send(UiEvent::Status(
                    "Playlist entries were not queued because Yt Save is shutting down.".to_owned(),
                ));
                return;
            }
            match store.queue_playlist_subscription_entries(subscription.id.clone(), items).await {
                Ok(count) => {
                    let _ = events.send(UiEvent::Refresh);
                    let message = if let Some(timestamp) = scheduled_at {
                        format!(
                            "Scheduled {count} new playlist {} for {} local time.",
                            plural_word(count, "entry", "entries"),
                            format_local_schedule(timestamp)
                        )
                    } else {
                        format!(
                            "Queued {count} new playlist {} from the subscription.",
                            plural_word(count, "entry", "entries")
                        )
                    };
                    let _ = events.send(UiEvent::Status(message));
                    queue_wake.notify_one();
                    if let Err(error) = refresh_playlist_subscription_view(
                        &store,
                        &review,
                        weak_window,
                        Some(subscription.id),
                    )
                    .await
                    {
                        error!(%error, "refresh queued playlist subscription entries");
                    }
                }
                Err(error) => {
                    error!(%error, "queue new playlist subscription entries");
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not queue new playlist entries: {error}"
                    )));
                }
            }
        });
    });

    let scheduled_subscription_store = store.clone();
    let scheduled_subscription_engine = engine.clone();
    let scheduled_subscription_review = playlist_subscription_review.clone();
    let scheduled_subscription_active = playlist_subscription_check_active.clone();
    let scheduled_subscription_window = window.as_weak();
    let scheduled_subscription_events = events_tx.clone();
    let scheduled_subscription_wake = queue_wake.clone();
    let scheduled_subscription_shutdown = shutdown_started.clone();
    handle.spawn(run_playlist_subscription_scheduler(
        scheduled_subscription_store,
        scheduled_subscription_engine,
        PlaylistSubscriptionSchedulerState {
            queue_wake: scheduled_subscription_wake,
            dispatch_gate: update_dispatch_gate.clone(),
            current_download_presets: current_download_presets.clone(),
            review_state: scheduled_subscription_review,
            check_active: scheduled_subscription_active,
            weak_window: scheduled_subscription_window,
            events: scheduled_subscription_events,
            shutdown_started: scheduled_subscription_shutdown,
        },
    ));

    window.on_enqueue_requested(move |submitted_url| {
        if enqueue_shutdown_started.load(Ordering::Acquire) {
            let _ =
                queue_events.send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        let display_url = submitted_url.to_string();
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
            video_only,
            subtitle_only,
            metadata_only,
            thumbnail_only,
            selected_subtitle_languages,
            selected_subtitle_format,
            include_auto_subtitles,
            selected_format,
            (selected_exact_format, format_choice_mode, format_choice_source_url),
            selected_quality,
            selected_video_quality,
            (selected_speed_limit, selected_directory),
            (schedule_enabled, schedule_local_time),
        ) = enqueue_window
            .upgrade()
            .map(|window| {
                (
                    window.get_audio_only(),
                    window.get_video_only(),
                    window.get_subtitle_only(),
                    window.get_metadata_only(),
                    window.get_thumbnail_only(),
                    window.get_subtitle_languages().to_string(),
                    window.get_subtitle_format().to_string(),
                    window.get_include_auto_subtitles(),
                    window.get_audio_format().to_string(),
                    (
                        window.get_selected_format().to_string(),
                        window.get_format_choice_mode().to_string(),
                        window.get_format_choice_source_url().to_string(),
                    ),
                    window.get_audio_quality().to_string(),
                    window.get_video_quality().to_string(),
                    (
                        window.get_download_speed_limit().to_string(),
                        window.get_download_directory().to_string(),
                    ),
                    (window.get_schedule_download(), window.get_schedule_local_time().to_string()),
                )
            })
            .unwrap_or_default();
        let scheduled_at = if schedule_enabled {
            match parse_local_schedule(&schedule_local_time, now()) {
                Ok(timestamp) => Some(timestamp),
                Err(error) => {
                    let _ = queue_events.send(UiEvent::Status(error.to_string()));
                    return;
                }
            }
        } else {
            None
        };
        let normalized_subtitle_languages = if subtitle_only {
            match normalize_subtitle_languages(&selected_subtitle_languages) {
                Some(languages) => languages,
                None => {
                    let _ = queue_events.send(UiEvent::Status(
                        "Enter one or more subtitle language codes, such as en or en,fr.".to_owned(),
                    ));
                    return;
                }
            }
        } else {
            "en".to_owned()
        };
        let inspection_url = url.clone();
        let mut record = DownloadRecord::queued(Uuid::new_v4().to_string(), url, now());
        let mut download_presets = DownloadPresets::from_ui_values(
            audio_only,
            video_only,
            &selected_format,
            &selected_quality,
            &selected_video_quality,
        )
        .with_download_speed_limit_label(&selected_speed_limit);
        if subtitle_only {
            download_presets.mode = DownloadMode::SubtitlesOnly;
        } else if metadata_only {
            download_presets.mode = DownloadMode::MetadataOnly;
        } else if thumbnail_only {
            download_presets.mode = DownloadMode::ThumbnailOnly;
        }
        record.download_mode = download_presets.mode;
        let selected_format_id = match selected_format_id_from_ui(
            &selected_exact_format,
            &format_choice_source_url,
            &format_choice_mode,
            &inspection_url,
            record.download_mode,
        ) {
            Ok(format_id) => format_id,
            Err(message) => {
                let _ = queue_events.send(UiEvent::Status(message.to_owned()));
                return;
            }
        };
        record.audio_format = download_presets.audio_format;
        record.audio_quality = download_presets.audio_quality;
        record.video_quality = download_presets.video_quality;
        record.download_speed_limit_bytes_per_second =
            download_presets.download_speed_limit_bytes_per_second;
        record.subtitle_languages = normalized_subtitle_languages;
        record.subtitle_format = SubtitleFormat::from_database_value(&selected_subtitle_format)
            .unwrap_or_default();
        record.include_auto_subtitles = include_auto_subtitles;
        record.scheduled_at = scheduled_at;
        if scheduled_at.is_some() {
            record.status = "Waiting for schedule".to_owned();
        }
        let store = queue_store.clone();
        let engine = queue_engine.clone();
        let events = queue_events.clone();
        let queue_wake = enqueue_queue_wake.clone();
        let inspector = engine.clone();
        let analysis_generation = enqueue_analysis_generation.clone();
        let dispatch_gate = enqueue_dispatch_gate.clone();
        let shutdown = enqueue_shutdown_started.clone();
        queue_handle.spawn(async move {
            match inspector.inspect(&inspection_url).await {
                Ok(metadata) if metadata.is_playlist => {
                    let playlist_message = if record.download_mode == DownloadMode::SubtitlesOnly {
                        "Subtitle-only downloads support one media URL at a time. Choose a single video or audio URL."
                    } else {
                        "This URL contains a playlist. Review its entries and select what to queue."
                    };
                    if analysis_generation.load(Ordering::Acquire) == request_id {
                        let _ = events.send(UiEvent::Metadata {
                            request_id,
                            source_url: inspection_url,
                            display_url,
                            selection_mode: record.download_mode,
                            metadata: Box::new(metadata),
                        });
                        let _ = events.send(UiEvent::Status(playlist_message.to_owned()));
                    } else {
                        let stale_message = if record.download_mode == DownloadMode::SubtitlesOnly {
                            "This link contains a playlist. Subtitle-only downloads support one media URL at a time."
                        } else {
                            "This link contains a playlist. Analyze it again to review its entries before queueing."
                        };
                        let _ = events.send(UiEvent::Status(stale_message.to_owned()));
                    }
                    return;
                }
                Ok(metadata) => {
                    if let Some(format_id) = selected_format_id.as_deref() {
                        if !format_is_available(&metadata.formats, format_id, record.download_mode) {
                            let _ = events.send(UiEvent::Status(
                                "The selected format is no longer available for this link. Analyze it again and choose another format."
                                    .to_owned(),
                            ));
                            return;
                        }
                        record.selected_format_id = Some(format_id.to_owned());
                    }
                    if record.download_mode == DownloadMode::SubtitlesOnly {
                        let mut available = metadata.manual_subtitle_languages.clone();
                        if record.include_auto_subtitles {
                            available.extend(metadata.automatic_subtitle_languages.iter().cloned());
                        }
                        if !subtitle_languages_available(&record.subtitle_languages, &available) {
                            let _ = events.send(UiEvent::Status(if record.include_auto_subtitles {
                                "yt-dlp did not report captions in the selected language(s) for this link.".to_owned()
                            } else {
                                "yt-dlp did not report manual captions in the selected language(s). Enable auto captions or choose another language.".to_owned()
                            }));
                            return;
                        }
                    }
                    record.title = metadata.title;
                    record.source = metadata.source;
                }
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
            let _dispatch_guard = dispatch_gate.lock().await;
            if shutdown.load(Ordering::Acquire) {
                let _ = events.send(UiEvent::Status(
                    "The queue item was not added because Yt Save is shutting down.".to_owned(),
                ));
                return;
            }
            if let Err(error) = store.save_download(record.clone()).await {
                let _ = events.send(UiEvent::Status(format!("Could not save queue item: {error}")));
                return;
            }
            let schedule_status = scheduled_at.map_or_else(
                || "Added to the download queue.".to_owned(),
                |timestamp| {
                    format!("Scheduled for {} local time.", format_local_schedule(timestamp))
                },
            );
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status(schedule_status));
            queue_wake.notify_one();
        });
    });

    let batch_store = store.clone();
    let batch_engine = engine.clone();
    let batch_events = events_tx.clone();
    let batch_wake = queue_wake.clone();
    let batch_handle = handle.clone();
    let batch_window = window.as_weak();
    let batch_in_progress = Arc::new(AtomicBool::new(false));
    let batch_in_progress_handler = batch_in_progress.clone();
    window.on_enqueue_batch_requested(move |submitted_urls| {
        if batch_shutdown.load(Ordering::Acquire) {
            let _ =
                batch_events.send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        let parsed = match parse_batch_urls(&submitted_urls) {
            Ok(parsed) => parsed,
            Err(message) => {
                let _ = batch_events.send(UiEvent::Status(message));
                return;
            }
        };
        let total_count = parsed.urls.len() + parsed.issues.len();
        let inspection_count = parsed.urls.len();
        let Some((
            audio_only,
            video_only,
            subtitle_only,
            metadata_only,
            thumbnail_only,
            selected_subtitle_languages,
            selected_subtitle_format,
            include_auto_subtitles,
            selected_format,
            selected_quality,
            selected_video_quality,
            selected_speed_limit,
            selected_directory,
            schedule_enabled,
            schedule_local_time,
        )) = batch_window.upgrade().map(|window| {
            (
                window.get_audio_only(),
                window.get_video_only(),
                window.get_subtitle_only(),
                window.get_metadata_only(),
                window.get_thumbnail_only(),
                window.get_subtitle_languages().to_string(),
                window.get_subtitle_format().to_string(),
                window.get_include_auto_subtitles(),
                window.get_audio_format().to_string(),
                window.get_audio_quality().to_string(),
                window.get_video_quality().to_string(),
                window.get_download_speed_limit().to_string(),
                window.get_download_directory().to_string(),
                window.get_schedule_download(),
                window.get_schedule_local_time().to_string(),
            )
        })
        else {
            let _ = batch_events
                .send(UiEvent::Status("The download window is unavailable.".to_owned()));
            return;
        };
        let scheduled_at = if schedule_enabled {
            match parse_local_schedule(&schedule_local_time, now()) {
                Ok(timestamp) => Some(timestamp),
                Err(error) => {
                    let _ = batch_events.send(UiEvent::Status(error.to_string()));
                    return;
                }
            }
        } else {
            None
        };
        let normalized_subtitle_languages = if subtitle_only {
            match normalize_subtitle_languages(&selected_subtitle_languages) {
                Some(languages) => languages,
                None => {
                    let _ = batch_events.send(UiEvent::Status(
                        "Enter one or more subtitle language codes, such as en or en,fr."
                            .to_owned(),
                    ));
                    return;
                }
            }
        } else {
            "en".to_owned()
        };
        let mut download_presets = DownloadPresets::from_ui_values(
            audio_only,
            video_only,
            &selected_format,
            &selected_quality,
            &selected_video_quality,
        )
        .with_download_speed_limit_label(&selected_speed_limit);
        if subtitle_only {
            download_presets.mode = DownloadMode::SubtitlesOnly;
        } else if metadata_only {
            download_presets.mode = DownloadMode::MetadataOnly;
        } else if thumbnail_only {
            download_presets.mode = DownloadMode::ThumbnailOnly;
        }
        let subtitle_format =
            SubtitleFormat::from_database_value(&selected_subtitle_format).unwrap_or_default();
        let Some(batch_guard) =
            BatchEnqueueGuard::start(&batch_in_progress_handler, batch_window.clone())
        else {
            let _ = batch_events.send(UiEvent::Status(
                "A media-link batch is already being checked.".to_owned(),
            ));
            return;
        };
        let store = batch_store.clone();
        let engine = batch_engine.clone();
        let events = batch_events.clone();
        let queue_wake = batch_wake.clone();
        let dispatch_gate = batch_dispatch_gate.clone();
        let shutdown = batch_shutdown.clone();
        batch_handle.spawn(async move {
            let _batch_guard = batch_guard;
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
            let settings = BatchDownloadSettings {
                presets: download_presets,
                subtitle_languages: normalized_subtitle_languages,
                subtitle_format,
                include_auto_subtitles,
                scheduled_at,
                download_directory: destination.to_string_lossy().into_owned(),
            };
            let mut issues = parsed.issues;
            let mut records = Vec::with_capacity(parsed.urls.len());
            for (index, item) in parsed.urls.into_iter().enumerate() {
                if shutdown.load(Ordering::Acquire) {
                    let _ = events.send(UiEvent::Status(
                        "Batch checking stopped because Yt Save is shutting down; no links from this batch were queued."
                            .to_owned(),
                    ));
                    return;
                }
                let _ = events.send(UiEvent::Status(format!(
                    "Checking link on line {} ({} of {}) before adding the batch…",
                    item.line_number,
                    index + 1,
                    inspection_count
                )));
                match engine.inspect(&item.url).await {
                    Ok(metadata) if metadata.is_playlist => {
                        issues.push(format!(
                            "line {}: playlist links need entry review; add them separately",
                            item.line_number
                        ));
                    }
                    Ok(metadata) => {
                        if settings.presets.mode == DownloadMode::SubtitlesOnly {
                            let mut available = metadata.manual_subtitle_languages.clone();
                            if settings.include_auto_subtitles {
                                available
                                    .extend(metadata.automatic_subtitle_languages.iter().cloned());
                            }
                            if !subtitle_languages_available(
                                &settings.subtitle_languages,
                                &available,
                            ) {
                                issues.push(format!(
                                    "line {}: selected subtitle language(s) are unavailable",
                                    item.line_number
                                ));
                                continue;
                            }
                        }
                        records.push(queued_batch_record(
                            item.url,
                            metadata.title,
                            metadata.source,
                            now(),
                            &settings,
                        ));
                    }
                    Err(error) => {
                        error!(line = item.line_number, %error, "inspect batch media link");
                        issues.push(format!(
                            "line {}: yt-dlp could not read this media link",
                            item.line_number
                        ));
                    }
                }
            }

            if records.is_empty() {
                let _ = events.send(UiEvent::Status(batch_result_status(
                    0,
                    total_count,
                    &issues,
                    scheduled_at,
                )));
                return;
            }
            let queued_count = records.len();
            let _dispatch_guard = dispatch_gate.lock().await;
            if shutdown.load(Ordering::Acquire) {
                let _ = events.send(UiEvent::Status(
                    "The links were not queued because Yt Save is shutting down.".to_owned(),
                ));
                return;
            }
            if let Err(error) = store.save_downloads(records).await {
                error!(%error, "persist validated media URL batch");
                let _ = events.send(UiEvent::Refresh);
                let _ = events
                    .send(UiEvent::Status(format!("Could not save the media link batch: {error}")));
                return;
            }
            let status = batch_result_status(queued_count, total_count, &issues, scheduled_at);
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status(status));
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
        let summary = playlist_selection_summary(selection);
        if let Some(window) = toggle_window.upgrade() {
            window.set_playlist_entries(ModelRc::new(VecModel::from(rows)));
            window.set_selected_playlist_count(selected_count as i32);
            window.set_selectable_playlist_count(selectable_count as i32);
            window.set_playlist_selection_summary(summary);
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
        let summary = playlist_selection_summary(selection);
        if let Some(window) = set_selection_window.upgrade() {
            window.set_playlist_entries(ModelRc::new(VecModel::from(rows)));
            window.set_selected_playlist_count(selected_count as i32);
            window.set_selectable_playlist_count(selectable_count as i32);
            window.set_playlist_selection_summary(summary);
        }
    });

    let playlist_page_loader = PlaylistPageLoader {
        selection_state: playlist_selection.clone(),
        engine: engine.clone(),
        handle: handle.clone(),
        events: events_tx.clone(),
        weak_window: window.as_weak(),
    };
    let load_more_loader = playlist_page_loader.clone();
    window.on_playlist_load_more_requested(move || {
        let requested = load_more_loader.selection_state.lock().ok().and_then(|state| {
            let selection = state.as_ref()?;
            selection.has_more_entries.then_some((selection.request_id, selection.next_page_start))
        });
        let Some((request_id, start_index)) = requested else { return };
        let end_index = start_index.saturating_add(PLAYLIST_PAGE_SIZE.saturating_sub(1));
        if let Err(error) =
            request_playlist_page(&load_more_loader, request_id, start_index, end_index, None)
        {
            let _ = load_more_loader.events.send(UiEvent::Status(error));
        }
    });

    let range_selection_state = playlist_selection.clone();
    let range_selection_window = window.as_weak();
    let range_selection_events = events_tx.clone();
    let range_page_loader = playlist_page_loader;
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
        if last_index.saturating_sub(first_index).saturating_add(1)
            > i64::try_from(PLAYLIST_PAGE_SIZE).unwrap_or(i64::MAX)
        {
            let _ = range_selection_events.send(UiEvent::Status(format!(
                "Select ranges of at most {PLAYLIST_PAGE_SIZE} entries at a time."
            )));
            return;
        }
        let Ok(mut state) = range_selection_state.lock() else { return };
        let Some(selection) = state.as_mut() else {
            let _ = range_selection_events
                .send(UiEvent::Status("Analyze a playlist before selecting a range.".to_owned()));
            return;
        };
        let request_id = selection.request_id;
        let missing_entries = (first_index..=last_index).any(|index| {
            !selection.entries.iter().any(|entry| entry.playlist_index == Some(index))
        });
        if missing_entries {
            drop(state);
            let start_index = match usize::try_from(first_index) {
                Ok(index) => index,
                Err(_) => {
                    let _ = range_selection_events.send(UiEvent::Status(
                        "The starting playlist index is too large.".to_owned(),
                    ));
                    return;
                }
            };
            let end_index = match usize::try_from(last_index) {
                Ok(index) => index,
                Err(_) => {
                    let _ = range_selection_events.send(UiEvent::Status(
                        "The ending playlist index is too large.".to_owned(),
                    ));
                    return;
                }
            };
            if let Err(error) = request_playlist_page(
                &range_page_loader,
                request_id,
                start_index,
                end_index,
                Some((first_index, last_index)),
            ) {
                let _ = range_selection_events.send(UiEvent::Status(error));
            }
            return;
        }
        if !select_playlist_range(selection, first_index, last_index) {
            let _ = range_selection_events.send(UiEvent::Status(
                "That range contains no selectable playlist entries.".to_owned(),
            ));
            return;
        }
        let rows = playlist_entry_rows(selection);
        let selected_count = selection.selected_count();
        let selectable_count = selection.selectable_count();
        let summary = playlist_selection_summary(selection);
        if let Some(window) = range_selection_window.upgrade() {
            window.set_playlist_entries(ModelRc::new(VecModel::from(rows)));
            window.set_selected_playlist_count(selected_count as i32);
            window.set_selectable_playlist_count(selectable_count as i32);
            window.set_playlist_selection_summary(summary);
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
    let selected_dispatch_gate = update_dispatch_gate.clone();
    window.on_enqueue_selected_playlist_entries_requested(move || {
        if selected_shutdown_started.load(Ordering::Acquire) {
            let _ = selected_queue_events
                .send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        if selected_queue_window
            .upgrade()
            .is_some_and(|window| window.get_subtitle_only())
        {
            let _ = selected_queue_events.send(UiEvent::Status(
                "Subtitle-only playlist queues are not available yet. Add one media URL at a time.".to_owned(),
            ));
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
            video_only,
            metadata_only,
            thumbnail_only,
            selected_format,
            selected_quality,
            selected_video_quality,
            selected_speed_limit,
            selected_directory,
            schedule_enabled,
            schedule_local_time,
        ) = selected_queue_window
            .upgrade()
            .map(|window| {
                (
                    window.get_audio_only(),
                    window.get_video_only(),
                    window.get_metadata_only(),
                    window.get_thumbnail_only(),
                    window.get_audio_format().to_string(),
                    window.get_audio_quality().to_string(),
                    window.get_video_quality().to_string(),
                window.get_download_speed_limit().to_string(),
                    window.get_download_directory().to_string(),
                    window.get_schedule_download(),
                    window.get_schedule_local_time().to_string(),
                )
            })
            .unwrap_or_default();
        let scheduled_at = if schedule_enabled {
            match parse_local_schedule(&schedule_local_time, now()) {
                Ok(timestamp) => Some(timestamp),
                Err(error) => {
                    let _ = selected_queue_events.send(UiEvent::Status(error.to_string()));
                    return;
                }
            }
        } else {
            None
        };
        let mut download_presets = DownloadPresets::from_ui_values(
            audio_only,
            video_only,
            &selected_format,
            &selected_quality,
            &selected_video_quality,
        )
        .with_download_speed_limit_label(&selected_speed_limit);
        if metadata_only {
            download_presets.mode = DownloadMode::MetadataOnly;
        } else if thumbnail_only {
            download_presets.mode = DownloadMode::ThumbnailOnly;
        }
        let _ = selected_queue_events.send(UiEvent::Status(format!(
            "Adding {} selected playlist entries to the queue…",
            selected_entries.len()
        )));
        let store = selected_queue_store.clone();
        let events = selected_queue_events.clone();
        let queue_wake = selected_queue_wake.clone();
        let dispatch_gate = selected_dispatch_gate.clone();
        let shutdown = selected_shutdown_started.clone();
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
            let playlist_group_id = Uuid::new_v4().to_string();
            let playlist_group_title = selection.title.clone();
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
                record.playlist_group_id = Some(playlist_group_id.clone());
                record.playlist_group_title = Some(playlist_group_title.clone());
                record.download_mode = download_presets.mode;
                record.audio_format = download_presets.audio_format;
                record.audio_quality = download_presets.audio_quality;
                record.video_quality = download_presets.video_quality;
                record.download_speed_limit_bytes_per_second =
                    download_presets.download_speed_limit_bytes_per_second;
                record.scheduled_at = scheduled_at;
                if scheduled_at.is_some() {
                    record.status = "Waiting for schedule".to_owned();
                }
                record.download_directory = destination.clone();
                records.push(record);
            }
            let selected_count = records.len();
            let _dispatch_guard = dispatch_gate.lock().await;
            if shutdown.load(Ordering::Acquire) {
                let _ = events.send(UiEvent::Status(
                    "Playlist entries were not queued because Yt Save is shutting down.".to_owned(),
                ));
                return;
            }
            if let Err(error) = store.save_downloads(records).await {
                error!(%error, "persist selected playlist batch");
                let _ = events.send(UiEvent::Refresh);
                let _ = events.send(UiEvent::Status(format!(
                    "Could not queue the selected playlist entries: {error}"
                )));
                return;
            }
            let _ = events.send(UiEvent::Refresh);
            let status = scheduled_at.map_or_else(
                || format!("Queued {selected_count} selected playlist entries. Each item will download separately."),
                |timestamp| {
                    format!("Scheduled {selected_count} playlist entries for {} local time.", format_local_schedule(timestamp))
                },
            );
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

    let host_limit_store = store.clone();
    let host_limit_events = events_tx.clone();
    let host_limit_wake = queue_wake.clone();
    let host_limit_handle = handle.clone();
    let host_limit_generation = Arc::new(AtomicU64::new(0));
    let host_limit_write_lock = Arc::new(AsyncMutex::new(()));
    let host_limit_window = window.as_weak();
    window.on_max_concurrent_per_host_requested(move |value| {
        let Ok(limit) = value.parse::<usize>() else {
            return;
        };
        if !(1..=MAX_CONCURRENT_PER_HOST).contains(&limit) {
            return;
        }
        let store = host_limit_store.clone();
        let events = host_limit_events.clone();
        let queue_wake = host_limit_wake.clone();
        let generation_state = host_limit_generation.clone();
        let generation = generation_state.fetch_add(1, Ordering::AcqRel) + 1;
        let write_lock = host_limit_write_lock.clone();
        let weak_window = host_limit_window.clone();
        host_limit_handle.spawn(async move {
            let _write_guard = write_lock.lock().await;
            if generation_state.load(Ordering::Acquire) != generation {
                return;
            }
            let previous_limit = store
                .load_max_concurrent_per_host()
                .await
                .unwrap_or(DEFAULT_MAX_CONCURRENT_PER_HOST);
            if generation_state.load(Ordering::Acquire) != generation {
                return;
            }
            match store.set_max_concurrent_per_host(limit, now()).await {
                Ok(()) => {
                    queue_wake.notify_one();
                    let _ = events.send(UiEvent::Status(format!(
                        "Each website can now run up to {limit} downloads at once."
                    )));
                }
                Err(error) => {
                    error!(%error, "save per-website concurrency limit");
                    if generation_state.load(Ordering::Acquire) == generation {
                        let previous_value = previous_limit.to_string().into();
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(window) = weak_window.upgrade() {
                                window.set_max_concurrent_per_host(previous_value);
                            }
                        });
                    }
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not save the per-website download limit: {error}"
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

    let presets_store = store.clone();
    let presets_events = events_tx.clone();
    let presets_handle = handle.clone();
    let presets_state = current_download_presets.clone();
    let preset_apply_guard = applying_named_download_preset.clone();
    let presets_generation = Arc::new(AtomicU64::new(0));
    let presets_write_lock = Arc::new(AsyncMutex::new(()));
    window.on_download_presets_changed(
        move |audio_only, video_only, subtitle_only, metadata_only, thumbnail_only, audio_format, audio_quality, video_quality, speed_limit| {
            if preset_apply_guard.load(Ordering::Acquire) {
                return;
            }
            let mut presets = DownloadPresets::from_ui_values(
                audio_only,
                video_only,
                audio_format.as_str(),
                audio_quality.as_str(),
                video_quality.as_str(),
            )
            .with_download_speed_limit_label(speed_limit.as_str());
            if subtitle_only {
                presets.mode = DownloadMode::SubtitlesOnly;
            } else if metadata_only {
                presets.mode = DownloadMode::MetadataOnly;
            } else if thumbnail_only {
                presets.mode = DownloadMode::ThumbnailOnly;
            }
            match presets_state.lock() {
                Ok(mut current) => *current = presets,
                Err(_) => {
                    error!("current New Download choices are unavailable");
                    return;
                }
            }

            let generation_state = presets_generation.clone();
            let generation = generation_state.fetch_add(1, Ordering::AcqRel) + 1;
            let write_lock = presets_write_lock.clone();
            let state = presets_state.clone();
            let store = presets_store.clone();
            let events = presets_events.clone();
            presets_handle.spawn(async move {
                let _write_guard = write_lock.lock().await;
                if generation_state.load(Ordering::Acquire) != generation {
                    return;
                }
                let current = match state.lock() {
                    Ok(current) => *current,
                    Err(_) => {
                        error!("current New Download choices are unavailable");
                        return;
                    }
                };
                if let Err(error) = store.set_download_presets(current, now()).await {
                    error!(%error, "save New Download choices");
                    let _ = events.send(UiEvent::Status(format!(
                        "Your New Download choices are active for this session but could not be saved: {error}"
                    )));
                }
            });
        },
    );

    let save_named_presets_store = store.clone();
    let save_named_presets_state = saved_download_presets_state.clone();
    let save_named_presets_current = current_download_presets.clone();
    let save_named_presets_handle = handle.clone();
    let save_named_presets_events = events_tx.clone();
    let save_named_presets_window = window.as_weak();
    window.on_save_saved_download_preset_requested(
        move |name, subtitle_languages, subtitle_format, include_auto_subtitles, download_directory| {
            let presets = match save_named_presets_current.lock() {
                Ok(presets) => *presets,
                Err(_) => {
                    error!("current New Download choices are unavailable");
                    return;
                }
            };
            let store = save_named_presets_store.clone();
            let state = save_named_presets_state.clone();
            let events = save_named_presets_events.clone();
            let weak_window = save_named_presets_window.clone();
            save_named_presets_handle.spawn(async move {
                match store
                    .save_named_download_preset(
                        name.to_string(),
                        presets,
                        subtitle_languages.to_string(),
                        SubtitleFormat::from_database_value(subtitle_format.as_str())
                            .unwrap_or_default(),
                        include_auto_subtitles,
                        download_directory.to_string(),
                        now(),
                    )
                    .await
                {
                    Ok(saved) => {
                        let selected_name = saved
                            .iter()
                            .find(|preset| preset.name.eq_ignore_ascii_case(name.as_str()))
                            .map(|preset| preset.name.clone())
                            .unwrap_or_else(|| name.to_string().trim().to_owned());
                        if let Ok(mut current) = state.lock() {
                            *current = saved.clone();
                        }
                        let model = saved_download_preset_model(&saved);
                        let selected_name_for_ui = selected_name.clone();
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(window) = weak_window.upgrade() {
                                window.set_saved_download_preset_names(model);
                                window.set_saved_download_preset_selection(selected_name_for_ui.into());
                                window.set_saved_download_preset_name_input(SharedString::default());
                            }
                        });
                        let _ = events.send(UiEvent::Status(format!(
                            "Saved download preset ‘{selected_name}’."
                        )));
                    }
                    Err(error) => {
                        error!(%error, "save named download preset");
                        let _ = events.send(UiEvent::Status(format!(
                            "Could not save this download preset: {error}"
                        )));
                    }
                }
            });
        },
    );

    let apply_named_presets_state = saved_download_presets_state.clone();
    let apply_named_presets_current = current_download_presets.clone();
    let apply_named_presets_guard = applying_named_download_preset.clone();
    let apply_named_presets_store = store.clone();
    let apply_named_presets_handle = handle.clone();
    let apply_named_presets_events = events_tx.clone();
    let apply_named_presets_window = window.as_weak();
    window.on_apply_saved_download_preset_requested(move |name| {
        let preset = match apply_named_presets_state.lock() {
            Ok(saved) => saved
                .iter()
                .find(|preset| preset.name == name.as_str())
                .cloned(),
            Err(_) => {
                error!("saved download presets are unavailable");
                None
            }
        };
        let Some(preset) = preset else {
            let _ = apply_named_presets_events.send(UiEvent::Status(
                "Choose an available saved download preset first.".to_owned(),
            ));
            return;
        };
        if let Ok(mut current) = apply_named_presets_current.lock() {
            *current = preset.presets;
        }
        let store = apply_named_presets_store.clone();
        let events = apply_named_presets_events.clone();
        let weak_window = apply_named_presets_window.clone();
        let applying = apply_named_presets_guard.clone();
        apply_named_presets_handle.spawn(async move {
            let destination = validate_download_directory(Path::new(&preset.download_directory))
                .ok()
                .map(|path| path.to_string_lossy().into_owned());
            let destination_error = if destination.is_none() {
                Some("The saved destination is unavailable; the current folder was kept.".to_owned())
            } else {
                None
            };
            let mut persistence_error = None;
            if let Err(error) = store.set_download_presets(preset.presets, now()).await {
                error!(%error, "persist applied named download preset");
                persistence_error = Some(error.to_string());
            }
            if let Some(directory) = destination.as_ref()
                && let Err(error) = store.set_default_download_directory(directory.clone(), now()).await
            {
                error!(%error, "persist named preset destination");
                persistence_error = Some(error.to_string());
            }
            let model_values = preset.clone();
            let selected_name = preset.name.clone();
            let weak_window_for_update = weak_window.clone();
            let _ = slint::invoke_from_event_loop(move || {
                if let Some(window) = weak_window_for_update.upgrade() {
                    applying.store(true, Ordering::Release);
                    window.set_audio_only(model_values.presets.mode == DownloadMode::AudioOnly);
                    window.set_video_only(model_values.presets.mode == DownloadMode::VideoOnly);
                    window.set_subtitle_only(
                        model_values.presets.mode == DownloadMode::SubtitlesOnly,
                    );
                    window.set_metadata_only(
                        model_values.presets.mode == DownloadMode::MetadataOnly,
                    );
                    window.set_thumbnail_only(
                        model_values.presets.mode == DownloadMode::ThumbnailOnly,
                    );
                    window.set_audio_format(model_values.presets.audio_format.as_database_value().into());
                    window.set_audio_quality(model_values.presets.audio_quality.as_database_value().into());
                    window.set_video_quality(model_values.presets.video_quality.label().into());
                    window.set_download_speed_limit(
                        download_speed_limit_label(
                            model_values.presets.download_speed_limit_bytes_per_second,
                        )
                        .into(),
                    );
                    window.set_subtitle_languages(model_values.subtitle_languages.into());
                    window.set_subtitle_format(
                        model_values.subtitle_format.as_database_value().into(),
                    );
                    window.set_include_auto_subtitles(model_values.include_auto_subtitles);
                    if let Some(directory) = destination {
                        window.set_download_directory(directory.into());
                    }
                    window.set_saved_download_preset_selection(selected_name.into());
                    applying.store(false, Ordering::Release);
                }
            });
            let message = if let Some(error) = persistence_error {
                format!(
                    "Applied ‘{}’ for this session, but could not save all defaults: {error}",
                    preset.name
                )
            } else if let Some(error) = destination_error {
                format!("Applied ‘{}’. {error}", preset.name)
            } else {
                format!("Applied download preset ‘{}’.", preset.name)
            };
            let _ = events.send(UiEvent::Status(message));
        });
    });

    let delete_named_presets_store = store.clone();
    let delete_named_presets_state = saved_download_presets_state.clone();
    let delete_named_presets_handle = handle.clone();
    let delete_named_presets_events = events_tx.clone();
    let delete_named_presets_window = window.as_weak();
    window.on_delete_saved_download_preset_requested(move |name| {
        let store = delete_named_presets_store.clone();
        let state = delete_named_presets_state.clone();
        let events = delete_named_presets_events.clone();
        let weak_window = delete_named_presets_window.clone();
        let selected_name = name.to_string();
        delete_named_presets_handle.spawn(async move {
            match store.delete_named_download_preset(selected_name.clone(), now()).await {
                Ok(saved) => {
                    if let Ok(mut current) = state.lock() {
                        *current = saved.clone();
                    }
                    let model = saved_download_preset_model(&saved);
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(window) = weak_window.upgrade() {
                            window.set_saved_download_preset_names(model);
                            window.set_saved_download_preset_selection(SharedString::default());
                        }
                    });
                    let _ = events.send(UiEvent::Status(format!(
                        "Removed saved download preset ‘{selected_name}’."
                    )));
                }
                Err(error) => {
                    error!(%error, "remove named download preset");
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not remove this download preset: {error}"
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

    let group_store = store.clone();
    let group_events = events_tx.clone();
    let group_wake = queue_wake.clone();
    let group_active_jobs = active_jobs.clone();
    let group_handle = handle.clone();
    let group_shutdown_started = shutdown_started.clone();
    window.on_playlist_group_action_requested(move |group_id, action| {
        if group_shutdown_started.load(Ordering::Acquire) {
            let _ = group_events
                .send(UiEvent::Status("The application is shutting down.".to_owned()));
            return;
        }
        let group_id = group_id.to_string();
        let action = action.to_string();
        let store = group_store.clone();
        let events = group_events.clone();
        let active_jobs = group_active_jobs.clone();
        let queue_wake = group_wake.clone();
        group_handle.spawn(async move {
            match action.as_str() {
                "pause" => match pause_playlist_group(&store, &active_jobs, &group_id).await {
                    Ok((queued, active)) => {
                        let _ = events.send(UiEvent::Refresh);
                        let _ = events.send(UiEvent::Status(format!(
                            "Paused {queued} queued playlist items and requested pauses for {active} active items."
                        )));
                    }
                    Err(error) => {
                        error!(%error, playlist_group_id = %group_id, "pause playlist group");
                        let _ = events.send(UiEvent::Status(format!(
                            "Could not pause the playlist group: {error}"
                        )));
                    }
                },
                "resume" => match resume_playlist_group(&store, &group_id).await {
                    Ok(resumed) => {
                        let _ = events.send(UiEvent::Refresh);
                        let _ = events.send(UiEvent::Status(format!(
                            "Resumed {resumed} paused playlist items."
                        )));
                        if resumed > 0 {
                            queue_wake.notify_one();
                        }
                    }
                    Err(error) => {
                        error!(%error, playlist_group_id = %group_id, "resume playlist group");
                        let _ = events.send(UiEvent::Status(format!(
                            "Could not resume the playlist group: {error}"
                        )));
                    }
                },
                _ => {}
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

    let main_pause_store = store.clone();
    let main_pause_active_jobs = active_jobs.clone();
    let main_pause_events = events_tx.clone();
    let main_pause_handle = handle.clone();
    let main_pause_shutdown = shutdown_started.clone();
    window.on_pause_all_requested(move || {
        if main_pause_shutdown.load(Ordering::Acquire) {
            return;
        }
        let store = main_pause_store.clone();
        let active_jobs = main_pause_active_jobs.clone();
        let events = main_pause_events.clone();
        main_pause_handle.spawn(async move {
            match pause_all_downloads(&store, &active_jobs, true).await {
                Ok((pending, active)) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(format!(
                        "Paused {pending} queued item(s) and requested a safe pause for {active} active item(s)."
                    )));
                }
                Err(error) => {
                    error!(%error, "pause all downloads from queue");
                    let _ = events.send(UiEvent::Status(format!("Could not pause all downloads: {error}")));
                }
            }
        });
    });

    let main_resume_store = store.clone();
    let main_resume_events = events_tx.clone();
    let main_resume_wake = queue_wake.clone();
    let main_resume_handle = handle.clone();
    let main_resume_shutdown = shutdown_started.clone();
    window.on_resume_all_requested(move || {
        if main_resume_shutdown.load(Ordering::Acquire) {
            return;
        }
        let store = main_resume_store.clone();
        let events = main_resume_events.clone();
        let queue_wake = main_resume_wake.clone();
        main_resume_handle.spawn(async move {
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
                    error!(%error, "resume all downloads from queue");
                    let _ = events
                        .send(UiEvent::Status(format!("Could not resume downloads: {error}")));
                }
            }
        });
    });

    let main_cancel_store = store.clone();
    let main_cancel_active_jobs = active_jobs.clone();
    let main_cancel_events = events_tx.clone();
    let main_cancel_handle = handle.clone();
    let main_cancel_shutdown = shutdown_started.clone();
    window.on_cancel_all_requested(move || {
        if main_cancel_shutdown.load(Ordering::Acquire) {
            return;
        }
        let store = main_cancel_store.clone();
        let active_jobs = main_cancel_active_jobs.clone();
        let events = main_cancel_events.clone();
        main_cancel_handle.spawn(async move {
            match cancel_all_downloads(&store, &active_jobs).await {
                Ok((queued, active)) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(format!(
                        "Canceled {queued} queued or paused item(s) and requested cancellation for {active} active item(s). Partial files were kept."
                    )));
                }
                Err(error) => {
                    error!(%error, "cancel all downloads");
                    let _ = events.send(UiEvent::Status(format!("Could not cancel all downloads: {error}")));
                }
            }
        });
    });

    let retry_all_store = store.clone();
    let retry_all_events = events_tx.clone();
    let retry_all_wake = queue_wake.clone();
    let retry_all_handle = handle.clone();
    let retry_all_shutdown = shutdown_started.clone();
    window.on_retry_all_failed_requested(move || {
        if retry_all_shutdown.load(Ordering::Acquire) {
            return;
        }
        let store = retry_all_store.clone();
        let events = retry_all_events.clone();
        let queue_wake = retry_all_wake.clone();
        retry_all_handle.spawn(async move {
            match retry_all_failed_downloads(&store).await {
                Ok(count) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(format!(
                        "Queued {count} failed or interrupted item(s) for retry."
                    )));
                    if count > 0 {
                        queue_wake.notify_one();
                    }
                }
                Err(error) => {
                    error!(%error, "retry all failed downloads");
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not retry failed downloads: {error}"
                    )));
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
            events.send(UiEvent::Status("Choose the moved download file to relink it.".to_owned()));
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
                    .send(UiEvent::Status("Choose a supported audio, video, subtitle, metadata, or image file.".to_owned()));
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

    clipboard_popup::configure_clipboard_popup(
        &popup,
        clipboard_popup::ClipboardPopupContext {
            main_window: window.as_weak(),
            popup_window: popup.as_weak(),
            popup_generation: clipboard_popup_generation.clone(),
            shutdown_started: shutdown_started.clone(),
        },
    );

    tray::configure_tray(
        &window,
        &tray,
        tray::TrayContext {
            store: store.clone(),
            handle: handle.clone(),
            events: events_tx.clone(),
            active_jobs: active_jobs.clone(),
            subscription_check_active: playlist_subscription_check_active.clone(),
            dispatch_gate: update_dispatch_gate.clone(),
            queue_wake: queue_wake.clone(),
            preferences: tray_preferences_state.clone(),
            shutdown_started,
            shutdown_committed,
        },
    );

    info!(product = env!("YT_SAVE_PRODUCT_NAME"), "application started");
    window.run().context("run Slint event loop")?;
    let _ = clipboard_popup::stop_clipboard_monitor(
        &clipboard_monitor_slot,
        &clipboard_monitor_generation,
    );
    runtime.shutdown_timeout(Duration::from_secs(3));
    Ok(())
}

#[cfg(windows)]
struct CookieAuthContext<'a> {
    main_window: &'a MainWindow,
    login_window: &'a CookieLoginWindow,
    cookie_store: CookieStore,
    handle: tokio::runtime::Handle,
    events: mpsc::UnboundedSender<UiEvent>,
    generation: Arc<AtomicU64>,
    login_session: Arc<AtomicU64>,
    shutdown_started: Arc<AtomicBool>,
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
            || (pause_retry_waits
                && matches!(record.status.as_str(), "Waiting for retry" | "Waiting for schedule")))
            && store.pause_queued_download(&record.id, now()).await?
        {
            pending_count += 1;
            continue;
        }
        if matches!(
            record.status.as_str(),
            "Analyzing" | "Downloading" | "Post-processing" | "Pausing" | "Canceling"
        ) && request_pause_for_active_record(store, active_jobs, &record).await?
        {
            active_count += 1;
        }
    }
    Ok((pending_count, active_count))
}

async fn cancel_all_downloads(store: &Store, active_jobs: &ActiveJobs) -> Result<(usize, usize)> {
    let records = store.list_downloads().await?;
    let mut pending_count = 0;
    let mut active_count = 0;
    for record in records {
        if matches!(
            record.status.as_str(),
            "Queued" | "Waiting for retry" | "Paused" | "Waiting for schedule"
        ) && store.cancel_queued_download(&record.id, now()).await?
        {
            pending_count += 1;
            continue;
        }
        if !matches!(
            record.status.as_str(),
            "Queued"
                | "Waiting for retry"
                | "Analyzing"
                | "Downloading"
                | "Post-processing"
                | "Pausing"
                | "Canceling"
        ) {
            continue;
        }
        let control = active_jobs
            .lock()
            .map_err(|_| anyhow::anyhow!("active download state is unavailable"))?
            .get(&record.id)
            .cloned();
        let Some(control) = control else { continue };
        if record.status == "Canceling"
            || store.request_active_cancel(&record.id, now()).await?
            || store.is_canceling(&record.id).await?
        {
            control.send_replace(JobSignal::Cancel);
            active_count += 1;
        }
    }
    Ok((pending_count, active_count))
}

async fn pause_playlist_group(
    store: &Store,
    active_jobs: &ActiveJobs,
    group_id: &str,
) -> Result<(usize, usize)> {
    let records = store.list_downloads().await?;
    let mut pending_count = 0;
    let mut active_count = 0;
    for record in
        records.into_iter().filter(|record| record.playlist_group_id.as_deref() == Some(group_id))
    {
        if matches!(record.status.as_str(), "Queued" | "Waiting for retry" | "Waiting for schedule")
            && store.pause_queued_download(&record.id, now()).await?
        {
            pending_count += 1;
            continue;
        }
        if matches!(
            record.status.as_str(),
            "Analyzing" | "Downloading" | "Post-processing" | "Pausing"
        ) && request_pause_for_active_record(store, active_jobs, &record).await?
        {
            active_count += 1;
        }
    }
    Ok((pending_count, active_count))
}

async fn resume_playlist_group(store: &Store, group_id: &str) -> Result<usize> {
    let records = store.list_downloads().await?;
    let mut resumed_count = 0;
    for record in records.into_iter().filter(|record| {
        record.playlist_group_id.as_deref() == Some(group_id) && record.status == "Paused"
    }) {
        if store.resume_download(&record.id, now()).await?.is_some() {
            resumed_count += 1;
        }
    }
    Ok(resumed_count)
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

async fn retry_all_failed_downloads(store: &Store) -> Result<usize> {
    let records = store.list_downloads().await?;
    let mut retried_count = 0;
    for record in records
        .into_iter()
        .filter(|record| matches!(record.status.as_str(), "Failed" | "Interrupted"))
    {
        if store.retry_download(&record.id, now()).await?.is_some() {
            retried_count += 1;
        }
    }
    Ok(retried_count)
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
    update_dispatch_gate: Arc<AsyncMutex<()>>,
    shutdown: ShutdownState,
) {
    let ShutdownState { started: shutdown_started, committed: shutdown_committed } = shutdown;
    let mut workers = JoinSet::new();
    let active_host_counts: ActiveHostCounts = Arc::new(Mutex::new(HashMap::new()));
    loop {
        if shutdown_started.load(Ordering::Acquire) {
            if drain_workers_during_shutdown(
                &mut workers,
                &queue_wake,
                &shutdown_started,
                &shutdown_committed,
            )
            .await
            {
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
        match store.promote_due_scheduled_downloads(now()).await {
            Ok(count) if count > 0 => {
                let _ = events.send(UiEvent::Refresh);
            }
            Ok(_) => {}
            Err(error) => {
                error!(%error, "promote due scheduled downloads");
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
            let per_host_limit = match store.load_max_concurrent_per_host().await {
                Ok(limit) => limit,
                Err(error) => {
                    error!(%error, "read per-website concurrency limit; using default");
                    retry_after_error = true;
                    DEFAULT_MAX_CONCURRENT_PER_HOST
                }
            };
            let active_host_snapshot = active_host_counts_snapshot(&active_host_counts);
            let _dispatch_guard = update_dispatch_gate.lock().await;
            if shutdown_started.load(Ordering::Acquire) {
                break;
            }
            let pending = match store
                .next_queued_download_with_host_limit(active_host_snapshot.clone(), per_host_limit)
                .await
            {
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
            let active = match store
                .claim_queued_download_if_host_eligible(
                    &pending.id,
                    active_host_snapshot,
                    per_host_limit,
                    now(),
                )
                .await
            {
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
            let host_registration = ActiveHostRegistration::register(
                normalized_host_key(&active.url).unwrap_or_default(),
                active_host_counts.clone(),
            );
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
                drop(host_registration);
                job_wake.notify_one();
            });
        }

        if shutdown_started.load(Ordering::Acquire) {
            continue;
        }
        let retry_deadline = match store.next_retry_at().await {
            Ok(deadline) => deadline,
            Err(error) => {
                error!(%error, "read next automatic retry deadline");
                retry_after_error = true;
                None
            }
        };
        let schedule_deadline = match store.next_scheduled_at().await {
            Ok(deadline) => deadline,
            Err(error) => {
                error!(%error, "read next scheduled download deadline");
                retry_after_error = true;
                None
            }
        };
        let next_deadline = [retry_deadline, schedule_deadline].into_iter().flatten().min();
        let retry_wait = next_deadline.map(|deadline| {
            Duration::from_secs(
                u64::try_from(deadline.saturating_sub(now()).max(0)).unwrap_or_default(),
            )
        });
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

async fn drain_workers_during_shutdown(
    workers: &mut JoinSet<()>,
    queue_wake: &Notify,
    shutdown_started: &AtomicBool,
    shutdown_committed: &AtomicBool,
) -> bool {
    loop {
        if shutdown_committed.load(Ordering::Acquire) {
            return true;
        }
        if !shutdown_started.load(Ordering::Acquire) {
            return false;
        }
        let notified = queue_wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if shutdown_committed.load(Ordering::Acquire) {
            return true;
        }
        if !shutdown_started.load(Ordering::Acquire) {
            return false;
        }
        if workers.is_empty() {
            notified.await;
            continue;
        }
        tokio::select! {
            result = workers.join_next() => {
                if let Some(Err(error)) = result {
                    error!(%error, "queue worker task failed while shutting down");
                }
            }
            _ = notified.as_mut() => {}
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
            if let Some(format_id) = active.selected_format_id.as_deref()
                && !format_is_available(&metadata.formats, format_id, active.download_mode)
            {
                active.status = "Failed".to_owned();
                active.error = Some(
                    "The selected format is no longer available. Remove this item, analyze the link again, and add a new item with another format."
                        .to_owned(),
                );
                active.updated_at = now();
                let message = active.error.clone().unwrap_or_default();
                match finish_failed_job(active.clone(), &store).await {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = events.send(UiEvent::Refresh);
                        return;
                    }
                    Err(error) => {
                        error!(%error, job_id = %active.id, "persist stale format failure")
                    }
                }
                let _ = events.send(UiEvent::Refresh);
                let _ = events.send(UiEvent::Status(message));
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
                .with_selected_format_id(active.selected_format_id.clone())
                .with_concurrent_fragments(concurrent_fragments)
                .with_download_speed_limit(active.download_speed_limit_bytes_per_second)
                .with_subtitle_options(
                    active.subtitle_languages.clone(),
                    active.subtitle_format,
                    active.include_auto_subtitles,
                )
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
            let message = if final_record.download_mode == DownloadMode::SubtitlesOnly {
                "Subtitle sidecar saved."
            } else if final_record.download_mode == DownloadMode::MetadataOnly {
                "Metadata sidecar saved."
            } else if final_record.download_mode == DownloadMode::ThumbnailOnly {
                "Thumbnail image saved."
            } else {
                "Download complete."
            };
            (true, false, false, message.to_owned())
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

const MAX_BATCH_URLS: usize = 100;

#[derive(Debug, PartialEq, Eq)]
struct BatchUrl {
    line_number: usize,
    url: String,
}

#[derive(Debug, PartialEq, Eq)]
struct ParsedBatchUrls {
    urls: Vec<BatchUrl>,
    issues: Vec<String>,
}

fn parse_batch_urls(value: &str) -> std::result::Result<ParsedBatchUrls, String> {
    let mut parsed = ParsedBatchUrls { urls: Vec::new(), issues: Vec::new() };
    let mut seen = HashSet::new();
    let mut nonempty_lines = 0;

    for (index, line) in value.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        nonempty_lines += 1;
        if nonempty_lines > MAX_BATCH_URLS {
            return Err(format!("Paste no more than {MAX_BATCH_URLS} media URLs at a time."));
        }

        match validate_url(line) {
            Ok(url) if seen.insert(url.clone()) => {
                parsed.urls.push(BatchUrl { line_number: index + 1, url });
            }
            Ok(_) => parsed.issues.push(format!("line {}: duplicate URL skipped", index + 1)),
            Err(message) => parsed.issues.push(format!("line {}: {message}", index + 1)),
        }
    }

    if parsed.urls.is_empty() {
        let detail = parsed.issues.first().map_or_else(
            || "Paste one HTTP(S) media URL per line.".to_owned(),
            |issue| format!("No valid media URLs found. {issue}."),
        );
        return Err(detail);
    }

    Ok(parsed)
}

#[derive(Clone)]
struct BatchDownloadSettings {
    presets: DownloadPresets,
    subtitle_languages: String,
    subtitle_format: SubtitleFormat,
    include_auto_subtitles: bool,
    scheduled_at: Option<i64>,
    download_directory: String,
}

fn queued_batch_record(
    url: String,
    title: String,
    source: String,
    created_at: i64,
    settings: &BatchDownloadSettings,
) -> DownloadRecord {
    let mut record = DownloadRecord::queued(Uuid::new_v4().to_string(), url, created_at);
    record.title = title;
    record.source = source;
    record.download_mode = settings.presets.mode;
    record.audio_format = settings.presets.audio_format;
    record.audio_quality = settings.presets.audio_quality;
    record.video_quality = settings.presets.video_quality;
    record.download_speed_limit_bytes_per_second =
        settings.presets.download_speed_limit_bytes_per_second;
    record.subtitle_languages = settings.subtitle_languages.clone();
    record.subtitle_format = settings.subtitle_format;
    record.include_auto_subtitles = settings.include_auto_subtitles;
    record.scheduled_at = settings.scheduled_at;
    record.download_directory = settings.download_directory.clone();
    if settings.scheduled_at.is_some() {
        record.status = "Waiting for schedule".to_owned();
    }
    record
}

fn batch_result_status(
    queued_count: usize,
    total_count: usize,
    issues: &[String],
    scheduled_at: Option<i64>,
) -> String {
    let mut message = if queued_count == 0 {
        "No media links were added.".to_owned()
    } else if let Some(timestamp) = scheduled_at {
        format!(
            "Scheduled {queued_count} of {total_count} links for {} local time.",
            format_local_schedule(timestamp)
        )
    } else {
        format!("Added {queued_count} of {total_count} links to the queue.")
    };

    if !issues.is_empty() {
        message.push_str(" Skipped: ");
        message.push_str(&issues.iter().take(4).cloned().collect::<Vec<_>>().join("; "));
        if issues.len() > 4 {
            message.push_str(&format!("; and {} more", issues.len() - 4));
        }
    }
    message
}

fn row_views(records: &[DownloadRecord]) -> Vec<DownloadRowData> {
    row_views_filtered(records, "")
}

fn row_views_filtered(records: &[DownloadRecord], query: &str) -> Vec<DownloadRowData> {
    let query = query.trim().to_lowercase();
    let mut position = 0usize;
    let mut displayed_groups = HashSet::new();
    records
        .iter()
        .filter_map(|record| {
            let queue_position = if record.status == "Queued" {
                position += 1;
                format!("#{}", position)
            } else if record.status == "Waiting for retry" {
                "Retry".to_owned()
            } else if record.status == "Waiting for schedule" {
                "Scheduled".to_owned()
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
                    record.playlist_group_title.as_deref().unwrap_or_default(),
                ]
                .iter()
                .any(|value| value.to_lowercase().contains(&query));
            if !matches_query {
                return None;
            }
            let group_action = record.playlist_group_id.as_deref().and_then(|group_id| {
                displayed_groups
                    .insert(group_id.to_owned())
                    .then(|| playlist_group_action_label(records, group_id))
                    .flatten()
            });
            Some(row_view_with_queue_position(
                record,
                &queue_position,
                group_action.map_or("", |(action, _)| action),
                group_action.map_or("", |(_, group_id)| group_id),
            ))
        })
        .collect()
}

fn playlist_group_action_label<'a>(
    records: &'a [DownloadRecord],
    group_id: &'a str,
) -> Option<(&'static str, &'a str)> {
    let mut has_pending = false;
    let mut has_paused = false;
    for record in
        records.iter().filter(|record| record.playlist_group_id.as_deref() == Some(group_id))
    {
        match record.status.as_str() {
            "Queued"
            | "Waiting for retry"
            | "Waiting for schedule"
            | "Analyzing"
            | "Downloading"
            | "Post-processing"
            | "Pausing" => has_pending = true,
            "Paused" => has_paused = true,
            _ => {}
        }
    }
    if has_pending {
        Some(("Pause group", group_id))
    } else if has_paused {
        Some(("Resume group", group_id))
    } else {
        None
    }
}

fn queue_summary_text(records: &[DownloadRecord]) -> String {
    let pending = records
        .iter()
        .filter(|record| {
            matches!(
                record.status.as_str(),
                "Queued" | "Waiting for retry" | "Waiting for schedule" | "Paused"
            )
        })
        .collect::<Vec<_>>();
    if pending.is_empty() {
        return "No pending work".to_owned();
    }

    let mut known_sizes = 0usize;
    let mut remaining_bytes = 0u64;
    let queued_count = pending.iter().filter(|record| record.status == "Queued").count();
    let retry_count = pending.iter().filter(|record| record.status == "Waiting for retry").count();
    let scheduled_count =
        pending.iter().filter(|record| record.status == "Waiting for schedule").count();
    let paused_count = pending.iter().filter(|record| record.status == "Paused").count();
    let mut queue_labels = Vec::new();
    if queued_count > 0 {
        queue_labels.push(format!("{queued_count} queued"));
    }
    if retry_count > 0 {
        queue_labels.push(format!("{retry_count} waiting for retry"));
    }
    if scheduled_count > 0 {
        queue_labels.push(format!("{scheduled_count} scheduled"));
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

fn queue_action_state(records: &[DownloadRecord]) -> QueueActionState {
    let mut state = QueueActionState::default();
    for record in records {
        match record.status.as_str() {
            "Queued" | "Waiting for retry" | "Waiting for schedule" => {
                state.can_pause_all = true;
                state.can_cancel_all = true;
            }
            "Analyzing" | "Downloading" | "Post-processing" | "Pausing" => {
                state.can_pause_all = true;
                state.can_cancel_all = true;
            }
            "Canceling" => state.can_cancel_all = true,
            "Paused" => {
                state.can_resume_all = true;
                state.can_cancel_all = true;
            }
            "Failed" | "Interrupted" => state.can_retry_failed = true,
            _ => {}
        }
    }
    state
}

fn row_view_with_queue_position(
    record: &DownloadRecord,
    queue_position: &str,
    group_action: &str,
    group_id: &str,
) -> DownloadRowData {
    let details = if record.status == "Paused" && record.scheduled_at.is_some() {
        let scheduled = record.scheduled_at.map(format_local_schedule).unwrap_or_default();
        format!("Paused · planned for {scheduled} local time; resume manually")
    } else if record.status == "Waiting for schedule" && record.scheduled_at.is_some() {
        let scheduled = record.scheduled_at.map(format_local_schedule).unwrap_or_default();
        format!("Starts {scheduled} local time · The app must be running")
    } else if record.status == "Waiting for schedule" {
        "Scheduled time unavailable".to_owned()
    } else if record.status == "Waiting for retry" {
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
    let selection = match (record.download_mode, record.selected_format_id.as_deref()) {
        (DownloadMode::Video, Some(format_id)) => format!("Video + audio · Format {format_id}"),
        (DownloadMode::VideoOnly, Some(format_id)) => format!("Video only · Format {format_id}"),
        (DownloadMode::AudioOnly, Some(format_id))
            if record.audio_format.supports_quality_setting() =>
        {
            format!(
                "Audio · {} · {} · Format {format_id}",
                record.audio_format.label(),
                record.audio_quality.label()
            )
        }
        (DownloadMode::AudioOnly, Some(format_id)) => {
            format!("Audio · {} · Format {format_id}", record.audio_format.label())
        }
        (DownloadMode::Video, None) => format!("Video · {}", record.video_quality.label()),
        (DownloadMode::VideoOnly, None) => {
            format!("Video only · {}", record.video_quality.label())
        }
        (DownloadMode::AudioOnly, None) if record.audio_format.supports_quality_setting() => {
            format!("Audio · {} · {}", record.audio_format.label(), record.audio_quality.label())
        }
        (DownloadMode::AudioOnly, None) => format!("Audio · {}", record.audio_format.label()),
        (DownloadMode::SubtitlesOnly, _) => format!(
            "Subtitles · {} · {}{}",
            record.subtitle_languages,
            record.subtitle_format.as_database_value().to_uppercase(),
            if record.include_auto_subtitles { " · auto captions" } else { " · manual captions" }
        ),
        (DownloadMode::MetadataOnly, _) => "Metadata · JSON".to_owned(),
        (DownloadMode::ThumbnailOnly, _) => "Thumbnail · Image".to_owned(),
    };
    let speed_limit = if record.download_speed_limit_bytes_per_second == 0 {
        String::new()
    } else {
        format!(
            " · Max {}",
            download_speed_limit_label(record.download_speed_limit_bytes_per_second)
        )
    };
    let playlist_group = record
        .playlist_group_title
        .as_deref()
        .filter(|title| !title.is_empty())
        .map(|title| format!("Playlist · {title} · "))
        .unwrap_or_default();
    let details = format!("{playlist_group}{selection}{speed_limit} · {details}");
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
        playlist_group_id: SharedString::from(group_id),
        playlist_group_action: SharedString::from(group_action),
    }
}

fn media_format_choice_labels(
    formats: &[MediaFormatOption],
    mode: DownloadMode,
) -> Vec<SharedString> {
    let mut labels = vec![SharedString::from(AUTOMATIC_FORMAT_CHOICE)];
    let mut seen = HashSet::new();
    for format in formats.iter().filter(|format| format.supports_mode(mode)) {
        if seen.insert(format.id.as_str()) && labels.len() <= 100 {
            labels.push(format.choice_label().into());
        }
    }
    labels
}

fn selected_format_id_from_ui(
    selection: &str,
    analyzed_url: &str,
    analyzed_mode: &str,
    queued_url: &str,
    queued_mode: DownloadMode,
) -> std::result::Result<Option<String>, &'static str> {
    if selection == AUTOMATIC_FORMAT_CHOICE {
        return Ok(None);
    }
    if analyzed_url != queued_url || analyzed_mode != queued_mode.as_database_value() {
        return Err(
            "Analyze this link again in the selected download mode before choosing an exact format.",
        );
    }
    let Some((format_id, _)) = selection.split_once(" · ") else {
        return Err(
            "The selected format is invalid. Analyze the link again and choose another format.",
        );
    };
    if !is_safe_format_id(format_id) {
        return Err(
            "The selected format is invalid. Analyze the link again and choose another format.",
        );
    }
    Ok(Some(format_id.to_owned()))
}

fn saved_download_preset_model(presets: &[NamedDownloadPreset]) -> ModelRc<SharedString> {
    ModelRc::new(VecModel::from(
        presets
            .iter()
            .map(|preset| SharedString::from(preset.name.as_str()))
            .collect::<Vec<_>>(),
    ))
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn parse_local_schedule(value: &str, current_timestamp: i64) -> Result<i64> {
    let local_datetime = chrono::NaiveDateTime::parse_from_str(value.trim(), "%Y-%m-%d %H:%M")
        .context("Enter a local date and time as YYYY-MM-DD HH:MM.")?;
    let local_datetime = match local_datetime.and_local_timezone(chrono::Local) {
        chrono::LocalResult::Single(datetime) => datetime,
        chrono::LocalResult::Ambiguous(_, _) => {
            anyhow::bail!(
                "That local time occurs twice during a daylight-saving change; choose another time."
            );
        }
        chrono::LocalResult::None => {
            anyhow::bail!(
                "That local time does not exist during a daylight-saving change; choose another time."
            );
        }
    };
    let timestamp = local_datetime.timestamp();
    if timestamp <= current_timestamp {
        anyhow::bail!("Choose a future local date and time.");
    }
    Ok(timestamp)
}

fn format_local_schedule(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|datetime| {
            datetime.with_timezone(&chrono::Local).format("%b %d, %Y %H:%M").to_string()
        })
        .unwrap_or_else(|| "an unknown time".to_owned())
}

fn app_update_cache_directory() -> Result<PathBuf> {
    let project_dirs =
        ProjectDirs::from("com", env!("YT_SAVE_PUBLISHER"), env!("YT_SAVE_PRODUCT_NAME"))
            .context("determine the per-user update cache directory")?;
    let directory = project_dirs.cache_dir().join("app-updates");
    std::fs::create_dir_all(&directory)
        .with_context(|| format!("create application update cache {}", directory.display()))?;
    Ok(directory)
}

fn app_update_notes_preview(body: &str) -> String {
    let mut text = body
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(8)
        .collect::<Vec<_>>()
        .join(" · ");
    text.retain(|character| !character.is_control() || character == '\t');
    const MAX_NOTES_CHARS: usize = 520;
    if text.chars().count() > MAX_NOTES_CHARS {
        text = text.chars().take(MAX_NOTES_CHARS).collect::<String>();
        text.push('…');
    }
    text
}

fn installer_kind_label(kind: InstallerKind) -> &'static str {
    match kind {
        InstallerKind::Msi => "MSI",
        InstallerKind::SetupExe | InstallerKind::SetupExeCurrentUser => "Inno Setup",
    }
}

fn open_release_page(value: &str) -> Result<()> {
    let url = url::Url::parse(value).context("parse the application release page URL")?;
    anyhow::ensure!(
        url.scheme() == "https"
            && url.host_str() == Some("github.com")
            && url.path().starts_with(&format!(
                "/{}/{}/releases/tag/",
                env!("YT_SAVE_GITHUB_OWNER"),
                env!("YT_SAVE_GITHUB_REPOSITORY")
            )),
        "the application release page did not match the configured GitHub repository"
    );
    #[cfg(windows)]
    let mut command = {
        let mut command = Command::new("cmd.exe");
        command.args(["/C", "start", "", url.as_str()]);
        command
    };
    #[cfg(not(windows))]
    let mut command = {
        let mut command = Command::new("xdg-open");
        command.arg(url.as_str());
        command
    };
    command.spawn().context("open the official application release page")?;
    Ok(())
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

fn playlist_folder_component(title: &str, subscription_id: &str) -> String {
    let mut component = title
        .chars()
        .map(|character| {
            if character.is_control() || "<>:\"/\\|?*".contains(character) {
                '_'
            } else {
                character
            }
        })
        .take(80)
        .collect::<String>();
    component =
        component.trim().trim_matches(|character| character == '.' || character == ' ').to_owned();
    if component.is_empty() || component == "." || component == ".." {
        component = "Playlist".to_owned();
    }
    let device_name = component.split('.').next().unwrap_or_default().to_ascii_uppercase();
    let reserved_device = matches!(device_name.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (device_name.len() == 4
            && (device_name.starts_with("COM") || device_name.starts_with("LPT"))
            && device_name.as_bytes()[3].is_ascii_digit()
            && device_name.as_bytes()[3] != b'0');
    if reserved_device {
        component.insert(0, '_');
    }
    let suffix = subscription_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .take(32)
        .collect::<String>();
    if suffix.is_empty() {
        format!("{component} [playlist]")
    } else {
        format!("{component} [{suffix}]")
    }
}

fn create_playlist_subscription_directory(
    root: &Path,
    playlist_title: &str,
    subscription_id: &str,
) -> Result<PathBuf> {
    let root = validate_download_directory(root)?;
    let destination = root.join(playlist_folder_component(playlist_title, subscription_id));
    std::fs::create_dir_all(&destination)
        .with_context(|| format!("create playlist folder {}", destination.display()))?;
    validate_download_directory(&destination)
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
        "Download files\0",
        "*.3g2;*.3gp;*.aac;*.ac3;*.aif;*.aiff;*.alac;*.amr;*.ape;*.asf;*.au;*.avi;*.avif;*.bmp;*.caf;*.flac;*.flv;*.gif;*.jpeg;*.jpg;*.m2ts;*.m4a;*.m4b;*.m4v;*.mka;*.mkv;*.mov;*.mp2;*.mp3;*.mp4;*.mpeg;*.mpg;*.oga;*.ogg;*.ogv;*.opus;*.png;*.ra;*.rm;*.srt;*.ts;*.vtt;*.wav;*.webm;*.webp;*.wma;*.wmv\0\0"
    )
    .encode_utf16()
    .collect();
    let title: Vec<u16> = "Locate moved download file".encode_utf16().chain(Some(0)).collect();
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
            | "avif"
            | "bmp"
            | "caf"
            | "flac"
            | "flv"
            | "gif"
            | "jpeg"
            | "jpg"
            | "json"
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
            | "png"
            | "ra"
            | "rm"
            | "ts"
            | "wav"
            | "webm"
            | "webp"
            | "wma"
            | "wmv"
            | "srt"
            | "vtt"
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
    data_local: PathBuf,
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
        let data_local = project_dirs.data_local_dir().to_owned();
        Ok(Self { database: data_local.join("library.sqlite3"), downloads, data_local })
    }
}

#[cfg(test)]
mod tests;
