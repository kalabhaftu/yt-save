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

#[cfg(windows)]
use std::{cell::RefCell, rc::Rc};

use anyhow::{Context, Result};
use directories::{ProjectDirs, UserDirs};
use slint::{CloseRequestResponse, ComponentHandle, ModelRc, SharedString, VecModel};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, mpsc, watch},
    task::JoinSet,
};
use tracing::{error, info, warn};
use uuid::Uuid;

const AUTOMATIC_FORMAT_CHOICE: &str = "Automatic (quality preset)";

use crate::{
    AppTray, ClipboardPopup, DownloadRowData, LibraryRowData, MainWindow, PlaylistEntryRowData,
    PlaylistSubscriptionEntryRowData, PlaylistSubscriptionRowData,
    app_update::{AppRelease, AppUpdateChannel, CheckOutcome, InstallerKind},
    clipboard_monitor::{ClipboardCandidate, ClipboardMonitor, WorkArea},
    cookie_auth::{CookieRecord, CookieStore},
    dependencies::DependencyPaths,
    engine::{
        DownloadEngine, DownloadOptions, EngineOutcome, JobSignal, PLAYLIST_PAGE_SIZE,
        ProgressUpdate, is_retryable_engine_error,
    },
    history::download_history_csv,
    model::{
        DownloadMode, DownloadPresets, DownloadRecord, MediaFormatOption, MediaMetadata,
        PlaylistEntry, PlaylistMediaEntry, SubtitleFormat, download_speed_limit_from_label,
        download_speed_limit_label, format_bytes, format_duration, format_is_available,
        is_safe_format_id, normalize_subtitle_languages, normalized_host_key,
        subtitle_languages_available,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct QueueActionState {
    can_pause_all: bool,
    can_resume_all: bool,
    can_cancel_all: bool,
    can_retry_failed: bool,
}

struct TrayContext {
    store: Store,
    handle: tokio::runtime::Handle,
    events: mpsc::UnboundedSender<UiEvent>,
    active_jobs: ActiveJobs,
    queue_wake: Arc<Notify>,
    preferences: Arc<Mutex<TrayPreferences>>,
    shutdown_started: Arc<AtomicBool>,
    shutdown_committed: Arc<AtomicBool>,
}

struct ShutdownState {
    started: Arc<AtomicBool>,
    committed: Arc<AtomicBool>,
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
                available: entry.available,
            })
        })
        .collect()
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
) -> Result<usize> {
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
    store
        .record_playlist_subscription_check(
            subscription_id.to_owned(),
            metadata.title,
            metadata.source,
            now(),
            entries,
        )
        .await
}

async fn run_playlist_subscription_scheduler(
    store: Store,
    engine: DownloadEngine,
    review_state: Arc<Mutex<PlaylistSubscriptionReviewState>>,
    check_active: Arc<AtomicBool>,
    weak_window: slint::Weak<MainWindow>,
    events: mpsc::UnboundedSender<UiEvent>,
    shutdown_started: Arc<AtomicBool>,
) {
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
        for subscription in due_subscriptions {
            if shutdown_started.load(Ordering::Acquire) {
                break;
            }
            match perform_playlist_subscription_check(&store, &engine, &subscription.id).await {
                Ok(count) => {
                    checked += 1;
                    new_entries = new_entries.saturating_add(count);
                }
                Err(error) => {
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

        if checked > 0 || failed > 0 {
            let message = format!(
                "Scheduled playlist checks: {checked} completed, {failed} failed, {new_entries} new {}. New entries wait for your review.",
                plural_word(new_entries, "entry", "entries")
            );
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
    let (cookie_support_enabled, cookie_support_status) = cookie_webview_runtime_status();
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
    window.set_audio_format(download_presets.audio_format.as_database_value().into());
    window.set_audio_quality(download_presets.audio_quality.as_database_value().into());
    window.set_video_quality(download_presets.video_quality.label().into());
    window.set_max_concurrent_downloads(max_concurrent_downloads.to_string().into());
    window.set_max_concurrent_per_host(max_concurrent_per_host.to_string().into());
    window.set_concurrent_fragments(concurrent_fragments.to_string().into());
    window.set_download_speed_limit(
        download_speed_limit_label(download_presets.download_speed_limit_bytes_per_second).into(),
    );
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
    configure_clipboard_popup_window(&popup, clipboard_popup_work_area.clone());
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
                    let details = format!(
                        "{} · {}{}{}{}",
                        metadata.source,
                        format_duration(metadata.duration_seconds),
                        format_summary,
                        playlist_summary,
                        subtitle_summary,
                    );
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
                        Err(error) => format!("Could not load playlist entries: {error}"),
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

    #[cfg(windows)]
    configure_cookie_auth(CookieAuthContext {
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
    let save_subscription_handle = handle.clone();
    let save_subscription_shutdown = shutdown_started.clone();
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
            match perform_playlist_subscription_check(&store, &engine, &subscription_id).await {
                Ok(new_count) => {
                    let message = if new_count == 0 {
                        "Playlist checked. No new entries were found.".to_owned()
                    } else {
                        format!("Found {new_count} new playlist {}. Review them before queueing.", plural_word(new_count, "entry", "entries"))
                    };
                    let _ = events.send(UiEvent::Status(message));
                }
                Err(error) => {
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
    let check_subscription_handle = handle.clone();
    let check_subscription_active = playlist_subscription_check_active.clone();
    window.on_playlist_subscription_check_requested(move |id| {
        let id = id.to_string();
        let store = check_subscription_store.clone();
        let engine = check_subscription_engine.clone();
        let review = check_subscription_review.clone();
        let weak_window = check_subscription_window.clone();
        let events = check_subscription_events.clone();
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
            match perform_playlist_subscription_check(&store, &engine, &id).await {
                Ok(new_count) => {
                    let message = if new_count == 0 {
                        "Playlist checked. No new entries were found.".to_owned()
                    } else {
                        format!(
                            "Found {new_count} new playlist {}. Review them before queueing.",
                            plural_word(new_count, "entry", "entries")
                        )
                    };
                    let _ = events.send(UiEvent::Status(message));
                }
                Err(error) => {
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
        let presets = DownloadPresets::from_ui_values(
            audio_only,
            video_only,
            &audio_format,
            &audio_quality,
            &video_quality,
        )
        .with_download_speed_limit_label(&speed_limit_label);
        let store = queue_subscription_store.clone();
        let review = queue_subscription_review.clone();
        let weak_window = queue_subscription_window.clone();
        let events = queue_subscription_events.clone();
        let queue_wake = queue_subscription_wake.clone();
        let handle = queue_subscription_handle.clone();
        let dispatch_gate = queue_subscription_dispatch_gate.clone();
        let shutdown = queue_subscription_shutdown.clone();
        handle.spawn(async move {
            let destination = match tokio::task::spawn_blocking(move || {
                validate_download_directory(Path::new(&directory))
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
                    if let Err(error) = store.set_download_presets(presets, now()).await {
                        error!(%error, "save new download presets after subscription queue");
                    }
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
    let scheduled_subscription_shutdown = shutdown_started.clone();
    handle.spawn(run_playlist_subscription_scheduler(
        scheduled_subscription_store,
        scheduled_subscription_engine,
        scheduled_subscription_review,
        scheduled_subscription_active,
        scheduled_subscription_window,
        scheduled_subscription_events,
        scheduled_subscription_shutdown,
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
        let download_presets = DownloadPresets::from_ui_values(
            audio_only,
            video_only,
            &selected_format,
            &selected_quality,
            &selected_video_quality,
        )
        .with_download_speed_limit_label(&selected_speed_limit);
        record.download_mode = if subtitle_only {
            DownloadMode::SubtitlesOnly
        } else {
            download_presets.mode
        };
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
            let preset_status = match store.set_download_presets(download_presets, now()).await {
                Ok(()) => schedule_status,
                Err(error) => {
                    format!("{schedule_status} Could not save the New download presets: {error}")
                }
            };
            let _ = events.send(UiEvent::Refresh);
            let _ = events.send(UiEvent::Status(preset_status));
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
        let download_presets = DownloadPresets::from_ui_values(
            audio_only,
            video_only,
            &selected_format,
            &selected_quality,
            &selected_video_quality,
        )
        .with_download_speed_limit_label(&selected_speed_limit);
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
                presets: if subtitle_only {
                    DownloadPresets { mode: DownloadMode::SubtitlesOnly, ..download_presets }
                } else {
                    download_presets
                },
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
                    Err(_) => {
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
            let preset_save_error = store.set_download_presets(download_presets, now()).await.err();
            let mut status = batch_result_status(queued_count, total_count, &issues, scheduled_at);
            if let Some(error) = preset_save_error {
                status.push_str(&format!(" New download presets could not be saved: {error}"));
            }
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
        let download_presets = DownloadPresets::from_ui_values(
            audio_only,
            video_only,
            &selected_format,
            &selected_quality,
            &selected_video_quality,
        )
        .with_download_speed_limit_label(&selected_speed_limit);
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
            let preset_save_error = store.set_download_presets(download_presets, now()).await.err();
            let _ = events.send(UiEvent::Refresh);
            let mut status = scheduled_at.map_or_else(
                || format!("Queued {selected_count} selected playlist entries. Each item will download separately."),
                |timestamp| {
                    format!("Scheduled {selected_count} playlist entries for {} local time.", format_local_schedule(timestamp))
                },
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

    let speed_limit_store = store.clone();
    let speed_limit_events = events_tx.clone();
    let speed_limit_handle = handle.clone();
    let speed_limit_generation = Arc::new(AtomicU64::new(0));
    let speed_limit_write_lock = Arc::new(AsyncMutex::new(()));
    let speed_limit_window = window.as_weak();
    window.on_download_speed_limit_requested(move |label| {
        let bytes_per_second = download_speed_limit_from_label(&label);
        let generation_state = speed_limit_generation.clone();
        let generation = generation_state.fetch_add(1, Ordering::AcqRel) + 1;
        let write_lock = speed_limit_write_lock.clone();
        let store = speed_limit_store.clone();
        let events = speed_limit_events.clone();
        let weak_window = speed_limit_window.clone();
        speed_limit_handle.spawn(async move {
            let _write_guard = write_lock.lock().await;
            if generation_state.load(Ordering::Acquire) != generation {
                return;
            }
            let previous = store
                .load_download_presets()
                .await
                .map(|presets| presets.download_speed_limit_bytes_per_second)
                .unwrap_or_default();
            if generation_state.load(Ordering::Acquire) != generation {
                return;
            }
            match store.set_download_speed_limit(bytes_per_second, now()).await {
                Ok(()) => {
                    let label = download_speed_limit_label(bytes_per_second);
                    let _ = events.send(UiEvent::Status(if bytes_per_second == 0 {
                        "Download speed is unlimited for new queue items.".to_owned()
                    } else {
                        format!("New queue items are limited to {label}.")
                    }));
                }
                Err(error) => {
                    error!(%error, "save download speed limit");
                    if generation_state.load(Ordering::Acquire) == generation {
                        let previous_value = download_speed_limit_label(previous).to_owned().into();
                        let _ = slint::invoke_from_event_loop(move || {
                            if let Some(window) = weak_window.upgrade() {
                                window.set_download_speed_limit(previous_value);
                            }
                        });
                    }
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not save the download speed limit: {error}"
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
            shutdown_committed,
        },
    );

    info!(product = env!("YT_SAVE_PRODUCT_NAME"), "application started");
    window.run().context("run Slint event loop")?;
    let _ = stop_clipboard_monitor(&clipboard_monitor_slot, &clipboard_monitor_generation);
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

#[cfg(windows)]
fn configure_cookie_auth(context: CookieAuthContext<'_>) {
    let CookieAuthContext {
        main_window,
        login_window,
        cookie_store,
        handle,
        events,
        generation,
        login_session,
        shutdown_started,
    } = context;
    main_window.on_cookie_runtime_download_requested(|| {
        if let Err(error) = open_webview2_runtime_page() {
            error!(%error, "open Microsoft WebView2 Runtime download page");
        }
    });

    let webview_slot = Rc::new(RefCell::new(None::<wry::WebView>));
    let login_url_slot = Rc::new(RefCell::new(None::<String>));

    let state_store = cookie_store.clone();
    let state_handle = handle.clone();
    let state_events = events.clone();
    let state_generation = generation.clone();
    main_window.on_cookie_vault_state_requested(move |requested_url| {
        let url = requested_url.to_string();
        let request_generation = state_generation.fetch_add(1, Ordering::AcqRel) + 1;
        if validate_cookie_sign_in_url(&url).is_err() {
            let _ = state_events.send(UiEvent::CookieAuthState {
                generation: request_generation,
                url,
                has_saved: Some(false),
                message: None,
                close_login: false,
                login_session: None,
                refresh_current_url: false,
            });
            return;
        }

        let store = state_store.clone();
        let events = state_events.clone();
        state_handle.spawn_blocking(move || {
            let (has_saved, message) = match store.has_saved_for_url(&url) {
                Ok(saved) => (saved, None),
                Err(_) => (
                    true,
                    Some(
                        "Saved sign-in data could not be read. Clear it and sign in again."
                            .to_owned(),
                    ),
                ),
            };
            let _ = events.send(UiEvent::CookieAuthState {
                generation: request_generation,
                url,
                has_saved: Some(has_saved),
                message,
                close_login: false,
                login_session: None,
                refresh_current_url: false,
            });
        });
    });

    let clear_store = cookie_store.clone();
    let clear_handle = handle.clone();
    let clear_events = events.clone();
    let clear_generation = generation.clone();
    main_window.on_cookie_clear_requested(move |requested_url| {
        let url = requested_url.to_string();
        let request_generation = clear_generation.fetch_add(1, Ordering::AcqRel) + 1;
        if validate_cookie_sign_in_url(&url).is_err() {
            let _ = clear_events.send(UiEvent::CookieAuthState {
                generation: request_generation,
                url,
                has_saved: Some(false),
                message: Some("Enter a secure HTTPS media URL first.".to_owned()),
                close_login: false,
                login_session: None,
                refresh_current_url: false,
            });
            return;
        }

        let store = clear_store.clone();
        let events = clear_events.clone();
        let reported_url = url.clone();
        let operation = clear_handle.spawn_blocking(move || store.clear_for_url(&url));
        clear_handle.spawn(async move {
            let outcome = operation.await;
            let (has_saved, message) = match outcome {
                Ok(Ok(_)) => (Some(false), Some("Saved sign-in removed.".to_owned())),
                _ => {
                    (Some(true), Some("Saved sign-in could not be removed. Try again.".to_owned()))
                }
            };
            let _ = events.send(UiEvent::CookieAuthState {
                generation: request_generation,
                url: reported_url,
                has_saved,
                message,
                close_login: false,
                login_session: None,
                refresh_current_url: false,
            });
        });
    });

    let sign_in_window = login_window.as_weak();
    let sign_in_main = main_window.as_weak();
    let sign_in_webview = webview_slot.clone();
    let sign_in_url = login_url_slot.clone();
    let sign_in_session = login_session.clone();
    let sign_in_shutdown = shutdown_started.clone();
    main_window.on_cookie_sign_in_requested(move |submitted_url| {
        if sign_in_shutdown.load(Ordering::Acquire) {
            if let Some(window) = sign_in_main.upgrade() {
                window.set_status_message("The application is shutting down.".into());
            }
            return;
        }
        let target_url = submitted_url.to_string();
        let (normalized_url, host) = match validate_cookie_sign_in_url(&target_url) {
            Ok(target) => target,
            Err(_) => {
                if let Some(window) = sign_in_main.upgrade() {
                    window.set_status_message(
                        "Enter a secure HTTPS media URL before signing in.".into(),
                    );
                }
                return;
            }
        };

        if sign_in_window.upgrade().is_some_and(|login| login.get_saving()) {
            if let Some(window) = sign_in_main.upgrade() {
                window.set_status_message("Sign-in cookies are still being protected. Please wait.".into());
            }
            return;
        }
        sign_in_session.fetch_add(1, Ordering::AcqRel);

        discard_cookie_webview(&sign_in_webview);
        *sign_in_url.borrow_mut() = Some(normalized_url.clone());
        let Some(login) = sign_in_window.upgrade() else {
            return;
        };
        login.set_site_label(host.into());
        login.set_login_status(
            "Sign in on the page below, then choose Save cookies. Nothing is saved if you cancel."
                .into(),
        );
        login.set_saving(false);
        login.window().set_size(slint::LogicalSize::new(920.0, 720.0));
        if let Err(error) = login.show() {
            error!(%error, "show embedded sign-in window");
            if let Some(window) = sign_in_main.upgrade() {
                window.set_status_message("The sign-in window could not be opened.".into());
            }
            return;
        }

        use slint::winit_030::WinitWindowAccessor;
        let scale = login.window().scale_factor();
        let bounds = wry::Rect {
            position: wry::dpi::Position::Physical(wry::dpi::PhysicalPosition::new(
                0,
                (108.0 * scale).round() as i32,
            )),
            size: wry::dpi::Size::Physical(wry::dpi::PhysicalSize::new(
                (920.0 * scale).round() as u32,
                (500.0 * scale).round() as u32,
            )),
        };
        let built = login.window().with_winit_window(|native_window| {
            wry::WebViewBuilder::new()
                .with_url(normalized_url)
                .with_bounds(bounds)
                .with_incognito(true)
                .with_devtools(false)
                .with_autoplay(false)
                .with_navigation_handler(is_secure_web_navigation)
                .with_new_window_req_handler(|_, _| wry::NewWindowResponse::Deny)
                .with_permission_handler(|_| wry::PermissionResponse::Deny)
                .with_download_started_handler(|_, _| false)
                .build_as_child(native_window)
        });
        match built {
            Some(Ok(webview)) => {
                *sign_in_webview.borrow_mut() = Some(webview);
                if let Some(webview) = sign_in_webview.borrow().as_ref()
                    && let Err(error) = webview.focus()
                {
                    error!(%error, "focus embedded sign-in view");
                }
            }
            Some(Err(_)) | None => {
                *sign_in_url.borrow_mut() = None;
                let _ = login.hide();
                if let Some(window) = sign_in_main.upgrade() {
                    window.set_status_message(
                        "The embedded sign-in view could not start. Check that Microsoft Edge WebView2 Runtime is installed.".into(),
                    );
                }
            }
        }
    });

    let save_login = login_window.as_weak();
    let save_webview = webview_slot.clone();
    let save_url = login_url_slot.clone();
    let save_store = cookie_store.clone();
    let save_handle = handle.clone();
    let save_events = events.clone();
    let save_generation = generation.clone();
    let save_session = login_session.clone();
    login_window.on_save_requested(move || {
        let Some(target_url) = save_url.borrow().clone() else {
            if let Some(login) = save_login.upgrade() {
                login.set_login_status("Open a site before saving sign-in cookies.".into());
            }
            return;
        };
        let cookie_records = {
            let borrowed = save_webview.borrow();
            let Some(webview) = borrowed.as_ref() else {
                if let Some(login) = save_login.upgrade() {
                    login.set_login_status("The sign-in view is no longer active. Close it and try again.".into());
                }
                return;
            };
            let cookies = match webview.cookies_for_url(&target_url) {
                Ok(cookies) => cookies,
                Err(_) => {
                    if let Some(login) = save_login.upgrade() {
                        login.set_login_status("Yt Save could not read cookies for this URL. Finish sign-in and try again.".into());
                    }
                    return;
                }
            };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64)
                .unwrap_or_default();
            cookies
                .into_iter()
                .filter_map(|cookie| {
                    let domain = cookie.domain_raw()?.to_owned();
                    let expires = cookie.expires_datetime().map(|date| date.unix_timestamp()).unwrap_or(0);
                    if expires != 0 && expires <= now {
                        return None;
                    }
                    Some(CookieRecord {
                        domain,
                        path: cookie.path().unwrap_or("/").to_owned(),
                        name: cookie.name().to_owned(),
                        value: cookie.value().to_owned(),
                        secure: cookie.secure().unwrap_or(false),
                        http_only: cookie.http_only().unwrap_or(false),
                        expires,
                    })
                })
                .collect::<Vec<_>>()
        };
        if cookie_records.is_empty() {
            if let Some(login) = save_login.upgrade() {
                login.set_login_status(
                    "No unexpired cookies were found for this URL. Complete sign-in, then try again."
                        .into(),
                );
            }
            return;
        }

        *save_url.borrow_mut() = None;
        discard_cookie_webview(&save_webview);
        if let Some(login) = save_login.upgrade() {
            login.set_saving(true);
            login.set_login_status("Protecting your sign-in cookies with Windows DPAPI…".into());
        }
        let request_generation = save_generation.load(Ordering::Acquire);
        let request_login_session = save_session.load(Ordering::Acquire);
        let store = save_store.clone();
        let target_for_save = target_url.clone();
        let operation = save_handle.spawn_blocking(move || {
            store.save_for_url(&target_for_save, &cookie_records)
        });
        let events = save_events.clone();
        save_handle.spawn(async move {
            let succeeded = matches!(operation.await, Ok(Ok(())));
            let _ = events.send(UiEvent::CookieAuthState {
                generation: request_generation,
                url: target_url,
                has_saved: Some(succeeded),
                message: Some(if succeeded {
                    "Saved sign-in cookies for this site. They will be used for matching HTTPS URLs.".to_owned()
                } else {
                    "Sign-in cookies could not be saved. Sign in again and retry.".to_owned()
                }),
                close_login: true,
                login_session: Some(request_login_session),
                refresh_current_url: true,
            });
        });
    });

    let cancel_webview = webview_slot.clone();
    let cancel_url = login_url_slot.clone();
    let cancel_login = login_window.as_weak();
    let cancel_session = login_session.clone();
    login_window.on_cancel_requested(move || {
        cancel_session.fetch_add(1, Ordering::AcqRel);
        discard_cookie_webview(&cancel_webview);
        *cancel_url.borrow_mut() = None;
        if let Some(login) = cancel_login.upgrade()
            && let Err(error) = login.hide()
        {
            error!(%error, "cancel embedded sign-in");
        }
    });

    let back_webview = webview_slot.clone();
    login_window.on_back_requested(move || {
        if let Some(webview) = back_webview.borrow().as_ref()
            && webview.can_go_back().unwrap_or(false)
            && let Err(error) = webview.go_back()
        {
            error!(%error, "navigate embedded sign-in view backward");
        }
    });

    let forward_webview = webview_slot.clone();
    login_window.on_forward_requested(move || {
        if let Some(webview) = forward_webview.borrow().as_ref()
            && webview.can_go_forward().unwrap_or(false)
            && let Err(error) = webview.go_forward()
        {
            error!(%error, "navigate embedded sign-in view forward");
        }
    });

    let reload_webview = webview_slot.clone();
    login_window.on_reload_requested(move || {
        if let Some(webview) = reload_webview.borrow().as_ref()
            && let Err(error) = webview.reload()
        {
            error!(%error, "reload embedded sign-in view");
        }
    });

    let close_webview = webview_slot.clone();
    let close_url = login_url_slot.clone();
    let close_login = login_window.as_weak();
    let close_session = login_session.clone();
    login_window.window().on_close_requested(move || {
        if close_login.upgrade().is_some_and(|login| login.get_saving()) {
            return CloseRequestResponse::KeepWindowShown;
        }
        close_session.fetch_add(1, Ordering::AcqRel);
        discard_cookie_webview(&close_webview);
        *close_url.borrow_mut() = None;
        CloseRequestResponse::HideWindow
    });
}

#[cfg(windows)]
fn cookie_webview_runtime_status() -> (bool, String) {
    match wry::webview_version() {
        Ok(version) if supports_private_webview_runtime(&version) => (true, String::new()),
        Ok(_) => (
            false,
            "Update Microsoft Edge WebView2 Runtime to enable secure site sign-in.".to_owned(),
        ),
        Err(_) => (
            false,
            "Install Microsoft Edge WebView2 Runtime to enable secure site sign-in.".to_owned(),
        ),
    }
}

#[cfg(windows)]
fn supports_private_webview_runtime(version: &str) -> bool {
    let Some(actual) = version
        .split('.')
        .take(4)
        .map(str::parse::<u64>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok()
    else {
        return false;
    };
    actual.len() == 4 && actual.as_slice() >= &[101, 0, 1210, 39]
}

#[cfg(windows)]
fn open_webview2_runtime_page() -> Result<()> {
    let mut command = Command::new("cmd.exe");
    command.args([
        "/C",
        "start",
        "",
        "https://developer.microsoft.com/en-us/microsoft-edge/webview2/",
    ]);
    command.spawn().context("launch the official WebView2 Runtime page")?;
    Ok(())
}

#[cfg(windows)]
fn validate_cookie_sign_in_url(input: &str) -> Result<(String, String)> {
    let url = url::Url::parse(input).context("Enter a complete secure HTTPS URL")?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("Sign-in cookies require a secure HTTPS URL without embedded credentials");
    }
    let host = url
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| anyhow::anyhow!("The URL must include a website host"))?
        .to_owned();
    Ok((url.to_string(), host))
}

#[cfg(windows)]
fn is_secure_web_navigation(input: String) -> bool {
    url::Url::parse(&input).is_ok_and(|url| {
        url.scheme() == "https" && url.username().is_empty() && url.password().is_none()
    })
}

#[cfg(windows)]
fn discard_cookie_webview(slot: &Rc<RefCell<Option<wry::WebView>>>) {
    if let Some(webview) = slot.borrow_mut().take() {
        let _ = webview.clear_all_browsing_data();
    }
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
        window.set_video_only(false);
        window.set_subtitle_only(false);
        window.set_format_choices(ModelRc::new(VecModel::from(vec![SharedString::from(
            AUTOMATIC_FORMAT_CHOICE,
        )])));
        window.set_selected_format(SharedString::from(AUTOMATIC_FORMAT_CHOICE));
        window.set_format_choice_mode(SharedString::default());
        window.set_format_choice_source_url(SharedString::default());
        window.set_format_choice_display_url(SharedString::default());
        window.set_format_choice_list_truncated(false);
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
        shutdown_committed,
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
    let quit_committed = shutdown_committed.clone();
    tray.on_quit_requested(move || {
        begin_orderly_quit(
            quit_handle.clone(),
            quit_store.clone(),
            quit_active_jobs.clone(),
            quit_queue_wake.clone(),
            quit_events.clone(),
            quit_window.clone(),
            ShutdownState { started: quit_guard.clone(), committed: quit_committed.clone() },
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
    let close_committed = shutdown_committed;
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
                ShutdownState { started: close_guard.clone(), committed: close_committed.clone() },
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

fn cancel_staged_update_helper(
    helper_state: &Arc<Mutex<Option<crate::app_update::UpdateHelper>>>,
) -> Result<()> {
    let mut helper_state =
        helper_state.lock().map_err(|_| anyhow::anyhow!("updater helper state is unavailable"))?;
    if let Some(helper) = helper_state.as_mut() {
        helper.cancel_and_wait()?;
    }
    helper_state.take();
    Ok(())
}

fn begin_orderly_quit(
    handle: tokio::runtime::Handle,
    store: Store,
    active_jobs: ActiveJobs,
    queue_wake: Arc<Notify>,
    events: mpsc::UnboundedSender<UiEvent>,
    window: slint::Weak<MainWindow>,
    shutdown: ShutdownState,
) {
    let shutdown_started = shutdown.started;
    let shutdown_committed = shutdown.committed;
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
                let quit_queue_wake = queue_wake.clone();
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = window.upgrade()
                        && let Err(error) = window.hide()
                    {
                        error!(%error, "hide window during orderly shutdown");
                    }
                    if let Err(error) = slint::quit_event_loop() {
                        shutdown_started.store(false, Ordering::Release);
                        shutdown_committed.store(false, Ordering::Release);
                        quit_queue_wake.notify_one();
                        error!(%error, "quit Slint event loop during orderly shutdown");
                        if let Some(window) = window.upgrade() {
                            window.set_status_message(
                                format!("Could not finish shutting down: {error}").into(),
                            );
                            let _ = window.show();
                        }
                        return;
                    }
                    shutdown_committed.store(true, Ordering::Release);
                    quit_queue_wake.notify_one();
                });
            }
            Err(error) => {
                shutdown_started.store(false, Ordering::Release);
                shutdown_committed.store(false, Ordering::Release);
                queue_wake.notify_one();
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
                DownloadMode::Video | DownloadMode::VideoOnly => "Video",
                DownloadMode::AudioOnly => "Audio",
                DownloadMode::SubtitlesOnly => "Subtitles",
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
            "Subtitles" => entry.row.kind.as_str() == "Subtitles",
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
        "*.3g2;*.3gp;*.aac;*.ac3;*.aif;*.aiff;*.alac;*.amr;*.ape;*.asf;*.au;*.avi;*.caf;*.flac;*.flv;*.m2ts;*.m4a;*.m4b;*.m4v;*.mka;*.mkv;*.mov;*.mp2;*.mp3;*.mp4;*.mpeg;*.mpg;*.oga;*.ogg;*.ogv;*.opus;*.ra;*.rm;*.srt;*.ts;*.vtt;*.wav;*.webm;*.wma;*.wmv\0\0"
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
mod tests {
    use super::*;

    #[test]
    fn active_host_counts_release_on_worker_exit() {
        let counts: ActiveHostCounts = Arc::new(Mutex::new(HashMap::new()));
        let first = ActiveHostRegistration::register("media.example".to_owned(), counts.clone());
        let second = ActiveHostRegistration::register("media.example".to_owned(), counts.clone());
        let other = ActiveHostRegistration::register("other.example".to_owned(), counts.clone());

        assert_eq!(active_host_counts_snapshot(&counts).get("media.example"), Some(&2));
        assert_eq!(active_host_counts_snapshot(&counts).get("other.example"), Some(&1));

        drop(first);
        assert_eq!(active_host_counts_snapshot(&counts).get("media.example"), Some(&1));
        drop(second);
        drop(other);
        assert!(active_host_counts_snapshot(&counts).is_empty());
    }

    #[cfg(windows)]
    #[test]
    fn private_webview_gate_requires_a_runtime_with_incognito_support() {
        assert!(supports_private_webview_runtime("101.0.1210.39"));
        assert!(supports_private_webview_runtime("146.0.3856.59"));
        assert!(!supports_private_webview_runtime("101.0.1210.38"));
        assert!(!supports_private_webview_runtime("not-a-version"));
    }

    #[test]
    fn playlist_subscription_frequency_labels_match_persisted_intervals() {
        let choices = [
            ("Manual", PLAYLIST_CHECK_INTERVAL_MANUAL),
            ("Every hour", PLAYLIST_CHECK_INTERVAL_1_HOUR),
            ("Every 6 hours", PLAYLIST_CHECK_INTERVAL_6_HOURS),
            ("Every 12 hours", PLAYLIST_CHECK_INTERVAL_12_HOURS),
            ("Daily", PLAYLIST_CHECK_INTERVAL_24_HOURS),
            ("Weekly", PLAYLIST_CHECK_INTERVAL_7_DAYS),
        ];

        for (label, seconds) in choices {
            assert_eq!(subscription_check_frequency_seconds(label), Some(seconds));
            assert_eq!(subscription_check_frequency_label(seconds), label);
        }
        assert_eq!(subscription_check_frequency_seconds("Unknown"), None);
    }

    #[test]
    fn subscription_entry_identity_falls_back_when_extractor_id_is_blank() {
        let entries = vec![
            PlaylistMediaEntry {
                media_id: Some("  ".to_owned()),
                media_url: " https://example.test/one ".to_owned(),
                playlist_index: Some(1),
                title: "One".to_owned(),
                available: true,
            },
            PlaylistMediaEntry {
                media_id: Some(" video-two ".to_owned()),
                media_url: "https://example.test/two".to_owned(),
                playlist_index: Some(2),
                title: "Two".to_owned(),
                available: true,
            },
            PlaylistMediaEntry {
                media_id: None,
                media_url: String::new(),
                playlist_index: Some(3),
                title: "Not addressable".to_owned(),
                available: false,
            },
        ];

        let inputs = playlist_subscription_entry_inputs(" Example ", &entries);
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].media_key, "url:https://example.test/one");
        assert_eq!(inputs[0].media_id, None);
        assert_eq!(inputs[0].media_url, "https://example.test/one");
        assert_eq!(inputs[1].media_key, "example:video-two");
        assert_eq!(inputs[1].media_id.as_deref(), Some("video-two"));
    }

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
            request_id: 1,
            source_url: "https://example.test/playlist".to_owned(),
            source: "Example".to_owned(),
            title: "Example playlist".to_owned(),
            entries: vec![
                PlaylistEntry { playlist_index: Some(1), title: "First".to_owned() },
                PlaylistEntry { playlist_index: None, title: "Unavailable".to_owned() },
                PlaylistEntry { playlist_index: Some(3), title: "Third".to_owned() },
            ],
            selected: vec![false, false, false],
            next_page_start: 51,
            has_more_entries: true,
            loading_page: false,
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
            request_id: 1,
            source_url: "https://example.test/playlist".to_owned(),
            source: "Example".to_owned(),
            title: "Example playlist".to_owned(),
            entries: vec![
                PlaylistEntry { playlist_index: Some(1), title: "First".to_owned() },
                PlaylistEntry { playlist_index: Some(2), title: "Second".to_owned() },
                PlaylistEntry { playlist_index: None, title: "Unavailable".to_owned() },
                PlaylistEntry { playlist_index: Some(4), title: "Fourth".to_owned() },
                PlaylistEntry { playlist_index: Some(5), title: "Fifth".to_owned() },
            ],
            selected: vec![true, false, false, false, false],
            next_page_start: 51,
            has_more_entries: true,
            loading_page: false,
        };

        assert!(select_playlist_range(&mut selection, 2, 4));
        assert_eq!(selection.selected, [true, true, false, true, false]);
        assert!(!select_playlist_range(&mut selection, 0, 2));
        assert!(!select_playlist_range(&mut selection, 5, 4));
        assert!(!select_playlist_range(&mut selection, 7, 9));
    }

    #[test]
    fn playlist_pages_keep_selection_and_ignore_duplicate_global_indexes() {
        let mut selection = PlaylistSelectionState {
            request_id: 7,
            source_url: "https://example.test/playlist".to_owned(),
            source: "Example".to_owned(),
            title: "Example playlist".to_owned(),
            entries: vec![
                PlaylistEntry { playlist_index: Some(1), title: "First".to_owned() },
                PlaylistEntry { playlist_index: Some(2), title: "Second".to_owned() },
            ],
            selected: vec![false, true],
            next_page_start: 51,
            has_more_entries: true,
            loading_page: false,
        };

        merge_playlist_page(
            &mut selection,
            vec![
                PlaylistEntry { playlist_index: Some(51), title: "Fifty first".to_owned() },
                PlaylistEntry { playlist_index: Some(52), title: "Fifty second".to_owned() },
            ],
        );
        assert!(select_playlist_range(&mut selection, 51, 52));
        merge_playlist_page(
            &mut selection,
            vec![
                PlaylistEntry { playlist_index: Some(51), title: "Duplicate page copy".to_owned() },
                PlaylistEntry { playlist_index: Some(52), title: "Fifty second".to_owned() },
                PlaylistEntry { playlist_index: Some(53), title: "Fifty third".to_owned() },
            ],
        );

        assert_eq!(
            selection.entries.iter().filter_map(|entry| entry.playlist_index).collect::<Vec<_>>(),
            [1, 2, 51, 52, 53]
        );
        assert_eq!(selection.entries[2].title, "Fifty first");
        assert_eq!(selection.selected, [false, true, true, true, false]);
        assert_eq!(selection.selected_count(), 3);
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

    #[tokio::test]
    async fn scheduled_items_stay_waiting_on_shutdown_and_pause_explicitly() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let deadline = now() + 3600;
        let mut scheduled = DownloadRecord::queued(
            "scheduled-job".to_owned(),
            "https://example.test/media".to_owned(),
            now(),
        );
        scheduled.status = "Waiting for schedule".to_owned();
        scheduled.scheduled_at = Some(deadline);
        store.save_download(scheduled).await.unwrap();
        let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::new()));

        assert_eq!(pause_all_downloads(&store, &active_jobs, false).await.unwrap().0, 0);
        let waiting = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(waiting.status, "Waiting for schedule");
        assert_eq!(waiting.scheduled_at, Some(deadline));

        assert_eq!(pause_all_downloads(&store, &active_jobs, true).await.unwrap().0, 1);
        let paused = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(paused.status, "Paused");
        assert_eq!(paused.scheduled_at, Some(deadline));

        let resumed = store.resume_download("scheduled-job", now()).await.unwrap().unwrap();
        assert_eq!(resumed.status, "Waiting for schedule");
        assert_eq!(resumed.scheduled_at, Some(deadline));
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn cancel_all_cancels_pending_rows_and_signals_active_workers() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/queued".to_owned(),
            10,
        );
        let mut retry_wait = DownloadRecord::queued(
            "retry-wait".to_owned(),
            "https://example.test/retry".to_owned(),
            11,
        );
        retry_wait.status = "Waiting for retry".to_owned();
        let mut paused = DownloadRecord::queued(
            "paused".to_owned(),
            "https://example.test/paused".to_owned(),
            12,
        );
        paused.status = "Paused".to_owned();
        let mut active = DownloadRecord::queued(
            "active".to_owned(),
            "https://example.test/active".to_owned(),
            13,
        );
        active.status = "Downloading".to_owned();
        let mut failed = DownloadRecord::queued(
            "failed".to_owned(),
            "https://example.test/failed".to_owned(),
            14,
        );
        failed.status = "Failed".to_owned();
        let mut completed = DownloadRecord::queued(
            "completed".to_owned(),
            "https://example.test/completed".to_owned(),
            15,
        );
        completed.status = "Completed".to_owned();
        store
            .save_downloads(vec![queued, retry_wait, paused, active, failed, completed])
            .await
            .unwrap();

        let (active_sender, active_signal) = watch::channel(JobSignal::Continue);
        let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::from([(
            "active".to_owned(),
            Arc::new(active_sender),
        )])));

        assert_eq!(cancel_all_downloads(&store, &active_jobs).await.unwrap(), (3, 1));
        assert_eq!(*active_signal.borrow(), JobSignal::Cancel);
        let rows = store.list_downloads().await.unwrap();
        for id in ["queued", "retry-wait", "paused"] {
            assert_eq!(rows.iter().find(|row| row.id == id).unwrap().status, "Canceled");
        }
        assert_eq!(rows.iter().find(|row| row.id == "active").unwrap().status, "Canceling");
        assert_eq!(rows.iter().find(|row| row.id == "failed").unwrap().status, "Failed");
        assert_eq!(rows.iter().find(|row| row.id == "completed").unwrap().status, "Completed");
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn retry_all_failed_requeues_only_failed_and_interrupted_rows() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut failed = DownloadRecord::queued(
            "failed".to_owned(),
            "https://example.test/failed".to_owned(),
            10,
        );
        failed.status = "Failed".to_owned();
        failed.error = Some("Temporary failure".to_owned());
        let mut interrupted = DownloadRecord::queued(
            "interrupted".to_owned(),
            "https://example.test/interrupted".to_owned(),
            11,
        );
        interrupted.status = "Interrupted".to_owned();
        let mut canceled = DownloadRecord::queued(
            "canceled".to_owned(),
            "https://example.test/canceled".to_owned(),
            12,
        );
        canceled.status = "Canceled".to_owned();
        let mut completed = DownloadRecord::queued(
            "completed".to_owned(),
            "https://example.test/completed".to_owned(),
            13,
        );
        completed.status = "Completed".to_owned();
        store.save_downloads(vec![failed, interrupted, canceled, completed]).await.unwrap();

        assert_eq!(retry_all_failed_downloads(&store).await.unwrap(), 2);
        let rows = store.list_downloads().await.unwrap();
        for id in ["failed", "interrupted"] {
            let row = rows.iter().find(|row| row.id == id).unwrap();
            assert_eq!(row.status, "Queued");
            assert_eq!(row.error, None);
        }
        assert_eq!(rows.iter().find(|row| row.id == "canceled").unwrap().status, "Canceled");
        assert_eq!(rows.iter().find(|row| row.id == "completed").unwrap().status, "Completed");
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

    #[tokio::test]
    async fn shutdown_worker_drain_resumes_when_shutdown_is_rolled_back() {
        let shutdown_started = Arc::new(AtomicBool::new(true));
        let queue_wake = Arc::new(Notify::new());
        let mut workers = JoinSet::new();
        workers.spawn(async { std::future::pending::<()>().await });

        let rollback_shutdown = shutdown_started.clone();
        let rollback_wake = queue_wake.clone();
        let rollback = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            rollback_shutdown.store(false, Ordering::Release);
            rollback_wake.notify_one();
        });

        let shutdown_committed = AtomicBool::new(false);
        let should_exit = tokio::time::timeout(
            Duration::from_secs(1),
            drain_workers_during_shutdown(
                &mut workers,
                &queue_wake,
                &shutdown_started,
                &shutdown_committed,
            ),
        )
        .await
        .expect("the shutdown rollback should wake the dispatcher");
        rollback.await.unwrap();

        assert!(!should_exit);
        assert_eq!(workers.len(), 1, "an active worker must not be discarded");
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn shutdown_worker_drain_waits_for_rollback_after_last_worker_finishes() {
        let shutdown_started = Arc::new(AtomicBool::new(true));
        let queue_wake = Arc::new(Notify::new());
        let mut workers = JoinSet::new();
        workers.spawn(async { tokio::time::sleep(Duration::from_millis(10)).await });

        let rollback_shutdown = shutdown_started.clone();
        let rollback_wake = queue_wake.clone();
        let rollback = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            rollback_shutdown.store(false, Ordering::Release);
            rollback_wake.notify_one();
        });

        let shutdown_committed = AtomicBool::new(false);
        let should_exit = tokio::time::timeout(
            Duration::from_secs(1),
            drain_workers_during_shutdown(
                &mut workers,
                &queue_wake,
                &shutdown_started,
                &shutdown_committed,
            ),
        )
        .await
        .expect("the shutdown rollback should wake an empty dispatcher");
        rollback.await.unwrap();
        assert!(!should_exit);
        assert!(workers.is_empty());
    }

    #[tokio::test]
    async fn shutdown_worker_drain_exits_after_shutdown_is_committed() {
        let shutdown_started = AtomicBool::new(true);
        let shutdown_committed = Arc::new(AtomicBool::new(false));
        let queue_wake = Arc::new(Notify::new());
        let mut workers = JoinSet::new();

        let commit = shutdown_committed.clone();
        let wake = queue_wake.clone();
        let commit_shutdown = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            commit.store(true, Ordering::Release);
            wake.notify_one();
        });

        let should_exit = tokio::time::timeout(
            Duration::from_secs(1),
            drain_workers_during_shutdown(
                &mut workers,
                &queue_wake,
                &shutdown_started,
                &shutdown_committed,
            ),
        )
        .await
        .expect("the committed shutdown should wake the dispatcher");
        commit_shutdown.await.unwrap();
        assert!(should_exit);
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
    fn queue_action_state_enables_only_actions_with_matching_rows() {
        assert_eq!(queue_action_state(&[]), QueueActionState::default());

        let mut queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/queued".to_owned(),
            10,
        );
        let mut paused = DownloadRecord::queued(
            "paused".to_owned(),
            "https://example.test/paused".to_owned(),
            11,
        );
        paused.status = "Paused".to_owned();
        let mut failed = DownloadRecord::queued(
            "failed".to_owned(),
            "https://example.test/failed".to_owned(),
            12,
        );
        failed.status = "Failed".to_owned();

        assert_eq!(
            queue_action_state(&[queued.clone(), paused.clone(), failed.clone()]),
            QueueActionState {
                can_pause_all: true,
                can_resume_all: true,
                can_cancel_all: true,
                can_retry_failed: true,
            }
        );

        queued.status = "Completed".to_owned();
        paused.status = "Completed".to_owned();
        failed.status = "Canceled".to_owned();
        assert_eq!(queue_action_state(&[queued, paused, failed]), QueueActionState::default());
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
        let subtitle_path = temp_dir.join("captions.srt");
        std::fs::write(&video_path, b"video-bytes").unwrap();
        std::fs::write(&audio_path, b"audio").unwrap();
        std::fs::write(&subtitle_path, b"subs").unwrap();

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

        let mut subtitles = DownloadRecord::queued(
            "subtitles".to_owned(),
            "https://video.example/watch/4".to_owned(),
            12,
        );
        subtitles.title = "English captions".to_owned();
        subtitles.source = "Video Example".to_owned();
        subtitles.status = "Completed".to_owned();
        subtitles.download_mode = DownloadMode::SubtitlesOnly;
        subtitles.output_path = Some(subtitle_path.to_string_lossy().into_owned());
        subtitles.updated_at = 15;

        let mut missing = DownloadRecord::queued(
            "missing".to_owned(),
            "https://video.example/watch/3".to_owned(),
            12,
        );
        missing.title = "Moved tutorial".to_owned();
        missing.status = "Completed".to_owned();
        missing.output_path = Some(temp_dir.join("moved.mp4").to_string_lossy().into_owned());
        missing.updated_at = 30;

        let records = [video, audio, subtitles, missing];
        let all = library_row_views(&records, "", "All files", "Newest first");
        assert_eq!(all.len(), 4);
        assert_eq!(all[0].id.as_str(), "missing");
        assert!(!all[0].path_available);
        assert_eq!(all[0].file_status.as_str(), "Unavailable · locate moved file");
        assert_eq!(all[1].id.as_str(), "video");
        assert!(all[1].path_available);
        assert!(all[1].size_label.as_str().contains("on disk"));
        assert_eq!(all[2].id.as_str(), "subtitles");
        assert_eq!(all[2].kind.as_str(), "Subtitles");

        let audio_only = library_row_views(&records, "audio.example", "Audio", "Title A–Z");
        assert_eq!(audio_only.len(), 1);
        assert_eq!(audio_only[0].id.as_str(), "audio");
        let subtitle_only = library_row_views(&records, "", "Subtitles", "Newest first");
        assert_eq!(subtitle_only.len(), 1);
        assert_eq!(subtitle_only[0].id.as_str(), "subtitles");
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
        assert!(has_supported_media_extension(Path::new("captions.SRT")));
        assert!(has_supported_media_extension(Path::new("captions.vtt")));
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

        let row = row_view_with_queue_position(&record, "", "", "");
        assert!(row.detail.as_str().contains(record.error.as_deref().unwrap()));
        assert!(row.detail.as_str().starts_with("Video · "));
    }

    #[test]
    fn playlist_group_controls_reflect_all_members_and_survive_queue_filtering() {
        let group_id = "playlist-group";
        let mut queued = DownloadRecord::queued(
            "queued-entry".to_owned(),
            "https://example.test/playlist".to_owned(),
            10,
        );
        queued.title = "First entry".to_owned();
        queued.status = "Queued".to_owned();
        queued.playlist_group_id = Some(group_id.to_owned());
        queued.playlist_group_title = Some("Example playlist".to_owned());
        let mut paused = DownloadRecord::queued(
            "paused-entry".to_owned(),
            "https://example.test/playlist".to_owned(),
            11,
        );
        paused.title = "Second entry".to_owned();
        paused.status = "Paused".to_owned();
        paused.playlist_group_id = Some(group_id.to_owned());
        paused.playlist_group_title = Some("Example playlist".to_owned());

        let rows = row_views_filtered(&[queued.clone(), paused.clone()], "Second entry");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].playlist_group_id.as_str(), group_id);
        assert_eq!(rows[0].playlist_group_action.as_str(), "Pause group");
        assert!(rows[0].detail.as_str().contains("Playlist · Example playlist"));

        queued.status = "Completed".to_owned();
        let rows = row_views_filtered(&[queued.clone(), paused.clone()], "Second entry");
        assert_eq!(rows[0].playlist_group_action.as_str(), "Resume group");

        paused.status = "Completed".to_owned();
        let rows = row_views_filtered(&[queued, paused], "Second entry");
        assert!(rows[0].playlist_group_action.is_empty());
        assert!(rows[0].playlist_group_id.is_empty());
    }

    #[tokio::test]
    async fn playlist_group_pause_and_resume_transitions_waiting_items_and_signals_active_jobs() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut queued = DownloadRecord::queued(
            "group-queued".to_owned(),
            "https://example.test/playlist".to_owned(),
            10,
        );
        queued.playlist_group_id = Some("group-1".to_owned());
        let mut retry = DownloadRecord::queued(
            "group-retry".to_owned(),
            "https://example.test/playlist".to_owned(),
            11,
        );
        retry.status = "Waiting for retry".to_owned();
        retry.playlist_group_id = Some("group-1".to_owned());
        let mut active = DownloadRecord::queued(
            "group-active".to_owned(),
            "https://example.test/playlist".to_owned(),
            12,
        );
        active.status = "Downloading".to_owned();
        active.playlist_group_id = Some("group-1".to_owned());
        let unrelated = DownloadRecord::queued(
            "other-queued".to_owned(),
            "https://example.test/other".to_owned(),
            13,
        );
        store.save_downloads(vec![queued, retry, active, unrelated]).await.unwrap();

        let (active_sender, active_signal) = watch::channel(JobSignal::Continue);
        let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::from([(
            "group-active".to_owned(),
            Arc::new(active_sender),
        )])));
        assert_eq!(pause_playlist_group(&store, &active_jobs, "group-1").await.unwrap(), (2, 1));
        assert_eq!(*active_signal.borrow(), JobSignal::Pause);

        let rows = store.list_downloads().await.unwrap();
        for id in ["group-queued", "group-retry"] {
            assert_eq!(rows.iter().find(|row| row.id == id).unwrap().status, "Paused");
        }
        assert_eq!(rows.iter().find(|row| row.id == "group-active").unwrap().status, "Pausing");
        assert_eq!(rows.iter().find(|row| row.id == "other-queued").unwrap().status, "Queued");

        assert_eq!(resume_playlist_group(&store, "group-1").await.unwrap(), 2);
        let rows = store.list_downloads().await.unwrap();
        for id in ["group-queued", "group-retry"] {
            assert_eq!(rows.iter().find(|row| row.id == id).unwrap().status, "Queued");
        }
        assert_eq!(rows.iter().find(|row| row.id == "group-active").unwrap().status, "Pausing");
        assert_eq!(rows.iter().find(|row| row.id == "other-queued").unwrap().status, "Queued");
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn queue_worker_downloads_fixture_and_persists_completed_media() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-e2e-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let fixture_source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("fake_yt_dlp.rs");
        let fake_yt_dlp = temp_dir.join("yt-dlp.exe");
        let compilation = Command::new("rustc")
            .arg("--edition=2024")
            .arg(&fixture_source)
            .arg("-o")
            .arg(&fake_yt_dlp)
            .output()
            .expect("launch rustc for the deterministic yt-dlp fixture");
        assert!(
            compilation.status.success(),
            "could not compile fake yt-dlp: {}",
            String::from_utf8_lossy(&compilation.stderr)
        );

        let output_directory = temp_dir.join("downloads");
        std::fs::create_dir_all(&output_directory).unwrap();
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut queued = DownloadRecord::queued(
            "fixture-job".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        queued.download_directory = output_directory.to_string_lossy().into_owned();
        store.save_download(queued).await.unwrap();
        let active = store
            .claim_queued_download_if_next("fixture-job", 11)
            .await
            .unwrap()
            .expect("fixture job should be claimed");

        let (signal_sender, signal_receiver) = watch::channel(JobSignal::Continue);
        let signal_sender = Arc::new(signal_sender);
        let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::from([(
            active.id.clone(),
            signal_sender.clone(),
        )])));
        let registration = ActiveJobRegistration {
            id: active.id.clone(),
            jobs: active_jobs,
            sender: signal_sender,
        };
        std::fs::write(temp_dir.join("ffmpeg.exe"), b"fixture ffmpeg placeholder").unwrap();
        let dependencies = DependencyPaths {
            directory: temp_dir.clone(),
            yt_dlp: fake_yt_dlp.clone(),
            deno: temp_dir.join("deno.exe"),
            ffmpeg: temp_dir.join("ffmpeg.exe"),
            ffprobe: temp_dir.join("ffprobe.exe"),
        };
        let engine = DownloadEngine::new(dependencies, output_directory.clone());
        let (events, _receiver) = mpsc::unbounded_channel();
        run_download_job(active, store.clone(), engine, events, signal_receiver, registration)
            .await;

        let completed = store
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.id == "fixture-job")
            .unwrap();
        let expected_path = output_directory.join("Fixture media [fixture-123].mp4");
        assert_eq!(completed.status, "Completed");
        assert_eq!(completed.title, "Fixture media");
        assert_eq!(completed.source, "Fixture");
        assert_eq!(completed.progress, 100.0);
        assert_eq!(completed.output_path.as_deref(), Some(expected_path.to_str().unwrap()));
        assert_eq!(std::fs::read(expected_path).unwrap(), b"tiny deterministic media fixture");

        let mut subtitle_queued = DownloadRecord::queued(
            "fixture-subtitle-job".to_owned(),
            "https://example.test/media-with-captions".to_owned(),
            12,
        );
        subtitle_queued.download_directory = output_directory.to_string_lossy().into_owned();
        subtitle_queued.download_mode = DownloadMode::SubtitlesOnly;
        subtitle_queued.subtitle_languages = "en".to_owned();
        subtitle_queued.subtitle_format = SubtitleFormat::Srt;
        subtitle_queued.include_auto_subtitles = true;
        store.save_download(subtitle_queued).await.unwrap();
        let subtitle_active = store
            .claim_queued_download_if_next("fixture-subtitle-job", 13)
            .await
            .unwrap()
            .expect("subtitle fixture job should be claimed");
        let (subtitle_sender, subtitle_receiver) = watch::channel(JobSignal::Continue);
        let subtitle_sender = Arc::new(subtitle_sender);
        let subtitle_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::from([(
            subtitle_active.id.clone(),
            subtitle_sender.clone(),
        )])));
        let subtitle_registration = ActiveJobRegistration {
            id: subtitle_active.id.clone(),
            jobs: subtitle_jobs,
            sender: subtitle_sender,
        };
        let subtitle_dependencies = DependencyPaths {
            directory: temp_dir.clone(),
            yt_dlp: fake_yt_dlp,
            deno: temp_dir.join("deno.exe"),
            ffmpeg: temp_dir.join("ffmpeg.exe"),
            ffprobe: temp_dir.join("ffprobe.exe"),
        };
        let subtitle_engine = DownloadEngine::new(subtitle_dependencies, output_directory.clone());
        let (subtitle_events, _subtitle_event_receiver) = mpsc::unbounded_channel();
        run_download_job(
            subtitle_active,
            store.clone(),
            subtitle_engine,
            subtitle_events,
            subtitle_receiver,
            subtitle_registration,
        )
        .await;

        let subtitle_completed = store
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.id == "fixture-subtitle-job")
            .unwrap();
        let expected_subtitle_path = output_directory.join("Fixture media [fixture-123].en.srt");
        assert_eq!(subtitle_completed.status, "Completed");
        assert_eq!(subtitle_completed.download_mode, DownloadMode::SubtitlesOnly);
        assert_eq!(subtitle_completed.subtitle_languages, "en");
        assert_eq!(subtitle_completed.subtitle_format, SubtitleFormat::Srt);
        assert!(subtitle_completed.include_auto_subtitles);
        assert_eq!(
            subtitle_completed.output_path.as_deref(),
            Some(expected_subtitle_path.to_str().unwrap())
        );
        assert_eq!(
            std::fs::read(expected_subtitle_path).unwrap(),
            b"1\n00:00:00,000 --> 00:00:01,000\nfixture subtitle\n"
        );
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    #[ignore = "requires packaged Windows media tools and live YouTube access"]
    async fn packaged_yt_dlp_deno_ffmpeg_complete_a_youtube_queue_job() {
        let package_bin = std::env::var_os("YT_SAVE_DEPENDENCY_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("..")
                    .join("..")
                    .join("target")
                    .join("package")
                    .join("bin")
            })
            .canonicalize()
            .expect("build package dependencies or set YT_SAVE_DEPENDENCY_DIR before running this acceptance test");
        for dependency in ["yt-dlp.exe", "deno.exe", "ffmpeg.exe", "ffprobe.exe"] {
            assert!(
                package_bin.join(dependency).is_file(),
                "packaged dependency is missing: {}",
                package_bin.join(dependency).display()
            );
        }
        let temp_dir = std::env::temp_dir().join(format!("yt-save-live-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let output_directory = temp_dir.join("downloads");
        std::fs::create_dir_all(&output_directory).unwrap();
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut queued = DownloadRecord::queued(
            "youtube-live-job".to_owned(),
            "https://www.youtube.com/watch?v=jNQXAC9IVRw".to_owned(),
            now(),
        );
        queued.download_directory = output_directory.to_string_lossy().into_owned();
        queued.video_quality = crate::model::VideoQuality::Lowest;
        store.save_download(queued).await.unwrap();
        let active = store
            .claim_queued_download_if_next("youtube-live-job", now())
            .await
            .unwrap()
            .expect("YouTube queue job should be claimed");

        let (signal_sender, signal_receiver) = watch::channel(JobSignal::Continue);
        let signal_sender = Arc::new(signal_sender);
        let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::from([(
            active.id.clone(),
            signal_sender.clone(),
        )])));
        let registration = ActiveJobRegistration {
            id: active.id.clone(),
            jobs: active_jobs,
            sender: signal_sender,
        };
        let dependencies = DependencyPaths {
            directory: package_bin.clone(),
            yt_dlp: package_bin.join("yt-dlp.exe"),
            deno: package_bin.join("deno.exe"),
            ffmpeg: package_bin.join("ffmpeg.exe"),
            ffprobe: package_bin.join("ffprobe.exe"),
        };
        let engine = DownloadEngine::new(dependencies, output_directory);
        let (events, _receiver) = mpsc::unbounded_channel();
        run_download_job(active, store.clone(), engine, events, signal_receiver, registration)
            .await;

        let completed = store
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.id == "youtube-live-job")
            .unwrap();
        assert_eq!(completed.status, "Completed", "{:?}", completed.error);
        assert!(completed.title.contains("zoo"), "unexpected title: {}", completed.title);
        let output_path = PathBuf::from(completed.output_path.as_deref().unwrap());
        assert!(output_path.is_file(), "missing downloaded file: {}", output_path.display());
        assert!(std::fs::metadata(&output_path).unwrap().len() > 0);
        std::fs::remove_dir_all(temp_dir).unwrap();
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

        let row = row_view_with_queue_position(&record, "Retry", "", "");
        assert_eq!(row.status.as_str(), "Waiting for retry");
        assert_eq!(row.queue_position.as_str(), "Retry");
        assert!(row.detail.as_str().contains("Retry 2 scheduled for"));
        assert!(queue_summary_text(&[record]).contains("1 waiting for retry"));
    }

    #[test]
    fn local_schedule_requires_a_valid_future_time() {
        let current_timestamp = now();
        let future_local = (chrono::Local::now() + chrono::Duration::hours(4))
            .format("%Y-%m-%d %H:%M")
            .to_string();
        let scheduled_at = parse_local_schedule(&future_local, current_timestamp).unwrap();
        assert!(scheduled_at > current_timestamp);
        assert!(parse_local_schedule("next Tuesday", current_timestamp).is_err());

        let past_local = (chrono::Local::now() - chrono::Duration::hours(4))
            .format("%Y-%m-%d %H:%M")
            .to_string();
        assert!(parse_local_schedule(&past_local, current_timestamp).is_err());
    }

    #[test]
    fn scheduled_queue_rows_show_local_start_time_and_bulk_actions() {
        let mut record = DownloadRecord::queued(
            "scheduled".to_owned(),
            "https://example.test/media".to_owned(),
            now(),
        );
        record.title = "Example media title".to_owned();
        record.status = "Waiting for schedule".to_owned();
        record.scheduled_at = Some(now() + 3600);

        let row = row_view_with_queue_position(&record, "Scheduled", "", "");
        assert_eq!(row.title.as_str(), "Example media title");
        assert_eq!(row.status.as_str(), "Waiting for schedule");
        assert_eq!(row.queue_position.as_str(), "Scheduled");
        assert!(row.detail.as_str().contains("local time"));
        assert!(row.detail.as_str().contains("app must be running"));
        assert!(queue_summary_text(&[record.clone()]).contains("1 scheduled"));
        let actions = queue_action_state(&[record.clone()]);
        assert!(actions.can_pause_all);
        assert!(actions.can_cancel_all);
        record.status = "Paused".to_owned();
        let paused_row = row_view_with_queue_position(&record, "Paused", "", "");
        assert!(paused_row.detail.as_str().contains("Paused · planned for"));
        assert!(paused_row.detail.as_str().contains("resume manually"));
        assert!(!paused_row.detail.as_str().contains("Resumes"));
    }

    #[test]
    fn row_view_labels_audio_output_and_video_only_selection() {
        let mut record = DownloadRecord::queued(
            "audio-job".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        record.download_mode = DownloadMode::AudioOnly;
        record.audio_format = AudioFormat::Mp3;

        let row = row_view_with_queue_position(&record, "", "", "");

        assert!(row.detail.as_str().starts_with("Audio · MP3 · Best · "));

        let mut video = DownloadRecord::queued(
            "video-only-job".to_owned(),
            "https://example.test/video".to_owned(),
            11,
        );
        video.download_mode = DownloadMode::VideoOnly;
        video.video_quality = crate::model::VideoQuality::P720;
        video.selected_format_id = Some("video-137".to_owned());
        let row = row_view_with_queue_position(&video, "", "", "");
        assert!(row.detail.as_str().starts_with("Video only · Format video-137 · "));
    }

    #[test]
    fn format_choices_include_only_formats_compatible_with_the_analyzed_mode() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"id":"sample","formats":[{"format_id":"av","ext":"mp4","vcodec":"avc1","acodec":"none"},{"format_id":"aa","ext":"m4a","vcodec":"none","acodec":"aac"},{"format_id":"muxed","ext":"mp4","vcodec":"avc1","acodec":"aac"}]}"#,
        )
        .unwrap();
        let metadata = MediaMetadata::from_yt_dlp_json_for_playlist_item(
            &value,
            "https://example.test/watch",
            None,
        );
        let video_only = media_format_choice_labels(&metadata.formats, DownloadMode::VideoOnly);
        let video = media_format_choice_labels(&metadata.formats, DownloadMode::Video);
        let audio = media_format_choice_labels(&metadata.formats, DownloadMode::AudioOnly);

        assert_eq!(video_only.len(), 2);
        assert!(video_only[0].as_str().starts_with("Automatic"));
        assert!(video_only[1].as_str().starts_with("av · "));
        assert_eq!(video.len(), 2);
        assert!(video[1].as_str().starts_with("muxed · "));
        assert_eq!(audio.len(), 2);
        assert!(audio[1].as_str().starts_with("aa · "));
    }

    #[test]
    fn exact_format_queue_choice_requires_the_current_url_and_mode() {
        let selection = "137 · mp4 · 1080p · video only · avc1";
        assert_eq!(
            selected_format_id_from_ui(
                selection,
                "https://example.test/watch",
                "video_only",
                "https://example.test/watch",
                DownloadMode::VideoOnly,
            )
            .unwrap()
            .as_deref(),
            Some("137")
        );
        assert!(
            selected_format_id_from_ui(
                selection,
                "https://example.test/old",
                "video_only",
                "https://example.test/watch",
                DownloadMode::VideoOnly,
            )
            .is_err()
        );
        assert!(
            selected_format_id_from_ui(
                selection,
                "https://example.test/watch",
                "video",
                "https://example.test/watch",
                DownloadMode::VideoOnly,
            )
            .is_err()
        );
        assert!(
            selected_format_id_from_ui(
                "137/140 · combined",
                "https://example.test/watch",
                "video",
                "https://example.test/watch",
                DownloadMode::Video,
            )
            .is_err()
        );
        assert_eq!(
            selected_format_id_from_ui(
                AUTOMATIC_FORMAT_CHOICE,
                "https://example.test/old",
                "audio",
                "https://example.test/watch",
                DownloadMode::Video,
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn exact_format_menu_exposes_all_parsed_choices_up_to_the_display_cap() {
        let formats = (0..120)
            .map(|index| MediaFormatOption {
                id: format!("video-{index}"),
                extension: "mp4".to_owned(),
                width: Some(1920),
                height: Some(1080),
                fps: None,
                size_bytes: None,
                size_is_estimate: false,
                video_codec: Some("avc1".to_owned()),
                audio_codec: None,
            })
            .collect::<Vec<_>>();

        let choices = media_format_choice_labels(&formats, DownloadMode::VideoOnly);

        assert_eq!(choices.len(), 101);
        assert!(choices[100].as_str().starts_with("video-99 · "));
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
    fn batch_url_parser_handles_crlf_blanks_duplicates_and_line_errors() {
        let parsed = parse_batch_urls(
            "\r\n https://Media.example/watch/one \r\nhttps://media.example/watch/one\nftp://media.example/audio\nnot a url\nhttps://peer.example/video\n",
        )
        .unwrap();

        assert_eq!(
            parsed.urls,
            [
                BatchUrl { line_number: 2, url: "https://media.example/watch/one".to_owned() },
                BatchUrl { line_number: 6, url: "https://peer.example/video".to_owned() },
            ]
        );
        assert_eq!(
            parsed.issues,
            [
                "line 3: duplicate URL skipped",
                "line 4: Only http and https media URLs are accepted.",
                "line 5: Enter a complete http or https URL.",
            ]
        );
    }

    #[test]
    fn batch_url_parser_rejects_empty_and_oversized_batches() {
        assert!(parse_batch_urls("\n  \r\n").is_err());
        assert!(
            parse_batch_urls("file:///C:/private.mp4\nnot a URL").unwrap_err().contains("line 1")
        );

        let oversized = std::iter::repeat_n("https://media.example/watch", MAX_BATCH_URLS + 1)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(parse_batch_urls(&oversized).unwrap_err().contains("no more than 100"));
    }

    #[test]
    fn queued_batch_records_snapshot_download_settings_and_schedule() {
        let settings = BatchDownloadSettings {
            presets: DownloadPresets::from_ui_values(true, false, "mp3", "192k", "720p")
                .with_download_speed_limit_label("2 MB/s"),
            subtitle_languages: "en,fr".to_owned(),
            subtitle_format: SubtitleFormat::Srt,
            include_auto_subtitles: true,
            scheduled_at: Some(2_000_000_000),
            download_directory: "C:\\Users\\Example\\Downloads".to_owned(),
        };

        let record = queued_batch_record(
            "https://media.example/audio".to_owned(),
            "Audio title".to_owned(),
            "Media example".to_owned(),
            1_900_000_000,
            &settings,
        );

        assert_eq!(record.url, "https://media.example/audio");
        assert_eq!(record.title, "Audio title");
        assert_eq!(record.source, "Media example");
        assert_eq!(record.download_mode, settings.presets.mode);
        assert_eq!(record.audio_format, settings.presets.audio_format);
        assert_eq!(record.audio_quality, settings.presets.audio_quality);
        assert_eq!(record.video_quality, settings.presets.video_quality);
        assert_eq!(record.download_speed_limit_bytes_per_second, 2_000_000);
        assert_eq!(record.subtitle_languages, settings.subtitle_languages);
        assert_eq!(record.subtitle_format, settings.subtitle_format);
        assert!(record.include_auto_subtitles);
        assert_eq!(record.scheduled_at, settings.scheduled_at);
        assert_eq!(record.status, "Waiting for schedule");
        assert_eq!(record.download_directory, settings.download_directory);
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
            mode: DownloadMode::VideoOnly,
            audio_format: AudioFormat::Flac,
            audio_quality: AudioQuality::K320,
            video_quality: VideoQuality::P1440,
            download_speed_limit_bytes_per_second: 2_000_000,
        };
        store
            .set_download_speed_limit(saved_presets.download_speed_limit_bytes_per_second, 1)
            .await
            .unwrap();
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
        active.download_mode = DownloadMode::VideoOnly;
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
        assert_eq!(recovered_active.download_mode, DownloadMode::VideoOnly);
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
