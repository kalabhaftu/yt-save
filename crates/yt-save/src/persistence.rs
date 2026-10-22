use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use anyhow::{Context, Result};
use rusqlite::{
    Connection, OptionalExtension, Row, TransactionBehavior, named_params, types::Type,
};

use crate::{
    app_update::AppUpdateChannel,
    model::{
        AudioFormat, AudioQuality, DownloadMode, DownloadPresets, DownloadRecord,
        NamedDownloadPreset, SubtitleFormat, VideoQuality, MAX_SAVED_DOWNLOAD_PRESETS,
        normalize_download_speed_limit, normalize_saved_download_preset_name, normalized_host_key,
    },
};

mod downloads;
mod settings;
mod subscriptions;

pub const DEFAULT_MAX_CONCURRENT_DOWNLOADS: usize = 1;
pub const MAX_CONCURRENT_DOWNLOADS: usize = 8;
pub const DEFAULT_MAX_CONCURRENT_PER_HOST: usize = 1;
pub const MAX_CONCURRENT_PER_HOST: usize = 8;
pub const DEFAULT_CONCURRENT_FRAGMENTS: usize = 1;
pub const MAX_CONCURRENT_FRAGMENTS: usize = 8;
pub const DEFAULT_AUTO_RETRY_ATTEMPTS: usize = 3;
pub const MAX_AUTO_RETRY_ATTEMPTS: usize = 5;

pub const PLAYLIST_CHECK_INTERVAL_MANUAL: i64 = 0;
pub const PLAYLIST_CHECK_INTERVAL_1_HOUR: i64 = 60 * 60;
pub const PLAYLIST_CHECK_INTERVAL_6_HOURS: i64 = 6 * 60 * 60;
pub const PLAYLIST_CHECK_INTERVAL_12_HOURS: i64 = 12 * 60 * 60;
pub const PLAYLIST_CHECK_INTERVAL_24_HOURS: i64 = 24 * 60 * 60;
pub const PLAYLIST_CHECK_INTERVAL_7_DAYS: i64 = 7 * 24 * 60 * 60;
pub const PLAYLIST_CHECK_INTERVALS_SECONDS: &[i64] = &[
    PLAYLIST_CHECK_INTERVAL_MANUAL,
    PLAYLIST_CHECK_INTERVAL_1_HOUR,
    PLAYLIST_CHECK_INTERVAL_6_HOURS,
    PLAYLIST_CHECK_INTERVAL_12_HOURS,
    PLAYLIST_CHECK_INTERVAL_24_HOURS,
    PLAYLIST_CHECK_INTERVAL_7_DAYS,
];
pub const MAX_DUE_PLAYLIST_SUBSCRIPTIONS_PER_BATCH: usize = 64;

pub fn is_supported_playlist_check_interval_seconds(interval_seconds: i64) -> bool {
    PLAYLIST_CHECK_INTERVALS_SECONDS.contains(&interval_seconds)
}

fn playlist_next_check_at(interval_seconds: i64, checked_at: i64) -> Result<Option<i64>> {
    anyhow::ensure!(
        is_supported_playlist_check_interval_seconds(interval_seconds),
        "unsupported playlist check interval: {interval_seconds} seconds"
    );
    if interval_seconds == PLAYLIST_CHECK_INTERVAL_MANUAL {
        Ok(None)
    } else {
        checked_at
            .checked_add(interval_seconds)
            .map(Some)
            .context("playlist next-check timestamp overflow")
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrayPreferences {
    pub close_to_tray: bool,
    pub minimize_to_tray: bool,
}

#[derive(Clone)]
pub struct Store {
    path: PathBuf,
}

/// Saved playlist and its persisted check schedule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaylistSubscription {
    pub id: String,
    pub url: String,
    pub title: String,
    pub source: String,
    pub created_at: i64,
    /// Seconds between checks, or zero when only manual checks are enabled.
    pub check_interval_seconds: i64,
    /// Next scheduled check, absent when manual checks are enabled.
    pub next_check_at: Option<i64>,
    /// Whether newly discovered available entries are queued without review.
    pub auto_queue_new_entries: bool,
    /// Timestamp of the most recent successful or failed check attempt.
    pub last_checked_at: Option<i64>,
    pub last_check_error: Option<String>,
    /// Number of entries not yet queued or dismissed, including unavailable archive entries.
    pub pending_count: usize,
}

/// Metadata for one item returned by a successful playlist check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaylistSubscriptionEntryInput {
    /// Stable identity within an extractor, for example `youtube:abc123`.
    pub media_key: String,
    pub media_id: Option<String>,
    /// URL that the download queue should use for this individual item.
    pub media_url: String,
    pub title: String,
    pub playlist_index: Option<i64>,
    /// False for private, deleted, or otherwise unavailable playlist items.
    pub available: bool,
}

/// One archived playlist item, including its durable offer and queue state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaylistSubscriptionEntry {
    pub media_key: String,
    pub media_id: Option<String>,
    pub media_url: String,
    pub title: String,
    pub playlist_index: Option<i64>,
    pub available: bool,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
    /// `new`, `queued`, `ignored`, or `downloaded` (derived from a durable completion marker).
    pub state: String,
    pub download_id: Option<String>,
    pub completed_at: Option<i64>,
}

/// Associates an archived media key with the download row to create for it.
#[derive(Clone, Debug)]
pub struct PlaylistSubscriptionQueueItem {
    pub media_key: String,
    pub record: DownloadRecord,
}

struct PlaylistSubscriptionSnapshot {
    id: String,
    title: String,
    source: String,
    checked_at: i64,
    entries: Vec<PlaylistSubscriptionEntryInput>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueMove {
    Up,
    Down,
    Top,
    Bottom,
}

impl Store {
    pub fn open(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("create application data directory {}", parent.display())
            })?;
        }
        let store = Self { path };
        store.with_connection(|connection| {
            connection.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA foreign_keys=ON;
                 CREATE TABLE IF NOT EXISTS downloads (
                    id TEXT PRIMARY KEY NOT NULL,
                    url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    progress REAL NOT NULL DEFAULT 0,
                    downloaded_bytes INTEGER,
                    total_bytes INTEGER,
                    speed_bytes_per_second REAL,
                    eta_seconds INTEGER,
                    output_path TEXT,
                    error TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    playlist_index INTEGER,
                    playlist_group_id TEXT,
                    playlist_group_title TEXT,
                    download_mode TEXT NOT NULL DEFAULT 'video',
                    audio_format TEXT NOT NULL DEFAULT 'original',
                    audio_quality TEXT NOT NULL DEFAULT 'best',
                    video_quality TEXT NOT NULL DEFAULT 'best',
                    download_directory TEXT NOT NULL DEFAULT '',
                    queue_rank INTEGER NOT NULL DEFAULT 0,
                    priority INTEGER NOT NULL DEFAULT 0,
                    retry_attempts INTEGER NOT NULL DEFAULT 0,
                    next_retry_at INTEGER,
                    subtitle_languages TEXT NOT NULL DEFAULT 'en',
                    subtitle_format TEXT NOT NULL DEFAULT 'vtt',
                    include_auto_subtitles INTEGER NOT NULL DEFAULT 0,
                    scheduled_at INTEGER,
                    selected_format_id TEXT,
                    download_speed_limit_bytes_per_second INTEGER NOT NULL DEFAULT 0
                 );
                 CREATE INDEX IF NOT EXISTS idx_downloads_updated_at
                    ON downloads(updated_at DESC);
                 CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY NOT NULL,
                    value TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS playlist_subscriptions (
                    id TEXT PRIMARY KEY NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    check_interval_seconds INTEGER NOT NULL DEFAULT 0,
                    next_check_at INTEGER,
                    last_checked_at INTEGER,
                    last_check_error TEXT,
                    auto_queue_new_entries INTEGER NOT NULL DEFAULT 0
                        CHECK (auto_queue_new_entries IN (0, 1))
                 );
                 CREATE TABLE IF NOT EXISTS playlist_subscription_entries (
                    subscription_id TEXT NOT NULL,
                    media_key TEXT NOT NULL,
                    media_id TEXT,
                    media_url TEXT NOT NULL DEFAULT '',
                    title TEXT NOT NULL DEFAULT '',
                    playlist_index INTEGER,
                    available INTEGER NOT NULL DEFAULT 0 CHECK (available IN (0, 1)),
                    first_seen_at INTEGER NOT NULL,
                    last_seen_at INTEGER NOT NULL,
                    state_updated_at INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'new'
                        CHECK (state IN ('new', 'queued', 'ignored')),
                    download_id TEXT UNIQUE,
                    completed_at INTEGER,
                    PRIMARY KEY (subscription_id, media_key),
                    FOREIGN KEY (subscription_id) REFERENCES playlist_subscriptions(id)
                        ON DELETE CASCADE
                 );
                 CREATE INDEX IF NOT EXISTS idx_playlist_subscription_entries_order
                    ON playlist_subscription_entries(subscription_id, playlist_index, media_key);
                 CREATE INDEX IF NOT EXISTS idx_playlist_subscription_entries_pending
                    ON playlist_subscription_entries(subscription_id, state, available);",
            )?;
            let subscription_columns = {
                let mut statement =
                    connection.prepare("PRAGMA table_info(playlist_subscriptions)")?;
                let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
                columns.collect::<rusqlite::Result<Vec<_>>>()?
            };
            if !subscription_columns.iter().any(|name| name == "check_interval_seconds") {
                connection.execute_batch(
                    "ALTER TABLE playlist_subscriptions
                     ADD COLUMN check_interval_seconds INTEGER NOT NULL DEFAULT 0;",
                )?;
            }
            if !subscription_columns.iter().any(|name| name == "next_check_at") {
                connection.execute_batch(
                    "ALTER TABLE playlist_subscriptions ADD COLUMN next_check_at INTEGER;",
                )?;
            }
            if !subscription_columns.iter().any(|name| name == "auto_queue_new_entries") {
                connection.execute_batch(
                    "ALTER TABLE playlist_subscriptions
                     ADD COLUMN auto_queue_new_entries INTEGER NOT NULL DEFAULT 0
                         CHECK (auto_queue_new_entries IN (0, 1));",
                )?;
            }
            connection.execute_batch(
                "CREATE INDEX IF NOT EXISTS idx_playlist_subscriptions_due
                    ON playlist_subscriptions(next_check_at, id)
                    WHERE next_check_at IS NOT NULL;",
            )?;
            let has_subscription_completed_at = {
                let mut statement =
                    connection.prepare("PRAGMA table_info(playlist_subscription_entries)")?;
                let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
                columns
                    .collect::<rusqlite::Result<Vec<_>>>()?
                    .iter()
                    .any(|name| name == "completed_at")
            };
            if !has_subscription_completed_at {
                connection.execute_batch(
                    "ALTER TABLE playlist_subscription_entries ADD COLUMN completed_at INTEGER;",
                )?;
            }
            let has_playlist_index = {
                let mut statement = connection.prepare("PRAGMA table_info(downloads)")?;
                let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
                columns
                    .collect::<rusqlite::Result<Vec<_>>>()?
                    .iter()
                    .any(|name| name == "playlist_index")
            };
            if !has_playlist_index {
                connection
                    .execute_batch("ALTER TABLE downloads ADD COLUMN playlist_index INTEGER;")?;
            }
            for (column, definition) in [
                ("download_mode", "TEXT NOT NULL DEFAULT 'video'"),
                ("audio_format", "TEXT NOT NULL DEFAULT 'original'"),
                ("audio_quality", "TEXT NOT NULL DEFAULT 'best'"),
                ("video_quality", "TEXT NOT NULL DEFAULT 'best'"),
                ("download_directory", "TEXT NOT NULL DEFAULT ''"),
                ("retry_attempts", "INTEGER NOT NULL DEFAULT 0"),
                ("next_retry_at", "INTEGER"),
                ("playlist_group_id", "TEXT"),
                ("playlist_group_title", "TEXT"),
                ("subtitle_languages", "TEXT NOT NULL DEFAULT 'en'"),
                ("subtitle_format", "TEXT NOT NULL DEFAULT 'vtt'"),
                ("include_auto_subtitles", "INTEGER NOT NULL DEFAULT 0"),
                ("scheduled_at", "INTEGER"),
                ("selected_format_id", "TEXT"),
                ("download_speed_limit_bytes_per_second", "INTEGER NOT NULL DEFAULT 0"),
            ] {
                let exists = {
                    let mut statement = connection.prepare("PRAGMA table_info(downloads)")?;
                    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
                    columns.collect::<rusqlite::Result<Vec<_>>>()?.iter().any(|name| name == column)
                };
                if !exists {
                    connection.execute_batch(&format!(
                        "ALTER TABLE downloads ADD COLUMN {column} {definition};"
                    ))?;
                }
            }
            let has_queue_rank = {
                let mut statement = connection.prepare("PRAGMA table_info(downloads)")?;
                let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
                columns
                    .collect::<rusqlite::Result<Vec<_>>>()?
                    .iter()
                    .any(|name| name == "queue_rank")
            };
            if !has_queue_rank {
                connection.execute_batch(
                    "ALTER TABLE downloads ADD COLUMN queue_rank INTEGER NOT NULL DEFAULT 0;
                     UPDATE downloads SET queue_rank = rowid * 1024;",
                )?;
            }
            let has_priority = {
                let mut statement = connection.prepare("PRAGMA table_info(downloads)")?;
                let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
                columns.collect::<rusqlite::Result<Vec<_>>>()?.iter().any(|name| name == "priority")
            };
            if !has_priority {
                connection.execute_batch(
                    "ALTER TABLE downloads ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;",
                )?;
            }
            connection.execute_batch(
                "CREATE INDEX IF NOT EXISTS idx_downloads_queue
                    ON downloads(status, priority DESC, queue_rank ASC);",
            )?;
            Ok(())
        })?;
        Ok(store)
    }

    fn with_connection<T>(&self, operation: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let connection = Connection::open(&self.path)
            .with_context(|| format!("open application database {}", self.path.display()))?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch("PRAGMA foreign_keys=ON;")?;
        operation(&connection)
    }

    fn with_connection_mut<T>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T>,
    ) -> Result<T> {
        let mut connection = Connection::open(&self.path)
            .with_context(|| format!("open application database {}", self.path.display()))?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch("PRAGMA foreign_keys=ON;")?;
        operation(&mut connection)
    }
}

fn save_download_record(connection: &Connection, record: &DownloadRecord) -> Result<()> {
    let downloaded_bytes = sqlite_integer(record.downloaded_bytes, "downloaded_bytes")?;
    let total_bytes = sqlite_integer(record.total_bytes, "total_bytes")?;
    let eta_seconds = sqlite_integer(record.eta_seconds, "eta_seconds")?;
    connection.execute(
        "INSERT INTO downloads (
            id, url, title, source, status, progress, downloaded_bytes,
            total_bytes, speed_bytes_per_second, eta_seconds, output_path,
            error, created_at, updated_at, playlist_index,
            playlist_group_id, playlist_group_title,
            download_mode, audio_format, audio_quality, video_quality,
            download_directory, queue_rank, priority, retry_attempts, next_retry_at,
            subtitle_languages, subtitle_format, include_auto_subtitles, scheduled_at,
            selected_format_id, download_speed_limit_bytes_per_second
        ) VALUES (
            :id, :url, :title, :source, :status, :progress, :downloaded,
            :total, :speed, :eta, :output, :error, :created, :updated,
            :playlist_index, :playlist_group_id, :playlist_group_title,
            :download_mode, :audio_format, :audio_quality, :video_quality,
            :download_directory,
            coalesce((SELECT max(queue_rank) FROM downloads
                      WHERE priority = :priority), 0) + 1024,
            :priority, :retry_attempts, :next_retry_at,
            :subtitle_languages, :subtitle_format, :include_auto_subtitles, :scheduled_at,
            :selected_format_id, :speed_limit
        ) ON CONFLICT(id) DO UPDATE SET
            url=excluded.url, title=excluded.title, source=excluded.source,
            status=excluded.status, progress=excluded.progress,
            downloaded_bytes=excluded.downloaded_bytes,
            total_bytes=excluded.total_bytes,
            speed_bytes_per_second=excluded.speed_bytes_per_second,
            eta_seconds=excluded.eta_seconds, output_path=excluded.output_path,
            error=excluded.error, updated_at=excluded.updated_at,
            playlist_index=excluded.playlist_index,
            playlist_group_id=excluded.playlist_group_id,
            playlist_group_title=excluded.playlist_group_title,
            download_mode=excluded.download_mode,
            audio_format=excluded.audio_format,
            audio_quality=excluded.audio_quality,
            video_quality=excluded.video_quality,
            download_directory=excluded.download_directory,
            retry_attempts=excluded.retry_attempts,
            next_retry_at=excluded.next_retry_at,
            subtitle_languages=excluded.subtitle_languages,
            subtitle_format=excluded.subtitle_format,
            include_auto_subtitles=excluded.include_auto_subtitles,
            scheduled_at=excluded.scheduled_at,
            selected_format_id=excluded.selected_format_id,
            download_speed_limit_bytes_per_second=excluded.download_speed_limit_bytes_per_second",
        named_params! {
            ":id": record.id,
            ":url": record.url,
            ":title": record.title,
            ":source": record.source,
            ":status": record.status,
            ":progress": record.progress,
            ":downloaded": downloaded_bytes,
            ":total": total_bytes,
            ":speed": record.speed_bytes_per_second,
            ":eta": eta_seconds,
            ":output": record.output_path,
            ":error": record.error,
            ":created": record.created_at,
            ":updated": record.updated_at,
            ":playlist_index": record.playlist_index,
            ":playlist_group_id": record.playlist_group_id,
            ":playlist_group_title": record.playlist_group_title,
            ":download_mode": record.download_mode.as_database_value(),
            ":audio_format": record.audio_format.as_database_value(),
            ":audio_quality": record.audio_quality.as_database_value(),
            ":video_quality": record.video_quality.as_database_value(),
            ":download_directory": record.download_directory,
            ":priority": record.priority,
            ":retry_attempts": i64::from(record.retry_attempts),
            ":next_retry_at": record.next_retry_at,
            ":subtitle_languages": record.subtitle_languages,
            ":subtitle_format": record.subtitle_format.as_database_value(),
            ":include_auto_subtitles": record.include_auto_subtitles,
            ":scheduled_at": record.scheduled_at,
            ":selected_format_id": record.selected_format_id,
            ":speed_limit": i64::try_from(normalize_download_speed_limit(
                record.download_speed_limit_bytes_per_second,
            ))?,
        },
    )?;
    Ok(())
}

fn validate_media_key(media_key: &str) -> Result<()> {
    anyhow::ensure!(!media_key.trim().is_empty(), "playlist media key is empty");
    anyhow::ensure!(media_key == media_key.trim(), "playlist media key has surrounding whitespace");
    anyhow::ensure!(media_key.len() <= 1024, "playlist media key exceeds 1024 bytes");
    anyhow::ensure!(
        !media_key.chars().any(char::is_control),
        "playlist media key contains a control character"
    );
    Ok(())
}

fn validate_media_keys(media_keys: &[String]) -> Result<()> {
    let mut seen = HashSet::with_capacity(media_keys.len());
    for media_key in media_keys {
        validate_media_key(media_key)?;
        anyhow::ensure!(seen.insert(media_key), "duplicate playlist media key {media_key}");
    }
    Ok(())
}

fn validate_subscription_snapshot(entries: &[PlaylistSubscriptionEntryInput]) -> Result<()> {
    let keys = entries.iter().map(|entry| entry.media_key.clone()).collect::<Vec<_>>();
    validate_media_keys(&keys)
}

fn validate_queue_items(items: &[PlaylistSubscriptionQueueItem]) -> Result<()> {
    let keys = items.iter().map(|item| item.media_key.clone()).collect::<Vec<_>>();
    validate_media_keys(&keys)?;
    let mut download_ids = HashSet::with_capacity(items.len());
    for item in items {
        anyhow::ensure!(
            download_ids.insert(item.record.id.as_str()),
            "duplicate download id {} in subscription queue batch",
            item.record.id
        );
        anyhow::ensure!(
            item.record.status == "Queued",
            "subscription download {} must start in Queued state",
            item.record.id
        );
        anyhow::ensure!(
            !item.record.url.trim().is_empty(),
            "subscription download {} has an empty URL",
            item.record.id
        );
    }
    Ok(())
}

fn first_eligible_queued_download_id(
    connection: &Connection,
    active_host_counts: &HashMap<String, usize>,
    per_host_limit: usize,
) -> Result<Option<String>> {
    let mut statement = connection.prepare(
        "SELECT id, url FROM downloads WHERE status = 'Queued'
         ORDER BY priority DESC, queue_rank ASC, rowid ASC",
    )?;
    let rows =
        statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
    for row in rows {
        let (id, url) = row?;
        let host_key = normalized_host_key(&url).unwrap_or_default();
        if active_host_counts.get(&host_key).copied().unwrap_or_default() < per_host_limit {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

fn setting_value(connection: &Connection, key: &str) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT value FROM app_settings WHERE key = :key",
            named_params! { ":key": key },
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
}

fn load_download_by_id(connection: &Connection, id: &str) -> Result<Option<DownloadRecord>> {
    let mut statement = connection.prepare(
        "SELECT id, url, title, source, status, progress, downloaded_bytes,
                total_bytes, speed_bytes_per_second, eta_seconds, output_path,
                error, created_at, updated_at, playlist_index,
                playlist_group_id, playlist_group_title,
                download_mode, audio_format, audio_quality, video_quality, queue_rank,
                priority, download_directory, retry_attempts, next_retry_at,
                subtitle_languages, subtitle_format, include_auto_subtitles, scheduled_at,
                selected_format_id, download_speed_limit_bytes_per_second
         FROM downloads WHERE id = :id",
    )?;
    let record = statement
        .query_row(named_params! { ":id": id }, |row| {
            Ok(DownloadRecord {
                id: row.get(0)?,
                url: row.get(1)?,
                title: row.get(2)?,
                source: row.get(3)?,
                status: row.get(4)?,
                progress: row.get(5)?,
                downloaded_bytes: optional_u64(row, 6)?,
                total_bytes: optional_u64(row, 7)?,
                speed_bytes_per_second: row.get(8)?,
                eta_seconds: optional_u64(row, 9)?,
                output_path: row.get(10)?,
                error: row.get(11)?,
                created_at: row.get(12)?,
                updated_at: row.get(13)?,
                playlist_index: row.get(14)?,
                playlist_group_id: row.get(15)?,
                playlist_group_title: row.get(16)?,
                download_mode: download_mode_from_row(row, 17)?,
                audio_format: audio_format_from_row(row, 18)?,
                audio_quality: audio_quality_from_row(row, 19)?,
                video_quality: video_quality_from_row(row, 20)?,
                download_directory: row.get(23)?,
                queue_order: row.get(21)?,
                priority: row.get(22)?,
                retry_attempts: retry_attempts_from_row(row, 24)?,
                next_retry_at: row.get(25)?,
                subtitle_languages: row.get(26)?,
                subtitle_format: subtitle_format_from_row(row, 27)?,
                include_auto_subtitles: row.get(28)?,
                scheduled_at: row.get(29)?,
                selected_format_id: row.get(30)?,
                download_speed_limit_bytes_per_second: download_speed_limit_from_row(row, 31)?,
            })
        })
        .optional()?;
    Ok(record)
}

fn sqlite_integer(value: Option<u64>, field: &str) -> Result<Option<i64>> {
    value
        .map(|value| {
            i64::try_from(value)
                .with_context(|| format!("{field} exceeds SQLite's signed integer range"))
        })
        .transpose()
}

fn load_downloads(connection: &Connection) -> Result<Vec<DownloadRecord>> {
    let mut statement = connection.prepare(
        "SELECT id, url, title, source, status, progress, downloaded_bytes,
                total_bytes, speed_bytes_per_second, eta_seconds, output_path,
                error, created_at, updated_at, playlist_index,
                playlist_group_id, playlist_group_title,
                download_mode, audio_format, audio_quality, video_quality, queue_rank,
                priority, download_directory, retry_attempts, next_retry_at,
                subtitle_languages, subtitle_format, include_auto_subtitles, scheduled_at,
                selected_format_id, download_speed_limit_bytes_per_second
         FROM downloads
        ORDER BY CASE WHEN status IN ('Queued', 'Waiting for retry', 'Paused', 'Waiting for schedule') THEN 0 ELSE 1 END,
                  CASE WHEN status IN ('Queued', 'Waiting for retry', 'Paused', 'Waiting for schedule') THEN priority ELSE 0 END DESC,
                  CASE WHEN status IN ('Queued', 'Waiting for retry', 'Paused', 'Waiting for schedule') THEN queue_rank ELSE 0 END ASC,
                  CASE WHEN status IN ('Queued', 'Waiting for retry', 'Paused', 'Waiting for schedule') THEN rowid ELSE 0 END ASC,
                  created_at DESC, rowid DESC",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(DownloadRecord {
            id: row.get(0)?,
            url: row.get(1)?,
            title: row.get(2)?,
            source: row.get(3)?,
            status: row.get(4)?,
            progress: row.get(5)?,
            downloaded_bytes: optional_u64(row, 6)?,
            total_bytes: optional_u64(row, 7)?,
            speed_bytes_per_second: row.get(8)?,
            eta_seconds: optional_u64(row, 9)?,
            output_path: row.get(10)?,
            error: row.get(11)?,
            created_at: row.get(12)?,
            updated_at: row.get(13)?,
            playlist_index: row.get(14)?,
            playlist_group_id: row.get(15)?,
            playlist_group_title: row.get(16)?,
            download_mode: download_mode_from_row(row, 17)?,
            audio_format: audio_format_from_row(row, 18)?,
            audio_quality: audio_quality_from_row(row, 19)?,
            video_quality: video_quality_from_row(row, 20)?,
            download_directory: row.get(23)?,
            queue_order: row.get(21)?,
            priority: row.get(22)?,
            retry_attempts: retry_attempts_from_row(row, 24)?,
            next_retry_at: row.get(25)?,
            subtitle_languages: row.get(26)?,
            subtitle_format: subtitle_format_from_row(row, 27)?,
            include_auto_subtitles: row.get(28)?,
            scheduled_at: row.get(29)?,
            selected_format_id: row.get(30)?,
            download_speed_limit_bytes_per_second: download_speed_limit_from_row(row, 31)?,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

fn download_mode_from_row(row: &Row<'_>, index: usize) -> rusqlite::Result<DownloadMode> {
    let value: String = row.get(index)?;
    DownloadMode::from_database_value(&value).ok_or_else(|| invalid_enum_value(index, &value))
}

fn audio_format_from_row(row: &Row<'_>, index: usize) -> rusqlite::Result<AudioFormat> {
    let value: String = row.get(index)?;
    AudioFormat::from_database_value(&value).ok_or_else(|| invalid_enum_value(index, &value))
}

fn audio_quality_from_row(row: &Row<'_>, index: usize) -> rusqlite::Result<AudioQuality> {
    let value: String = row.get(index)?;
    AudioQuality::from_database_value(&value).ok_or_else(|| invalid_enum_value(index, &value))
}

fn video_quality_from_row(row: &Row<'_>, index: usize) -> rusqlite::Result<VideoQuality> {
    let value: String = row.get(index)?;
    VideoQuality::from_database_value(&value).ok_or_else(|| invalid_enum_value(index, &value))
}

fn download_speed_limit_from_row(row: &Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    Ok(u64::try_from(value).map(normalize_download_speed_limit).unwrap_or_default())
}

fn subtitle_format_from_row(row: &Row<'_>, index: usize) -> rusqlite::Result<SubtitleFormat> {
    let value: String = row.get(index)?;
    SubtitleFormat::from_database_value(&value).ok_or_else(|| invalid_enum_value(index, &value))
}

fn invalid_enum_value(index: usize, value: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unrecognized persisted download choice: {value}"),
        )),
    )
}

fn retry_attempts_from_row(row: &Row<'_>, index: usize) -> rusqlite::Result<u32> {
    let value = row.get::<_, i64>(index)?;
    u32::try_from(value).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "automatic retry count must be a nonnegative 32-bit integer",
            )),
        )
    })
}

fn optional_u64(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<u64>> {
    row.get::<_, Option<i64>>(index)?
        .map(|value| {
            u64::try_from(value).map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    Type::Integer,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "expected a nonnegative SQLite integer",
                    )),
                )
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests;
