use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::{
    Connection, OptionalExtension, Row, TransactionBehavior, named_params, types::Type,
};

use crate::model::{
    AudioFormat, AudioQuality, DownloadMode, DownloadPresets, DownloadRecord, VideoQuality,
};

pub const DEFAULT_MAX_CONCURRENT_DOWNLOADS: usize = 1;
pub const MAX_CONCURRENT_DOWNLOADS: usize = 8;
pub const DEFAULT_CONCURRENT_FRAGMENTS: usize = 1;
pub const MAX_CONCURRENT_FRAGMENTS: usize = 8;
pub const DEFAULT_AUTO_RETRY_ATTEMPTS: usize = 3;
pub const MAX_AUTO_RETRY_ATTEMPTS: usize = 5;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrayPreferences {
    pub close_to_tray: bool,
    pub minimize_to_tray: bool,
}

#[derive(Clone)]
pub struct Store {
    path: PathBuf,
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
                    next_retry_at INTEGER
                 );
                 CREATE INDEX IF NOT EXISTS idx_downloads_updated_at
                    ON downloads(updated_at DESC);
                 CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY NOT NULL,
                    value TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                 );",
            )?;
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

    pub async fn list_downloads(&self) -> Result<Vec<DownloadRecord>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.with_connection(load_downloads))
            .await
            .context("join database read")?
    }

    /// Loads the saved destination, creating it from `fallback` for older installations.
    /// Queue rows created before destination snapshots existed are assigned this initial path.
    pub async fn get_or_initialize_download_directory(&self, fallback: String) -> Result<String> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "INSERT OR IGNORE INTO app_settings (key, value, updated_at)
                     VALUES ('download_directory', :fallback, :updated)",
                    named_params! { ":fallback": fallback, ":updated": chrono::Utc::now().timestamp() },
                )?;
                let configured: String = connection.query_row(
                    "SELECT value FROM app_settings WHERE key = 'download_directory'",
                    [],
                    |row| row.get(0),
                )?;
                Ok(configured)
            })
        })
        .await
        .context("join settings read")?
    }

    /// Assigns a stable destination to queue rows created before destination snapshots existed.
    pub async fn backfill_legacy_download_directories(&self, directory: String) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "UPDATE downloads SET download_directory = :directory
                     WHERE download_directory = ''",
                    named_params! { ":directory": directory },
                )?;
                Ok(())
            })
        })
        .await
        .context("join legacy queue migration")??;
        Ok(())
    }

    /// Changes only the default for future queue items; saved jobs keep their destination.
    pub async fn set_default_download_directory(
        &self,
        directory: String,
        updated_at: i64,
    ) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "INSERT INTO app_settings (key, value, updated_at)
                     VALUES ('download_directory', :directory, :updated)
                     ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value, updated_at = excluded.updated_at",
                    named_params! { ":directory": directory, ":updated": updated_at },
                )?;
                Ok(())
            })
        })
        .await
        .context("join settings write")??;
        Ok(())
    }

    /// Loads the New download form defaults, ignoring unknown persisted values independently.
    pub async fn load_download_presets(&self) -> Result<DownloadPresets> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let mode = setting_value(connection, "new_download_mode")?;
                let audio_format = setting_value(connection, "new_download_audio_format")?;
                let audio_quality = setting_value(connection, "new_download_audio_quality")?;
                let video_quality = setting_value(connection, "new_download_video_quality")?;
                Ok(DownloadPresets::from_database_values(
                    mode.as_deref(),
                    audio_format.as_deref(),
                    audio_quality.as_deref(),
                    video_quality.as_deref(),
                ))
            })
        })
        .await
        .context("join preset read")?
    }

    /// Loads the persisted queue concurrency limit; invalid or missing values use one worker.
    pub async fn load_max_concurrent_downloads(&self) -> Result<usize> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let parsed = setting_value(connection, "max_concurrent_downloads")?
                    .and_then(|value| value.parse::<usize>().ok())
                    .filter(|value| (1..=MAX_CONCURRENT_DOWNLOADS).contains(value));
                Ok(parsed.unwrap_or(DEFAULT_MAX_CONCURRENT_DOWNLOADS))
            })
        })
        .await
        .context("join concurrency setting read")?
    }

    /// Saves the bounded queue concurrency limit for subsequent dispatches.
    pub async fn set_max_concurrent_downloads(&self, limit: usize, updated_at: i64) -> Result<()> {
        let limit = limit.clamp(1, MAX_CONCURRENT_DOWNLOADS).to_string();
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "INSERT INTO app_settings (key, value, updated_at)
                     VALUES ('max_concurrent_downloads', :value, :updated)
                     ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value, updated_at = excluded.updated_at",
                    named_params! { ":value": limit, ":updated": updated_at },
                )?;
                Ok(())
            })
        })
        .await
        .context("join concurrency setting write")??;
        Ok(())
    }

    /// Loads yt-dlp's per-download fragment worker count; invalid or missing values use one.
    pub async fn load_concurrent_fragments(&self) -> Result<usize> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let parsed = setting_value(connection, "concurrent_fragments")?
                    .and_then(|value| value.parse::<usize>().ok())
                    .filter(|value| (1..=MAX_CONCURRENT_FRAGMENTS).contains(value));
                Ok(parsed.unwrap_or(DEFAULT_CONCURRENT_FRAGMENTS))
            })
        })
        .await
        .context("join fragment concurrency setting read")?
    }

    /// Saves yt-dlp's bounded per-download fragment worker count.
    pub async fn set_concurrent_fragments(&self, count: usize, updated_at: i64) -> Result<()> {
        let count = count.clamp(1, MAX_CONCURRENT_FRAGMENTS).to_string();
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "INSERT INTO app_settings (key, value, updated_at)
                     VALUES ('concurrent_fragments', :value, :updated)
                     ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value, updated_at = excluded.updated_at",
                    named_params! { ":value": count, ":updated": updated_at },
                )?;
                Ok(())
            })
        })
        .await
        .context("join fragment concurrency setting write")??;
        Ok(())
    }

    /// Loads the maximum number of automatic retries after the initial attempt.
    pub async fn load_auto_retry_attempts(&self) -> Result<usize> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let parsed = setting_value(connection, "auto_retry_attempts")?
                    .and_then(|value| value.parse::<usize>().ok())
                    .filter(|value| *value <= MAX_AUTO_RETRY_ATTEMPTS);
                Ok(parsed.unwrap_or(DEFAULT_AUTO_RETRY_ATTEMPTS))
            })
        })
        .await
        .context("join automatic retry setting read")?
    }

    /// Saves a bounded automatic retry count, where zero disables automatic retries.
    pub async fn set_auto_retry_attempts(&self, count: usize, updated_at: i64) -> Result<usize> {
        let count = count.min(MAX_AUTO_RETRY_ATTEMPTS);
        let count_text = count.to_string();
        let store = self.clone();
        let canceled_waits = tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute(
                    "INSERT INTO app_settings (key, value, updated_at)
                     VALUES ('auto_retry_attempts', :value, :updated)
                     ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value, updated_at = excluded.updated_at",
                    named_params! { ":value": count_text, ":updated": updated_at },
                )?;
                let canceled_waits = transaction.execute(
                    "UPDATE downloads
                     SET status = 'Failed', next_retry_at = NULL,
                         error = CASE
                             WHEN error IS NULL OR trim(error) = '' THEN
                                 'Automatic retry canceled because the retry limit was lowered.'
                             ELSE error || ' Automatic retry canceled because the retry limit was lowered.'
                         END,
                         updated_at = :updated
                     WHERE status = 'Waiting for retry'
                       AND retry_attempts > :max_attempts",
                    named_params! {
                        ":updated": updated_at,
                        ":max_attempts": i64::try_from(count).unwrap_or(i64::MAX),
                    },
                )?;
                transaction.commit()?;
                Ok(canceled_waits)
            })
        })
        .await
        .context("join automatic retry setting write")??;
        Ok(canceled_waits)
    }

    /// Promotes retry waits whose persisted deadlines have elapsed.
    pub async fn promote_due_retries(&self, timestamp: i64) -> Result<usize> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let changed = connection.execute(
                    "UPDATE downloads
                     SET status = 'Queued', next_retry_at = NULL, updated_at = :now
                     WHERE status = 'Waiting for retry'
                       AND (next_retry_at IS NULL OR next_retry_at <= :now)",
                    named_params! { ":now": timestamp },
                )?;
                Ok(changed)
            })
        })
        .await
        .context("join due retry update")?
    }

    /// Returns the earliest retry deadline for dispatcher timer scheduling.
    pub async fn next_retry_at(&self) -> Result<Option<i64>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT min(next_retry_at) FROM downloads
                         WHERE status = 'Waiting for retry' AND next_retry_at IS NOT NULL",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(Into::into)
            })
        })
        .await
        .context("join next retry deadline read")?
    }

    /// Saves a retryable failure only while its worker still owns an active queue item.
    pub async fn schedule_automatic_retry(
        &self,
        record: DownloadRecord,
        retry_at: i64,
    ) -> Result<Option<usize>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let configured_attempts = setting_value(&transaction, "auto_retry_attempts")?
                    .and_then(|value| value.parse::<usize>().ok())
                    .filter(|value| *value <= MAX_AUTO_RETRY_ATTEMPTS)
                    .unwrap_or(DEFAULT_AUTO_RETRY_ATTEMPTS);
                let affected = transaction.execute(
                    "UPDATE downloads
                     SET title = :title, source = :source, status = 'Waiting for retry',
                         progress = :progress, downloaded_bytes = :downloaded,
                         total_bytes = :total, speed_bytes_per_second = NULL,
                         eta_seconds = NULL, output_path = :output, error = :error,
                         retry_attempts = retry_attempts + 1,
                         next_retry_at = :retry_at,
                         queue_rank = coalesce((
                             SELECT max(queue_rank) FROM downloads AS existing
                             WHERE existing.priority = downloads.priority
                               AND existing.id <> downloads.id
                         ), 0) + 1024,
                         updated_at = :updated
                     WHERE id = :id
                       AND status IN ('Analyzing', 'Downloading', 'Post-processing')
                       AND retry_attempts < :max_attempts",
                    named_params! {
                        ":id": record.id,
                        ":title": record.title,
                        ":source": record.source,
                        ":progress": record.progress,
                        ":downloaded": sqlite_integer(record.downloaded_bytes, "downloaded_bytes")?,
                        ":total": sqlite_integer(record.total_bytes, "total_bytes")?,
                        ":output": record.output_path,
                        ":error": record.error,
                        ":retry_at": retry_at,
                        ":updated": record.updated_at,
                        ":max_attempts": i64::try_from(configured_attempts).unwrap_or(i64::MAX),
                    },
                )?;
                transaction.commit()?;
                Ok((affected == 1).then_some(configured_attempts))
            })
        })
        .await
        .context("join automatic retry write")?
    }

    /// Clipboard monitoring is off unless the user previously enabled it explicitly.
    pub async fn load_clipboard_monitoring_enabled(&self) -> Result<bool> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                Ok(setting_value(connection, "clipboard_monitoring_enabled")?.as_deref()
                    == Some("true"))
            })
        })
        .await
        .context("join clipboard preference read")?
    }

    pub async fn set_clipboard_monitoring_enabled(
        &self,
        enabled: bool,
        updated_at: i64,
    ) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "INSERT INTO app_settings (key, value, updated_at)
                     VALUES ('clipboard_monitoring_enabled', :value, :updated)
                     ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value, updated_at = excluded.updated_at",
                    named_params! {
                        ":value": if enabled { "true" } else { "false" },
                        ":updated": updated_at,
                    },
                )?;
                Ok(())
            })
        })
        .await
        .context("join clipboard preference write")??;
        Ok(())
    }

    /// Tray behavior is opt-in; an unset or invalid preference keeps normal window behavior.
    pub async fn load_tray_preferences(&self) -> Result<TrayPreferences> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                Ok(TrayPreferences {
                    close_to_tray: setting_value(connection, "close_to_tray_enabled")?.as_deref()
                        == Some("true"),
                    minimize_to_tray: setting_value(connection, "minimize_to_tray_enabled")?
                        .as_deref()
                        == Some("true"),
                })
            })
        })
        .await
        .context("join tray preference read")?
    }

    pub async fn set_tray_preferences(
        &self,
        preferences: TrayPreferences,
        updated_at: i64,
    ) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "INSERT INTO app_settings (key, value, updated_at)
                     VALUES
                        ('close_to_tray_enabled', :close, :updated),
                        ('minimize_to_tray_enabled', :minimize, :updated)
                     ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value, updated_at = excluded.updated_at",
                    named_params! {
                        ":close": if preferences.close_to_tray { "true" } else { "false" },
                        ":minimize": if preferences.minimize_to_tray { "true" } else { "false" },
                        ":updated": updated_at,
                    },
                )?;
                Ok(())
            })
        })
        .await
        .context("join tray preference write")??;
        Ok(())
    }

    /// Updates New download defaults atomically; queue rows and retries are untouched.
    pub async fn set_download_presets(
        &self,
        presets: DownloadPresets,
        updated_at: i64,
    ) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let [(_, mode), (_, audio_format), (_, audio_quality), (_, video_quality)] =
                    presets.database_values();
                connection.execute(
                    "INSERT INTO app_settings (key, value, updated_at)
                     VALUES
                        ('new_download_mode', :mode, :updated),
                        ('new_download_audio_format', :audio_format, :updated),
                        ('new_download_audio_quality', :audio_quality, :updated),
                        ('new_download_video_quality', :video_quality, :updated)
                     ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value, updated_at = excluded.updated_at",
                    named_params! {
                        ":mode": mode,
                        ":audio_format": audio_format,
                        ":audio_quality": audio_quality,
                        ":video_quality": video_quality,
                        ":updated": updated_at,
                    },
                )?;
                Ok(())
            })
        })
        .await
        .context("join preset write")??;
        Ok(())
    }

    pub async fn save_download(&self, record: DownloadRecord) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| save_download_record(connection, &record))
        })
        .await
        .context("join database write")??;
        Ok(())
    }

    /// Saves related queue rows in one write transaction so playlist batches stay contiguous.
    pub async fn save_downloads(&self, records: Vec<DownloadRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                for record in &records {
                    save_download_record(&transaction, record)?;
                }
                transaction.commit()?;
                Ok(())
            })
        })
        .await
        .context("join queue batch write")??;
        Ok(())
    }

    pub async fn next_queued_download(&self) -> Result<Option<DownloadRecord>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let id = connection
                    .query_row(
                        "SELECT id FROM downloads WHERE status = 'Queued'
                         ORDER BY priority DESC, queue_rank ASC, rowid ASC LIMIT 1",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                match id {
                    Some(id) => load_download_by_id(connection, &id),
                    None => Ok(None),
                }
            })
        })
        .await
        .context("join queue read")?
    }

    /// Claims only if `id` is still first, so a concurrent reorder is honored before launch.
    pub async fn claim_queued_download_if_next(
        &self,
        id: &str,
        updated_at: i64,
    ) -> Result<Option<DownloadRecord>> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let next_id = transaction
                    .query_row(
                        "SELECT id FROM downloads WHERE status = 'Queued'
                         ORDER BY priority DESC, queue_rank ASC, rowid ASC LIMIT 1",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                if next_id.as_deref() != Some(id.as_str()) {
                    transaction.commit()?;
                    return Ok(None);
                }
                let affected = transaction.execute(
                    "UPDATE downloads SET status = 'Analyzing', error = NULL, updated_at = :updated
                     WHERE id = :id AND status = 'Queued'",
                    named_params! { ":id": id, ":updated": updated_at },
                )?;
                let record =
                    if affected == 1 { load_download_by_id(&transaction, &id)? } else { None };
                transaction.commit()?;
                Ok(record)
            })
        })
        .await
        .context("join queue claim")?
    }

    /// Moves a queued or paused item within its priority group.
    pub async fn move_download(&self, id: &str, movement: QueueMove) -> Result<bool> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let selected = transaction
                    .query_row(
                    "SELECT priority FROM downloads WHERE id = :id AND status IN ('Queued', 'Waiting for retry', 'Paused')",
                        named_params! { ":id": id },
                        |row| row.get::<_, i32>(0),
                    )
                    .optional()?;
                let Some(priority) = selected else {
                    transaction.commit()?;
                    return Ok(false);
                };
                let mut statement = transaction.prepare(
                    "SELECT id FROM downloads WHERE status IN ('Queued', 'Waiting for retry', 'Paused') AND priority = :priority
                     ORDER BY queue_rank ASC, rowid ASC",
                )?;
                let rows = statement.query_map(named_params! { ":priority": priority }, |row| row.get::<_, String>(0))?;
                let mut ids = rows.collect::<rusqlite::Result<Vec<_>>>()?;
                drop(statement);
                let Some(index) = ids.iter().position(|item_id| item_id == &id) else {
                    transaction.commit()?;
                    return Ok(false);
                };
                let destination = match movement {
                    QueueMove::Up => index.checked_sub(1),
                    QueueMove::Down => (index + 1 < ids.len()).then_some(index + 1),
                    QueueMove::Top => Some(0),
                    QueueMove::Bottom => Some(ids.len().saturating_sub(1)),
                };
                let Some(destination) = destination else {
                    transaction.commit()?;
                    return Ok(false);
                };
                if destination != index {
                    let moved = ids.remove(index);
                    ids.insert(destination, moved);
                    let base_rank = transaction.query_row(
                        "SELECT coalesce(max(queue_rank), 0) FROM downloads WHERE priority = :priority",
                        named_params! { ":priority": priority },
                        |row| row.get::<_, i64>(0),
                    )?;
                    for (rank_index, item_id) in ids.iter().enumerate() {
                        transaction.execute(
                            "UPDATE downloads SET queue_rank = :rank WHERE id = :id AND status IN ('Queued', 'Waiting for retry', 'Paused')",
                            named_params! {
                                ":rank": base_rank + (rank_index as i64 + 1) * 1024,
                                ":id": item_id,
                            },
                        )?;
                    }
                }
                transaction.commit()?;
                Ok(destination != index)
            })
        })
        .await
        .context("join queue reorder")?
    }

    pub async fn set_download_priority(
        &self,
        id: &str,
        priority: i32,
        updated_at: i64,
    ) -> Result<bool> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET priority = :priority, updated_at = :updated,
                         queue_rank = coalesce((
                             SELECT max(queue_rank) FROM downloads AS existing
                             WHERE existing.priority = :priority
                               AND existing.id <> downloads.id
                         ), 0) + 1024
                     WHERE id = :id AND status IN ('Queued', 'Waiting for retry', 'Paused')
                       AND priority <> :priority",
                    named_params! {
                        ":priority": priority.clamp(0, 1),
                        ":updated": updated_at,
                        ":id": id,
                    },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join queue priority write")?
    }

    /// Removes a terminal download record from the database without touching its output file.
    /// Active and queued work remains attached to its worker and cannot be removed here.
    pub async fn remove_download(&self, id: &str) -> Result<bool> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "DELETE FROM downloads
                     WHERE id = :id
                       AND status IN ('Completed', 'Failed', 'Canceled', 'Interrupted')",
                    named_params! { ":id": id },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    /// Reattaches a completed history record to a media file that was moved on disk.
    pub async fn update_completed_output_path(
        &self,
        id: &str,
        output_path: String,
    ) -> Result<bool> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET output_path = :path
                     WHERE id = :id AND status = 'Completed'",
                    named_params! { ":path": output_path, ":id": id },
                )?;
                Ok(affected == 1)
            })
        })
        .await
        .context("join completed output path update")?
    }

    /// Removes completed history records from the database without touching output files.
    pub async fn clear_completed_downloads(&self) -> Result<usize> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected =
                    connection.execute("DELETE FROM downloads WHERE status = 'Completed'", [])?;
                Ok(affected)
            })
        })
        .await
        .context("join database write")?
    }

    /// Pauses a waiting job only if its worker has not claimed it yet.
    pub async fn pause_queued_download(&self, id: &str, updated_at: i64) -> Result<bool> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET status = 'Paused', next_retry_at = NULL, error = NULL, updated_at = :updated
                     WHERE id = :id AND status IN ('Queued', 'Waiting for retry')",
                    named_params! { ":id": id, ":updated": updated_at },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    /// Marks a waiting or already-paused job canceled without racing an active worker.
    pub async fn cancel_queued_download(&self, id: &str, updated_at: i64) -> Result<bool> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET status = 'Canceled', next_retry_at = NULL, error = NULL, updated_at = :updated
                     WHERE id = :id AND status IN ('Queued', 'Waiting for retry', 'Paused')",
                    named_params! { ":id": id, ":updated": updated_at },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    /// Requests cancellation for a worker that has claimed the job or is stopping for pause.
    pub async fn request_active_cancel(&self, id: &str, updated_at: i64) -> Result<bool> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET status = 'Canceling', updated_at = :updated
                     WHERE id = :id AND status IN ('Analyzing', 'Downloading', 'Post-processing', 'Pausing')",
                    named_params! { ":id": id, ":updated": updated_at },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    /// Requests a pause for a worker that has already claimed the queue record.
    pub async fn request_active_pause(&self, id: &str, updated_at: i64) -> Result<bool> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET status = 'Pausing', updated_at = :updated
                     WHERE id = :id AND status IN ('Analyzing', 'Downloading', 'Post-processing')",
                    named_params! { ":id": id, ":updated": updated_at },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    pub async fn is_canceling(&self, id: &str) -> Result<bool> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let canceling = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM downloads WHERE id = :id AND status = 'Canceling'
                     )",
                    named_params! { ":id": id },
                    |row| row.get(0),
                )?;
                Ok(canceling)
            })
        })
        .await
        .context("join database read")?
    }

    pub async fn is_pausing(&self, id: &str) -> Result<bool> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let pausing = connection.query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM downloads WHERE id = :id AND status = 'Pausing'
                     )",
                    named_params! { ":id": id },
                    |row| row.get(0),
                )?;
                Ok(pausing)
            })
        })
        .await
        .context("join database read")?
    }

    /// Persists an active phase change only while no pause or cancellation has been requested.
    pub async fn update_active_download(&self, record: DownloadRecord) -> Result<bool> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET title = :title, source = :source, status = :status,
                         progress = :progress, downloaded_bytes = :downloaded,
                         total_bytes = :total, speed_bytes_per_second = :speed,
                         eta_seconds = :eta, output_path = :output, error = :error,
                         updated_at = :updated
                     WHERE id = :id
                       AND status IN ('Analyzing', 'Downloading', 'Post-processing')",
                    named_params! {
                        ":id": record.id,
                        ":title": record.title,
                        ":source": record.source,
                        ":status": record.status,
                        ":progress": record.progress,
                        ":downloaded": sqlite_integer(record.downloaded_bytes, "downloaded_bytes")?,
                        ":total": sqlite_integer(record.total_bytes, "total_bytes")?,
                        ":speed": record.speed_bytes_per_second,
                        ":eta": sqlite_integer(record.eta_seconds, "eta_seconds")?,
                        ":output": record.output_path,
                        ":error": record.error,
                        ":updated": record.updated_at,
                    },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    /// Persists progress only while the job remains active and has no pending stop request.
    pub async fn update_download_progress(&self, record: DownloadRecord) -> Result<bool> {
        let downloaded_bytes = sqlite_integer(record.downloaded_bytes, "downloaded_bytes")?;
        let total_bytes = sqlite_integer(record.total_bytes, "total_bytes")?;
        let eta_seconds = sqlite_integer(record.eta_seconds, "eta_seconds")?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET status = :status, progress = :progress,
                         downloaded_bytes = :downloaded, total_bytes = :total,
                         speed_bytes_per_second = :speed, eta_seconds = :eta,
                         updated_at = :updated
                     WHERE id = :id
                       AND status IN ('Analyzing', 'Downloading', 'Post-processing')",
                    named_params! {
                        ":id": record.id,
                        ":status": record.status,
                        ":progress": record.progress,
                        ":downloaded": downloaded_bytes,
                        ":total": total_bytes,
                        ":speed": record.speed_bytes_per_second,
                        ":eta": eta_seconds,
                        ":updated": record.updated_at,
                    },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    /// Persists a completed or failed outcome only while no pause or cancellation has won.
    pub async fn finish_download(&self, record: DownloadRecord) -> Result<bool> {
        let downloaded_bytes = sqlite_integer(record.downloaded_bytes, "downloaded_bytes")?;
        let total_bytes = sqlite_integer(record.total_bytes, "total_bytes")?;
        let eta_seconds = sqlite_integer(record.eta_seconds, "eta_seconds")?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET title = :title, source = :source, status = :status,
                         progress = :progress, downloaded_bytes = :downloaded,
                         total_bytes = :total, speed_bytes_per_second = :speed,
                         eta_seconds = :eta, output_path = :output, error = :error,
                         updated_at = :updated
                     WHERE id = :id
                       AND status IN ('Analyzing', 'Downloading', 'Post-processing')",
                    named_params! {
                        ":id": record.id,
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
                        ":updated": record.updated_at,
                    },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    /// Records a failed pause/cancel process stop without replacing a newer terminal state.
    pub async fn fail_stopping_download(&self, record: DownloadRecord) -> Result<bool> {
        let downloaded_bytes = sqlite_integer(record.downloaded_bytes, "downloaded_bytes")?;
        let total_bytes = sqlite_integer(record.total_bytes, "total_bytes")?;
        let eta_seconds = sqlite_integer(record.eta_seconds, "eta_seconds")?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET title = :title, source = :source, status = 'Failed',
                         progress = :progress, downloaded_bytes = :downloaded,
                         total_bytes = :total, speed_bytes_per_second = :speed,
                         eta_seconds = :eta, output_path = :output, error = :error,
                         updated_at = :updated
                     WHERE id = :id AND status IN ('Canceling', 'Pausing')",
                    named_params! {
                        ":id": record.id,
                        ":title": record.title,
                        ":source": record.source,
                        ":progress": record.progress,
                        ":downloaded": downloaded_bytes,
                        ":total": total_bytes,
                        ":speed": record.speed_bytes_per_second,
                        ":eta": eta_seconds,
                        ":output": record.output_path,
                        ":error": record.error,
                        ":updated": record.updated_at,
                    },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    /// Completes an active pause after the yt-dlp process tree has stopped.
    pub async fn finish_paused_download(&self, record: DownloadRecord) -> Result<bool> {
        let downloaded_bytes = sqlite_integer(record.downloaded_bytes, "downloaded_bytes")?;
        let total_bytes = sqlite_integer(record.total_bytes, "total_bytes")?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET title = :title, source = :source, status = 'Paused',
                         progress = :progress, downloaded_bytes = :downloaded,
                         total_bytes = :total, speed_bytes_per_second = NULL,
                         eta_seconds = NULL, output_path = :output, error = NULL,
                         updated_at = :updated
                     WHERE id = :id AND status = 'Pausing'",
                    named_params! {
                        ":id": record.id,
                        ":title": record.title,
                        ":source": record.source,
                        ":progress": record.progress,
                        ":downloaded": downloaded_bytes,
                        ":total": total_bytes,
                        ":output": record.output_path,
                        ":updated": record.updated_at,
                    },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    /// Marks a requested cancellation complete only after the worker stopped its process.
    pub async fn finish_canceled_download(&self, record: DownloadRecord) -> Result<bool> {
        let downloaded_bytes = sqlite_integer(record.downloaded_bytes, "downloaded_bytes")?;
        let total_bytes = sqlite_integer(record.total_bytes, "total_bytes")?;
        let eta_seconds = sqlite_integer(record.eta_seconds, "eta_seconds")?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET title = :title, source = :source, status = 'Canceled',
                         progress = :progress, downloaded_bytes = :downloaded,
                         total_bytes = :total, speed_bytes_per_second = :speed,
                         eta_seconds = :eta, output_path = :output, error = :error,
                         updated_at = :updated
                     WHERE id = :id AND status = 'Canceling'",
                    named_params! {
                        ":id": record.id,
                        ":title": record.title,
                        ":source": record.source,
                        ":progress": record.progress,
                        ":downloaded": downloaded_bytes,
                        ":total": total_bytes,
                        ":speed": record.speed_bytes_per_second,
                        ":eta": eta_seconds,
                        ":output": record.output_path,
                        ":error": record.error,
                        ":updated": record.updated_at,
                    },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    /// Atomically moves a failed, interrupted, or canceled record back to the queue.
    /// The conditional update prevents a stale or duplicate UI action from starting a second retry.
    pub async fn retry_download(
        &self,
        id: &str,
        updated_at: i64,
    ) -> Result<Option<DownloadRecord>> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET status = 'Queued', error = NULL, retry_attempts = 0,
                         next_retry_at = NULL, updated_at = :updated,
                         queue_rank = coalesce((
                             SELECT max(queue_rank) FROM downloads AS existing
                             WHERE existing.priority = downloads.priority
                               AND existing.id <> downloads.id
                         ), 0) + 1024
                     WHERE id = :id AND status IN ('Failed', 'Interrupted', 'Canceled')",
                    named_params! { ":id": id, ":updated": updated_at },
                )?;
                if affected == 0 {
                    return Ok(None);
                }

                load_download_by_id(connection, &id)
            })
        })
        .await
        .context("join database write")?
    }

    /// Resumes a paused job by returning it to the queue while retaining its partial progress.
    pub async fn resume_download(
        &self,
        id: &str,
        updated_at: i64,
    ) -> Result<Option<DownloadRecord>> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let affected = connection.execute(
                    "UPDATE downloads
                     SET status = 'Queued', error = NULL, next_retry_at = NULL, updated_at = :updated
                     WHERE id = :id AND status = 'Paused'",
                    named_params! { ":id": id, ":updated": updated_at },
                )?;
                if affected == 0 {
                    return Ok(None);
                }

                load_download_by_id(connection, &id)
            })
        })
        .await
        .context("join database write")?
    }

    fn with_connection<T>(&self, operation: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let connection = Connection::open(&self.path)
            .with_context(|| format!("open application database {}", self.path.display()))?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        operation(&connection)
    }

    fn with_connection_mut<T>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T>,
    ) -> Result<T> {
        let mut connection = Connection::open(&self.path)
            .with_context(|| format!("open application database {}", self.path.display()))?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
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
            download_directory, queue_rank, priority, retry_attempts, next_retry_at
        ) VALUES (
            :id, :url, :title, :source, :status, :progress, :downloaded,
            :total, :speed, :eta, :output, :error, :created, :updated,
            :playlist_index, :playlist_group_id, :playlist_group_title,
            :download_mode, :audio_format, :audio_quality, :video_quality,
            :download_directory,
            coalesce((SELECT max(queue_rank) FROM downloads
                      WHERE priority = :priority), 0) + 1024,
            :priority, :retry_attempts, :next_retry_at
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
            next_retry_at=excluded.next_retry_at",
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
        },
    )?;
    Ok(())
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
                priority, download_directory, retry_attempts, next_retry_at
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
                priority, download_directory, retry_attempts, next_retry_at
         FROM downloads
        ORDER BY CASE WHEN status IN ('Queued', 'Waiting for retry', 'Paused') THEN 0 ELSE 1 END,
                  CASE WHEN status IN ('Queued', 'Waiting for retry', 'Paused') THEN priority ELSE 0 END DESC,
                  CASE WHEN status IN ('Queued', 'Waiting for retry', 'Paused') THEN queue_rank ELSE 0 END ASC,
                  CASE WHEN status IN ('Queued', 'Waiting for retry', 'Paused') THEN rowid ELSE 0 END ASC,
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
mod tests {
    use super::*;

    #[tokio::test]
    async fn max_concurrent_downloads_defaults_bounds_and_persists() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite3");
        let store = Store::open(db_path.clone()).unwrap();

        assert_eq!(store.load_max_concurrent_downloads().await.unwrap(), 1);
        store.set_max_concurrent_downloads(6, 11).await.unwrap();
        assert_eq!(store.load_max_concurrent_downloads().await.unwrap(), 6);
        store.set_max_concurrent_downloads(usize::MAX, 12).await.unwrap();
        assert_eq!(store.load_max_concurrent_downloads().await.unwrap(), MAX_CONCURRENT_DOWNLOADS);
        store.set_max_concurrent_downloads(0, 13).await.unwrap();
        assert_eq!(store.load_max_concurrent_downloads().await.unwrap(), 1);
        store.set_max_concurrent_downloads(5, 14).await.unwrap();
        store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE app_settings SET value = 'invalid' WHERE key = 'max_concurrent_downloads'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(store.load_max_concurrent_downloads().await.unwrap(), 1);
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        assert_eq!(reopened.load_max_concurrent_downloads().await.unwrap(), 1);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn concurrent_fragments_defaults_bounds_and_persists() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite3");
        let store = Store::open(db_path.clone()).unwrap();

        assert_eq!(store.load_concurrent_fragments().await.unwrap(), DEFAULT_CONCURRENT_FRAGMENTS);
        store.set_concurrent_fragments(6, 11).await.unwrap();
        assert_eq!(store.load_concurrent_fragments().await.unwrap(), 6);
        store.set_concurrent_fragments(usize::MAX, 12).await.unwrap();
        assert_eq!(store.load_concurrent_fragments().await.unwrap(), MAX_CONCURRENT_FRAGMENTS);
        store.set_concurrent_fragments(0, 13).await.unwrap();
        assert_eq!(store.load_concurrent_fragments().await.unwrap(), DEFAULT_CONCURRENT_FRAGMENTS);
        store.set_concurrent_fragments(5, 14).await.unwrap();
        drop(store);

        let store = Store::open(db_path.clone()).unwrap();
        assert_eq!(store.load_concurrent_fragments().await.unwrap(), 5);
        store
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE app_settings SET value = 'invalid' WHERE key = 'concurrent_fragments'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(store.load_concurrent_fragments().await.unwrap(), DEFAULT_CONCURRENT_FRAGMENTS);
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        assert_eq!(
            reopened.load_concurrent_fragments().await.unwrap(),
            DEFAULT_CONCURRENT_FRAGMENTS
        );
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn automatic_retry_limit_defaults_bounds_and_persists() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite3");
        let store = Store::open(db_path.clone()).unwrap();

        assert_eq!(store.load_auto_retry_attempts().await.unwrap(), DEFAULT_AUTO_RETRY_ATTEMPTS);
        store.set_auto_retry_attempts(5, 11).await.unwrap();
        assert_eq!(store.load_auto_retry_attempts().await.unwrap(), 5);
        store.set_auto_retry_attempts(usize::MAX, 12).await.unwrap();
        assert_eq!(store.load_auto_retry_attempts().await.unwrap(), MAX_AUTO_RETRY_ATTEMPTS);
        store.set_auto_retry_attempts(0, 13).await.unwrap();
        assert_eq!(store.load_auto_retry_attempts().await.unwrap(), 0);
        drop(store);

        let reopened = Store::open(db_path.clone()).unwrap();
        assert_eq!(reopened.load_auto_retry_attempts().await.unwrap(), 0);
        reopened
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE app_settings SET value = 'invalid' WHERE key = 'auto_retry_attempts'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(reopened.load_auto_retry_attempts().await.unwrap(), DEFAULT_AUTO_RETRY_ATTEMPTS);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn lowering_retry_limit_cancels_only_waits_over_the_new_limit() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();

        for (id, attempts) in [("within-limit", 1), ("over-limit", 2)] {
            let mut record =
                DownloadRecord::queued(id.to_owned(), "https://example.test/media".to_owned(), 10);
            record.status = "Waiting for retry".to_owned();
            record.retry_attempts = attempts;
            record.next_retry_at = Some(50);
            record.error = Some("A temporary network error interrupted the request.".to_owned());
            store.save_download(record).await.unwrap();
        }

        assert_eq!(store.set_auto_retry_attempts(1, 60).await.unwrap(), 1);
        let records = store.list_downloads().await.unwrap();
        let within_limit = records.iter().find(|record| record.id == "within-limit").unwrap();
        assert_eq!(within_limit.status, "Waiting for retry");
        assert_eq!(within_limit.next_retry_at, Some(50));
        let over_limit = records.iter().find(|record| record.id == "over-limit").unwrap();
        assert_eq!(over_limit.status, "Failed");
        assert_eq!(over_limit.next_retry_at, None);
        assert!(
            over_limit.error.as_deref().unwrap_or_default().contains("retry limit was lowered")
        );

        assert_eq!(store.set_auto_retry_attempts(0, 61).await.unwrap(), 1);
        let records = store.list_downloads().await.unwrap();
        assert_eq!(
            records.iter().find(|record| record.id == "within-limit").unwrap().status,
            "Failed"
        );
        drop(store);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn retry_scheduling_observes_a_disabled_limit_atomically() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        store.set_auto_retry_attempts(0, 10).await.unwrap();
        store
            .save_download(DownloadRecord::queued(
                "disabled-retry".to_owned(),
                "https://example.test/media".to_owned(),
                10,
            ))
            .await
            .unwrap();
        let mut active =
            store.claim_queued_download_if_next("disabled-retry", 11).await.unwrap().unwrap();
        active.status = "Downloading".to_owned();
        active.error = Some("A temporary network error interrupted the request.".to_owned());
        active.updated_at = 12;
        assert!(store.update_active_download(active.clone()).await.unwrap());

        assert_eq!(store.schedule_automatic_retry(active, 50).await.unwrap(), None);
        let record = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(record.status, "Downloading");
        assert_eq!(record.retry_attempts, 0);
        assert_eq!(record.next_retry_at, None);
        drop(store);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn automatic_retry_deadline_and_attempt_survive_restart_and_promote_when_due() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite3");
        let store = Store::open(db_path.clone()).unwrap();
        store
            .save_download(DownloadRecord::queued(
                "retry-job".to_owned(),
                "https://example.test/media".to_owned(),
                10,
            ))
            .await
            .unwrap();
        let mut active =
            store.claim_queued_download_if_next("retry-job", 11).await.unwrap().unwrap();
        active.status = "Downloading".to_owned();
        active.progress = 37.0;
        active.downloaded_bytes = Some(370);
        active.total_bytes = Some(1_000);
        active.error = Some("A temporary network error interrupted the request.".to_owned());
        active.updated_at = 12;
        assert!(store.update_active_download(active.clone()).await.unwrap());
        assert_eq!(store.schedule_automatic_retry(active, 50).await.unwrap(), Some(3));

        let waiting = store
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.id == "retry-job")
            .unwrap();
        assert_eq!(waiting.status, "Waiting for retry");
        assert_eq!(waiting.retry_attempts, 1);
        assert_eq!(waiting.next_retry_at, Some(50));
        assert_eq!(waiting.progress, 37.0);
        assert_eq!(store.next_retry_at().await.unwrap(), Some(50));
        assert_eq!(store.promote_due_retries(49).await.unwrap(), 0);
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        let still_waiting = reopened
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.id == "retry-job")
            .unwrap();
        assert_eq!(still_waiting.status, "Waiting for retry");
        assert_eq!(still_waiting.retry_attempts, 1);
        assert_eq!(still_waiting.next_retry_at, Some(50));
        assert_eq!(reopened.promote_due_retries(50).await.unwrap(), 1);
        let queued = reopened
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.id == "retry-job")
            .unwrap();
        assert_eq!(queued.status, "Queued");
        assert_eq!(queued.retry_attempts, 1);
        assert_eq!(queued.next_retry_at, None);
        assert_eq!(reopened.next_retry_at().await.unwrap(), None);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn download_state_survives_reopening_the_database() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite");
        let store = Store::open(db_path.clone()).unwrap();
        let mut record =
            DownloadRecord::queued("job-1".to_owned(), "https://example.test/media".to_owned(), 10);
        record.playlist_index = Some(4);
        record.download_mode = DownloadMode::AudioOnly;
        record.audio_format = AudioFormat::Opus;
        record.audio_quality = AudioQuality::K320;
        record.video_quality = VideoQuality::P2160;
        record.downloaded_bytes = Some(345);
        record.total_bytes = Some(1_000);
        record.eta_seconds = Some(12);
        store.save_download(record).await.unwrap();
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        let rows = reopened.list_downloads().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "Queued");
        assert_eq!(rows[0].url, "https://example.test/media");
        assert_eq!(rows[0].downloaded_bytes, Some(345));
        assert_eq!(rows[0].total_bytes, Some(1_000));
        assert_eq!(rows[0].eta_seconds, Some(12));
        assert_eq!(rows[0].playlist_index, Some(4));
        assert_eq!(rows[0].download_mode, DownloadMode::AudioOnly);
        assert_eq!(rows[0].audio_format, AudioFormat::Opus);
        assert_eq!(rows[0].audio_quality, AudioQuality::K320);
        assert_eq!(rows[0].video_quality, VideoQuality::P2160);
        assert!(rows[0].queue_order > 0);
        assert_eq!(rows[0].priority, 0);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn queue_rank_priority_reordering_and_claiming_share_one_order() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let records = ["first", "second", "third"]
            .into_iter()
            .enumerate()
            .map(|(index, id)| {
                DownloadRecord::queued(
                    id.to_owned(),
                    format!("https://example.test/{id}"),
                    index as i64 + 10,
                )
            })
            .collect::<Vec<_>>();
        store.save_downloads(records).await.unwrap();

        let initial = store.list_downloads().await.unwrap();
        assert_eq!(
            initial.iter().map(|record| record.id.as_str()).collect::<Vec<_>>(),
            ["first", "second", "third"]
        );
        assert_eq!(initial[1].queue_order - initial[0].queue_order, 1024);
        assert_eq!(initial[2].queue_order - initial[1].queue_order, 1024);

        assert!(store.move_download("third", QueueMove::Top).await.unwrap());
        assert!(store.set_download_priority("first", 1, 19).await.unwrap());
        assert!(store.set_download_priority("second", 1, 20).await.unwrap());
        assert_eq!(store.next_queued_download().await.unwrap().unwrap().id, "first");
        assert!(store.claim_queued_download_if_next("third", 21).await.unwrap().is_none());
        assert_eq!(
            store.claim_queued_download_if_next("first", 22).await.unwrap().unwrap().status,
            "Analyzing"
        );
        assert_eq!(store.next_queued_download().await.unwrap().unwrap().id, "second");

        let pending_ids = store
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .filter(|record| record.status == "Queued")
            .map(|record| record.id)
            .collect::<Vec<_>>();
        assert_eq!(pending_ids, ["second", "third"]);
        assert!(!store.move_download("first", QueueMove::Bottom).await.unwrap());
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn reordering_pending_rows_does_not_reuse_active_queue_ranks() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let first =
            DownloadRecord::queued("first".to_owned(), "https://example.test/first".to_owned(), 10);
        let mut active = DownloadRecord::queued(
            "active".to_owned(),
            "https://example.test/active".to_owned(),
            11,
        );
        active.status = "Downloading".to_owned();
        let second = DownloadRecord::queued(
            "second".to_owned(),
            "https://example.test/second".to_owned(),
            12,
        );
        store.save_downloads(vec![first, active, second]).await.unwrap();

        assert!(store.move_download("second", QueueMove::Top).await.unwrap());
        let rows = store.list_downloads().await.unwrap();
        let mut ranks = rows.iter().map(|record| record.queue_order).collect::<Vec<_>>();
        ranks.sort_unstable();
        assert!(ranks.windows(2).all(|pair| pair[0] != pair[1]));
        assert_eq!(store.next_queued_download().await.unwrap().unwrap().id, "second");
        assert_eq!(rows.iter().find(|record| record.id == "active").unwrap().status, "Downloading");
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn video_quality_survives_reopen_and_retry_for_video_jobs() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite");
        let store = Store::open(db_path.clone()).unwrap();
        let mut record = DownloadRecord::queued(
            "video-retry".to_owned(),
            "https://example.test/video".to_owned(),
            10,
        );
        record.status = "Failed".to_owned();
        record.video_quality = VideoQuality::P1440;
        store.save_download(record).await.unwrap();
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        let retried = reopened.retry_download("video-retry", 20).await.unwrap().unwrap();
        assert_eq!(retried.download_mode, DownloadMode::Video);
        assert_eq!(retried.video_quality, VideoQuality::P1440);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn download_presets_round_trip_and_unknown_settings_fall_back_independently() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite");
        let store = Store::open(db_path.clone()).unwrap();
        assert_eq!(store.load_download_presets().await.unwrap(), DownloadPresets::default());
        let presets = DownloadPresets {
            mode: DownloadMode::AudioOnly,
            audio_format: AudioFormat::Flac,
            audio_quality: AudioQuality::K192,
            video_quality: VideoQuality::P1440,
        };
        store.set_download_presets(presets, 10).await.unwrap();
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        assert_eq!(reopened.load_download_presets().await.unwrap(), presets);
        reopened
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE app_settings SET value = 'unknown' WHERE key = 'new_download_audio_quality'",
                    [],
                )?;
                connection.execute(
                    "UPDATE app_settings SET value = '8k' WHERE key = 'new_download_video_quality'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let recovered = reopened.load_download_presets().await.unwrap();
        assert_eq!(recovered.mode, DownloadMode::AudioOnly);
        assert_eq!(recovered.audio_format, AudioFormat::Flac);
        assert_eq!(recovered.audio_quality, AudioQuality::Best);
        assert_eq!(recovered.video_quality, VideoQuality::Best);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn clipboard_monitoring_is_off_by_default_and_persists_explicit_choice() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite");
        let store = Store::open(db_path.clone()).unwrap();
        assert!(!store.load_clipboard_monitoring_enabled().await.unwrap());
        store.set_clipboard_monitoring_enabled(true, 10).await.unwrap();
        assert!(store.load_clipboard_monitoring_enabled().await.unwrap());
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        assert!(reopened.load_clipboard_monitoring_enabled().await.unwrap());
        reopened
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE app_settings SET value = 'yes' WHERE key = 'clipboard_monitoring_enabled'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        assert!(!reopened.load_clipboard_monitoring_enabled().await.unwrap());
        reopened.set_clipboard_monitoring_enabled(false, 20).await.unwrap();
        assert!(!reopened.load_clipboard_monitoring_enabled().await.unwrap());
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn tray_preferences_default_off_and_persist_independently() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite");
        let store = Store::open(db_path.clone()).unwrap();
        assert_eq!(store.load_tray_preferences().await.unwrap(), TrayPreferences::default());

        store
            .set_tray_preferences(
                TrayPreferences { close_to_tray: true, minimize_to_tray: false },
                10,
            )
            .await
            .unwrap();
        assert_eq!(
            store.load_tray_preferences().await.unwrap(),
            TrayPreferences { close_to_tray: true, minimize_to_tray: false }
        );
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        assert_eq!(
            reopened.load_tray_preferences().await.unwrap(),
            TrayPreferences { close_to_tray: true, minimize_to_tray: false }
        );
        reopened
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE app_settings SET value = 'yes' WHERE key = 'minimize_to_tray_enabled'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            reopened.load_tray_preferences().await.unwrap(),
            TrayPreferences { close_to_tray: true, minimize_to_tray: false }
        );
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn changing_form_presets_does_not_rewrite_existing_jobs_retries_or_resumes() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite");
        let store = Store::open(db_path.clone()).unwrap();
        let job_presets = DownloadPresets {
            mode: DownloadMode::AudioOnly,
            audio_format: AudioFormat::Opus,
            audio_quality: AudioQuality::K128,
            video_quality: VideoQuality::P480,
        };
        let mut job = DownloadRecord::queued(
            "older-job".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        job.status = "Failed".to_owned();
        job.download_mode = job_presets.mode;
        job.audio_format = job_presets.audio_format;
        job.audio_quality = job_presets.audio_quality;
        job.video_quality = job_presets.video_quality;
        store.save_download(job).await.unwrap();
        let resume_presets = DownloadPresets {
            mode: DownloadMode::AudioOnly,
            audio_format: AudioFormat::M4a,
            audio_quality: AudioQuality::K192,
            video_quality: VideoQuality::P720,
        };
        let mut paused_job = DownloadRecord::queued(
            "paused-job".to_owned(),
            "https://example.test/paused".to_owned(),
            11,
        );
        paused_job.status = "Paused".to_owned();
        paused_job.download_mode = resume_presets.mode;
        paused_job.audio_format = resume_presets.audio_format;
        paused_job.audio_quality = resume_presets.audio_quality;
        paused_job.video_quality = resume_presets.video_quality;
        store.save_download(paused_job).await.unwrap();

        let current_form = DownloadPresets {
            mode: DownloadMode::Video,
            audio_format: AudioFormat::Aac,
            audio_quality: AudioQuality::K320,
            video_quality: VideoQuality::P2160,
        };
        store.set_download_presets(current_form, 20).await.unwrap();
        let retried = store.retry_download("older-job", 30).await.unwrap().unwrap();
        assert_eq!(retried.download_mode, job_presets.mode);
        assert_eq!(retried.audio_format, job_presets.audio_format);
        assert_eq!(retried.audio_quality, job_presets.audio_quality);
        assert_eq!(retried.video_quality, job_presets.video_quality);
        let resumed = store.resume_download("paused-job", 31).await.unwrap().unwrap();
        assert_eq!(resumed.download_mode, resume_presets.mode);
        assert_eq!(resumed.audio_format, resume_presets.audio_format);
        assert_eq!(resumed.audio_quality, resume_presets.audio_quality);
        assert_eq!(resumed.video_quality, resume_presets.video_quality);
        assert_eq!(store.load_download_presets().await.unwrap(), current_form);
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        assert_eq!(reopened.load_download_presets().await.unwrap(), current_form);
        let saved_jobs = reopened.list_downloads().await.unwrap();
        let saved_job = saved_jobs.iter().find(|job| job.id == "older-job").unwrap();
        assert_eq!(saved_job.download_mode, job_presets.mode);
        assert_eq!(saved_job.audio_format, job_presets.audio_format);
        assert_eq!(saved_job.audio_quality, job_presets.audio_quality);
        assert_eq!(saved_job.video_quality, job_presets.video_quality);
        let saved_paused_job = saved_jobs.iter().find(|job| job.id == "paused-job").unwrap();
        assert_eq!(saved_paused_job.download_mode, resume_presets.mode);
        assert_eq!(saved_paused_job.audio_format, resume_presets.audio_format);
        assert_eq!(saved_paused_job.audio_quality, resume_presets.audio_quality);
        assert_eq!(saved_paused_job.video_quality, resume_presets.video_quality);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn default_destination_changes_only_future_jobs_and_job_path_survives_retry_and_reopen() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite");
        let store = Store::open(db_path.clone()).unwrap();
        let first_default = temp_dir.join("first").to_string_lossy().into_owned();
        let selected_default =
            store.get_or_initialize_download_directory(first_default.clone()).await.unwrap();
        assert_eq!(selected_default, first_default);

        let mut queued = DownloadRecord::queued(
            "snapshotted-job".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        queued.download_directory = selected_default.clone();
        store.save_download(queued.clone()).await.unwrap();

        let second_default = temp_dir.join("second").to_string_lossy().into_owned();
        store.set_default_download_directory(second_default.clone(), 20).await.unwrap();
        assert_eq!(
            store
                .get_or_initialize_download_directory("ignored-fallback".to_owned())
                .await
                .unwrap(),
            second_default
        );
        let still_queued = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(still_queued.download_directory, first_default);

        queued.status = "Failed".to_owned();
        store.save_download(queued).await.unwrap();
        let retried = store.retry_download("snapshotted-job", 30).await.unwrap().unwrap();
        assert_eq!(retried.download_directory, first_default);
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        assert_eq!(
            reopened
                .get_or_initialize_download_directory("ignored-fallback".to_owned())
                .await
                .unwrap(),
            second_default
        );
        assert_eq!(reopened.list_downloads().await.unwrap()[0].download_directory, first_default);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn alac_and_vorbis_choices_round_trip_through_sqlite() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("state.sqlite");
        let store = Store::open(db_path.clone()).unwrap();

        for (id, audio_format, audio_quality) in [
            ("alac-job", AudioFormat::Alac, AudioQuality::Best),
            ("vorbis-job", AudioFormat::Vorbis, AudioQuality::K192),
        ] {
            let mut record =
                DownloadRecord::queued(id.to_owned(), "https://example.test/media".to_owned(), 10);
            record.download_mode = DownloadMode::AudioOnly;
            record.audio_format = audio_format;
            record.audio_quality = audio_quality;
            store.save_download(record).await.unwrap();
        }
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        let rows = reopened.list_downloads().await.unwrap();
        for (id, audio_format, audio_quality) in [
            ("alac-job", AudioFormat::Alac, AudioQuality::Best),
            ("vorbis-job", AudioFormat::Vorbis, AudioQuality::K192),
        ] {
            let record = rows.iter().find(|record| record.id == id).unwrap();
            assert_eq!(record.download_mode, DownloadMode::AudioOnly);
            assert_eq!(record.audio_format, audio_format);
            assert_eq!(record.audio_quality, audio_quality);
        }
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn adds_playlist_index_to_legacy_database_and_preserves_selected_item() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("legacy.sqlite3");
        {
            let connection = Connection::open(&db_path).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE downloads (
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
                        updated_at INTEGER NOT NULL
                    );
                    INSERT INTO downloads (
                        id, url, title, source, status, progress,
                        created_at, updated_at
                    ) VALUES (
                        'legacy-job', 'https://example.test/list', 'Old row', 'Example',
                        'Queued', 0, 10, 20
                    );
                    INSERT INTO downloads (
                        id, url, title, source, status, progress,
                        created_at, updated_at
                    ) VALUES (
                        'legacy-job-later', 'https://example.test/other', 'Later row', 'Example',
                        'Queued', 0, 5, 20
                    );",
                )
                .unwrap();
        }

        let store = Store::open(db_path.clone()).unwrap();
        let legacy_default = temp_dir.join("Downloads").to_string_lossy().into_owned();
        assert_eq!(
            store.get_or_initialize_download_directory(legacy_default.clone()).await.unwrap(),
            legacy_default
        );
        store.backfill_legacy_download_directories(legacy_default.clone()).await.unwrap();
        let rows = store.list_downloads().await.unwrap();
        assert_eq!(
            rows.iter().map(|record| record.id.as_str()).collect::<Vec<_>>(),
            ["legacy-job", "legacy-job-later"]
        );
        assert_eq!(rows[1].queue_order, 2048);
        let legacy = &rows[0];
        assert_eq!(legacy.id, "legacy-job");
        assert_eq!(legacy.queue_order, 1024);
        assert_eq!(legacy.priority, 0);
        assert_eq!(legacy.playlist_index, None);
        assert_eq!(legacy.playlist_group_id, None);
        assert_eq!(legacy.playlist_group_title, None);
        assert_eq!(legacy.download_mode, DownloadMode::Video);
        assert_eq!(legacy.audio_format, AudioFormat::Original);
        assert_eq!(legacy.audio_quality, AudioQuality::Best);
        assert_eq!(legacy.video_quality, VideoQuality::Best);
        assert_eq!(legacy.download_directory, legacy_default);
        assert_eq!(legacy.retry_attempts, 0);
        assert_eq!(legacy.next_retry_at, None);

        let mut selected = DownloadRecord::queued(
            "selected-job".to_owned(),
            "https://example.test/list".to_owned(),
            30,
        );
        selected.playlist_index = Some(7);
        selected.playlist_group_id = Some("playlist-group-1".to_owned());
        selected.playlist_group_title = Some("Selected playlist".to_owned());
        selected.download_mode = DownloadMode::AudioOnly;
        selected.audio_format = AudioFormat::M4a;
        selected.audio_quality = AudioQuality::K192;
        selected.video_quality = VideoQuality::P720;
        selected.download_directory = temp_dir.join("Selected").to_string_lossy().into_owned();
        store.save_download(selected).await.unwrap();
        drop(store);

        let reopened = Store::open(db_path).unwrap();
        let selected = reopened
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.id == "selected-job")
            .unwrap();
        assert_eq!(selected.url, "https://example.test/list");
        assert_eq!(selected.playlist_index, Some(7));
        assert_eq!(selected.playlist_group_id.as_deref(), Some("playlist-group-1"));
        assert_eq!(selected.playlist_group_title.as_deref(), Some("Selected playlist"));
        assert_eq!(selected.download_mode, DownloadMode::AudioOnly);
        assert_eq!(selected.audio_format, AudioFormat::M4a);
        assert_eq!(selected.audio_quality, AudioQuality::K192);
        assert_eq!(selected.video_quality, VideoQuality::P720);
        assert_eq!(
            selected.download_directory,
            temp_dir.join("Selected").to_string_lossy().into_owned()
        );
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn adds_default_video_quality_to_existing_audio_settings_database() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("v018.sqlite3");
        {
            let connection = Connection::open(&db_path).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE downloads (
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
                        download_mode TEXT NOT NULL DEFAULT 'video',
                        audio_format TEXT NOT NULL DEFAULT 'original',
                        audio_quality TEXT NOT NULL DEFAULT 'best'
                    );
                    INSERT INTO downloads (
                        id, url, title, status, progress, created_at, updated_at
                    ) VALUES (
                        'existing-job', 'https://example.test/video', 'Existing job',
                        'Paused', 25, 10, 20
                    );",
                )
                .unwrap();
        }

        let store = Store::open(db_path).unwrap();
        let existing = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(existing.id, "existing-job");
        assert_eq!(existing.status, "Paused");
        assert_eq!(existing.video_quality, VideoQuality::Best);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn rejects_unsigned_byte_counts_outside_sqlite_integer_range() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut record =
            DownloadRecord::queued("job-1".to_owned(), "https://example.test/media".to_owned(), 10);
        record.downloaded_bytes = Some(u64::MAX);

        let error = store.save_download(record).await.unwrap_err();
        assert!(error.to_string().contains("SQLite's signed integer range"));
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn remove_download_deletes_only_terminal_rows_and_keeps_output_file() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let output_path = temp_dir.join("finished.mp4");
        std::fs::write(&output_path, b"downloaded media").unwrap();

        let mut completed = DownloadRecord::queued(
            "completed".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        completed.status = "Completed".to_owned();
        completed.output_path = Some(output_path.to_string_lossy().into_owned());
        store.save_download(completed).await.unwrap();

        let mut active = DownloadRecord::queued(
            "active".to_owned(),
            "https://example.test/active".to_owned(),
            11,
        );
        active.status = "Downloading".to_owned();
        store.save_download(active).await.unwrap();

        assert!(store.remove_download("completed").await.unwrap());
        assert!(!store.remove_download("completed").await.unwrap());
        assert!(!store.remove_download("active").await.unwrap());

        let rows = store.list_downloads().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "active");
        assert!(output_path.exists());
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn moved_library_file_can_be_relinked_only_for_completed_downloads() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut completed = DownloadRecord::queued(
            "completed".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        completed.status = "Completed".to_owned();
        completed.output_path = Some("C:\\old-location\\media.mp4".to_owned());
        store.save_download(completed).await.unwrap();
        let active = DownloadRecord::queued(
            "active".to_owned(),
            "https://example.test/active".to_owned(),
            11,
        );
        store.save_download(active).await.unwrap();

        assert!(
            store
                .update_completed_output_path("completed", "D:\\Media\\moved.mp4".to_owned())
                .await
                .unwrap()
        );
        assert!(
            !store
                .update_completed_output_path("active", "D:\\Media\\active.mp4".to_owned())
                .await
                .unwrap()
        );
        let records = store.list_downloads().await.unwrap();
        let completed = records.iter().find(|record| record.id == "completed").unwrap();
        assert_eq!(completed.output_path.as_deref(), Some("D:\\Media\\moved.mp4"));
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn clear_completed_downloads_returns_count_and_preserves_other_rows_and_files() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let completed_path = temp_dir.join("completed.mp4");
        std::fs::write(&completed_path, b"downloaded media").unwrap();

        for id in ["completed-1", "completed-2"] {
            let mut record =
                DownloadRecord::queued(id.to_owned(), "https://example.test/media".to_owned(), 10);
            record.status = "Completed".to_owned();
            record.output_path = Some(completed_path.to_string_lossy().into_owned());
            store.save_download(record).await.unwrap();
        }

        let queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/queued".to_owned(),
            11,
        );
        store.save_download(queued).await.unwrap();

        let mut failed = DownloadRecord::queued(
            "failed".to_owned(),
            "https://example.test/failed".to_owned(),
            12,
        );
        failed.status = "Failed".to_owned();
        store.save_download(failed).await.unwrap();

        assert_eq!(store.clear_completed_downloads().await.unwrap(), 2);
        assert_eq!(store.clear_completed_downloads().await.unwrap(), 0);

        let mut remaining: Vec<_> = store
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .map(|record| (record.id, record.status))
            .collect();
        remaining.sort();
        assert_eq!(
            remaining,
            vec![
                ("failed".to_owned(), "Failed".to_owned()),
                ("queued".to_owned(), "Queued".to_owned()),
            ]
        );
        assert!(completed_path.exists());
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn retry_download_requeues_failed_rows_once() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut failed = DownloadRecord::queued(
            "failed".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        failed.status = "Failed".to_owned();
        failed.progress = 34.5;
        failed.downloaded_bytes = Some(345);
        failed.total_bytes = Some(1_000);
        failed.retry_attempts = 4;
        failed.next_retry_at = Some(100);
        failed.error = Some("temporary network error".to_owned());
        failed.playlist_index = Some(5);
        failed.download_mode = DownloadMode::AudioOnly;
        failed.audio_format = AudioFormat::Mp3;
        failed.audio_quality = AudioQuality::K128;
        failed.video_quality = VideoQuality::P1080;
        store.save_download(failed).await.unwrap();

        let queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/queued".to_owned(),
            11,
        );
        store.save_download(queued).await.unwrap();

        let retried = store.retry_download("failed", 20).await.unwrap().unwrap();
        assert_eq!(retried.status, "Queued");
        assert_eq!(retried.error, None);
        assert_eq!(retried.progress, 34.5);
        assert_eq!(retried.downloaded_bytes, Some(345));
        assert_eq!(retried.total_bytes, Some(1_000));
        assert_eq!(retried.updated_at, 20);
        assert_eq!(retried.playlist_index, Some(5));
        assert_eq!(retried.download_mode, DownloadMode::AudioOnly);
        assert_eq!(retried.audio_format, AudioFormat::Mp3);
        assert_eq!(retried.audio_quality, AudioQuality::K128);
        assert_eq!(retried.video_quality, VideoQuality::P1080);
        assert_eq!(retried.retry_attempts, 0);
        assert_eq!(retried.next_retry_at, None);
        assert!(store.retry_download("failed", 21).await.unwrap().is_none());
        assert!(store.retry_download("queued", 22).await.unwrap().is_none());

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn active_cancel_blocks_stale_updates_and_canceled_rows_retry_with_partial_progress() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut active = DownloadRecord::queued(
            "active".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        active.status = "Downloading".to_owned();
        active.progress = 34.5;
        active.downloaded_bytes = Some(345);
        active.total_bytes = Some(1_000);
        active.playlist_index = Some(3);
        store.save_download(active.clone()).await.unwrap();

        assert!(store.request_active_cancel("active", 20).await.unwrap());
        assert!(store.is_canceling("active").await.unwrap());

        let mut stale_progress = active.clone();
        stale_progress.updated_at = 21;
        stale_progress.progress = 50.0;
        stale_progress.downloaded_bytes = Some(500);
        assert!(!store.update_download_progress(stale_progress.clone()).await.unwrap());

        let mut stale_phase = active.clone();
        stale_phase.status = "Post-processing".to_owned();
        stale_phase.updated_at = 22;
        assert!(!store.update_active_download(stale_phase).await.unwrap());

        let mut stale_terminal = active.clone();
        stale_terminal.status = "Completed".to_owned();
        stale_terminal.updated_at = 23;
        assert!(!store.finish_download(stale_terminal.clone()).await.unwrap());
        let persisted = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(persisted.status, "Canceling");
        assert_eq!(persisted.progress, 34.5);
        assert_eq!(persisted.downloaded_bytes, Some(345));

        stale_progress.status = "Canceled".to_owned();
        stale_progress.error = Some("Canceled by user; partial files were kept.".to_owned());
        stale_progress.updated_at = 24;
        assert!(store.finish_canceled_download(stale_progress).await.unwrap());
        assert!(!store.finish_download(stale_terminal).await.unwrap());

        let retried = store.retry_download("active", 25).await.unwrap().unwrap();
        assert_eq!(retried.status, "Queued");
        assert_eq!(retried.progress, 50.0);
        assert_eq!(retried.downloaded_bytes, Some(500));
        assert_eq!(retried.total_bytes, Some(1_000));
        assert_eq!(retried.playlist_index, Some(3));
        assert!(retried.error.is_none());
        assert!(store.retry_download("active", 26).await.unwrap().is_none());

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn queued_pause_blocks_claim_and_resume_preserves_download_choices_and_partial_state() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        queued.download_mode = crate::model::DownloadMode::AudioOnly;
        queued.audio_format = crate::model::AudioFormat::Mp3;
        queued.audio_quality = crate::model::AudioQuality::K192;
        queued.video_quality = crate::model::VideoQuality::P480;
        queued.download_directory = "C:\\Media\\QueueSnapshot".to_owned();
        queued.progress = 48.0;
        queued.downloaded_bytes = Some(480);
        queued.total_bytes = Some(1_000);
        queued.playlist_index = Some(4);
        store.save_download(queued).await.unwrap();

        assert!(store.pause_queued_download("queued", 20).await.unwrap());
        assert!(!store.pause_queued_download("queued", 21).await.unwrap());
        assert!(store.claim_queued_download_if_next("queued", 21).await.unwrap().is_none());

        let paused = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(paused.status, "Paused");
        assert_eq!(paused.progress, 48.0);
        assert_eq!(paused.downloaded_bytes, Some(480));
        assert_eq!(paused.audio_format, crate::model::AudioFormat::Mp3);
        assert_eq!(paused.audio_quality, crate::model::AudioQuality::K192);
        assert_eq!(paused.video_quality, crate::model::VideoQuality::P480);
        assert_eq!(paused.download_directory, "C:\\Media\\QueueSnapshot");
        assert_eq!(paused.playlist_index, Some(4));

        let resumed = store.resume_download("queued", 22).await.unwrap().unwrap();
        assert_eq!(resumed.status, "Queued");
        assert_eq!(resumed.progress, 48.0);
        assert_eq!(resumed.downloaded_bytes, Some(480));
        assert_eq!(resumed.download_mode, crate::model::DownloadMode::AudioOnly);
        assert_eq!(resumed.audio_format, crate::model::AudioFormat::Mp3);
        assert_eq!(resumed.audio_quality, crate::model::AudioQuality::K192);
        assert_eq!(resumed.video_quality, crate::model::VideoQuality::P480);
        assert_eq!(resumed.download_directory, "C:\\Media\\QueueSnapshot");
        assert_eq!(resumed.playlist_index, Some(4));
        assert!(store.resume_download("queued", 23).await.unwrap().is_none());
        assert_eq!(
            store.claim_queued_download_if_next("queued", 24).await.unwrap().unwrap().status,
            "Analyzing"
        );

        let paused_for_cancel = DownloadRecord::queued(
            "paused-for-cancel".to_owned(),
            "https://example.test/other-media".to_owned(),
            11,
        );
        store.save_download(paused_for_cancel).await.unwrap();
        assert!(store.pause_queued_download("paused-for-cancel", 25).await.unwrap());
        assert!(store.cancel_queued_download("paused-for-cancel", 26).await.unwrap());
        let canceled = store
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.id == "paused-for-cancel")
            .unwrap();
        assert_eq!(canceled.status, "Canceled");

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn concurrent_queued_pause_and_worker_claim_have_one_winner() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        store.save_download(queued).await.unwrap();

        let (paused, claimed) = tokio::join!(
            store.pause_queued_download("queued", 20),
            store.claim_queued_download_if_next("queued", 21),
        );
        let paused = paused.unwrap();
        let claimed = claimed.unwrap();
        assert_ne!(paused, claimed.is_some());

        let persisted = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(persisted.status, if paused { "Paused" } else { "Analyzing" });
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn active_pause_blocks_stale_writes_and_cancel_can_supersede_pausing() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut active = DownloadRecord::queued(
            "active".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        active.status = "Downloading".to_owned();
        active.progress = 30.0;
        active.downloaded_bytes = Some(300);
        active.total_bytes = Some(1_000);
        store.save_download(active.clone()).await.unwrap();

        assert!(store.request_active_pause("active", 20).await.unwrap());
        assert!(store.is_pausing("active").await.unwrap());
        let mut stale = active.clone();
        stale.status = "Post-processing".to_owned();
        stale.progress = 60.0;
        stale.downloaded_bytes = Some(600);
        assert!(!store.update_download_progress(stale.clone()).await.unwrap());
        assert!(!store.finish_download(stale.clone()).await.unwrap());

        assert!(store.request_active_cancel("active", 21).await.unwrap());
        assert!(!store.finish_paused_download(stale.clone()).await.unwrap());
        let mut canceled = active;
        canceled.status = "Canceled".to_owned();
        canceled.error = Some("Canceled by user; partial files were kept.".to_owned());
        assert!(store.finish_canceled_download(canceled.clone()).await.unwrap());
        assert!(!store.finish_paused_download(canceled).await.unwrap());
        let canceled = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(canceled.status, "Canceled");
        assert_eq!(canceled.progress, 30.0);
        assert_eq!(canceled.downloaded_bytes, Some(300));

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn concurrent_active_pause_and_terminal_write_have_one_persisted_winner() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut active = DownloadRecord::queued(
            "active".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        active.status = "Downloading".to_owned();
        store.save_download(active.clone()).await.unwrap();

        let mut completed = active;
        completed.status = "Completed".to_owned();
        completed.progress = 100.0;
        let (pause_requested, terminal_saved) = tokio::join!(
            store.request_active_pause("active", 20),
            store.finish_download(completed),
        );
        let pause_requested = pause_requested.unwrap();
        let terminal_saved = terminal_saved.unwrap();
        assert_ne!(pause_requested, terminal_saved);

        let persisted = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(persisted.status, if pause_requested { "Pausing" } else { "Completed" });
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn active_pause_finishes_as_paused_and_can_resume_without_losing_progress() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut active = DownloadRecord::queued(
            "active".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        active.status = "Downloading".to_owned();
        active.progress = 62.5;
        active.downloaded_bytes = Some(625);
        active.total_bytes = Some(1_000);
        active.speed_bytes_per_second = Some(200.0);
        active.eta_seconds = Some(2);
        store.save_download(active.clone()).await.unwrap();

        assert!(store.request_active_pause("active", 20).await.unwrap());
        active.status = "Paused".to_owned();
        active.updated_at = 21;
        assert!(store.finish_paused_download(active).await.unwrap());
        let resumed = store.resume_download("active", 22).await.unwrap().unwrap();
        assert_eq!(resumed.status, "Queued");
        assert_eq!(resumed.progress, 62.5);
        assert_eq!(resumed.downloaded_bytes, Some(625));
        assert_eq!(resumed.total_bytes, Some(1_000));
        assert_eq!(resumed.speed_bytes_per_second, None);
        assert_eq!(resumed.eta_seconds, None);
        assert!(!store.request_active_pause("active", 23).await.unwrap());

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn active_cancel_and_terminal_write_have_one_persisted_winner() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut active = DownloadRecord::queued(
            "racing".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        active.status = "Downloading".to_owned();
        store.save_download(active.clone()).await.unwrap();

        let mut completed = active;
        completed.status = "Completed".to_owned();
        completed.progress = 100.0;
        let (cancel_requested, terminal_saved) = tokio::join!(
            store.request_active_cancel("racing", 20),
            store.finish_download(completed),
        );
        let cancel_requested = cancel_requested.unwrap();
        let terminal_saved = terminal_saved.unwrap();
        assert_ne!(cancel_requested, terminal_saved);

        let persisted = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(persisted.status, if cancel_requested { "Canceling" } else { "Completed" });
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn failed_process_tree_stop_only_finishes_a_stopping_row() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut active = DownloadRecord::queued(
            "active".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        active.status = "Downloading".to_owned();
        store.save_download(active.clone()).await.unwrap();
        assert!(store.request_active_cancel("active", 20).await.unwrap());

        active.status = "Failed".to_owned();
        active.error = Some("process-tree termination could not be confirmed".to_owned());
        active.updated_at = 21;
        assert!(store.fail_stopping_download(active.clone()).await.unwrap());
        assert!(!store.fail_stopping_download(active).await.unwrap());
        let persisted = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(persisted.status, "Failed");
        assert_eq!(
            persisted.error.as_deref(),
            Some("process-tree termination could not be confirmed")
        );

        let mut pausing = DownloadRecord::queued(
            "pausing".to_owned(),
            "https://example.test/other-media".to_owned(),
            11,
        );
        pausing.status = "Downloading".to_owned();
        store.save_download(pausing.clone()).await.unwrap();
        assert!(store.request_active_pause("pausing", 22).await.unwrap());
        pausing.status = "Failed".to_owned();
        pausing.error = Some("process-tree pause could not be confirmed".to_owned());
        pausing.updated_at = 23;
        assert!(store.fail_stopping_download(pausing.clone()).await.unwrap());
        assert!(!store.fail_stopping_download(pausing).await.unwrap());
        let pausing = store
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.id == "pausing")
            .unwrap();
        assert_eq!(pausing.status, "Failed");
        assert_eq!(pausing.error.as_deref(), Some("process-tree pause could not be confirmed"));
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn cancel_wins_over_a_waiting_worker_claim() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        store.save_download(queued).await.unwrap();

        assert!(store.cancel_queued_download("queued", 20).await.unwrap());
        assert!(!store.cancel_queued_download("queued", 21).await.unwrap());
        assert!(store.claim_queued_download_if_next("queued", 21).await.unwrap().is_none());

        let canceled = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(canceled.status, "Canceled");
        assert_eq!(canceled.updated_at, 20);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn concurrent_cancel_and_worker_claim_have_one_winner() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        store.save_download(queued).await.unwrap();

        let (canceled, claimed) = tokio::join!(
            store.cancel_queued_download("queued", 20),
            store.claim_queued_download_if_next("queued", 21),
        );
        let canceled = canceled.unwrap();
        let claimed = claimed.unwrap();
        assert_ne!(canceled, claimed.is_some());

        let persisted = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(persisted.status, if canceled { "Canceled" } else { "Analyzing" });
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn concurrent_cancel_and_dispatcher_claim_have_one_winner() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        store.save_download(queued).await.unwrap();

        let (canceled, claimed) = tokio::join!(
            store.cancel_queued_download("queued", 20),
            store.claim_queued_download_if_next("queued", 21),
        );
        let canceled = canceled.unwrap();
        let claimed = claimed.unwrap();
        assert_ne!(canceled, claimed.is_some());

        let persisted = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(persisted.status, if canceled { "Canceled" } else { "Analyzing" });
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn concurrent_reorder_and_dispatcher_claim_use_one_queue_order() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        store
            .save_downloads(
                ["first", "second"]
                    .into_iter()
                    .map(|id| {
                        DownloadRecord::queued(
                            id.to_owned(),
                            format!("https://example.test/{id}"),
                            10,
                        )
                    })
                    .collect(),
            )
            .await
            .unwrap();

        let (moved, claimed) = tokio::join!(
            store.move_download("first", QueueMove::Bottom),
            store.claim_queued_download_if_next("first", 20),
        );
        let moved = moved.unwrap();
        let claimed = claimed.unwrap();
        assert_ne!(moved, claimed.is_some());

        let next = store.next_queued_download().await.unwrap().unwrap();
        assert_eq!(next.id, "second");
        let pending_ids = store
            .list_downloads()
            .await
            .unwrap()
            .into_iter()
            .filter(|record| record.status == "Queued")
            .map(|record| record.id)
            .collect::<Vec<_>>();
        if moved {
            assert_eq!(pending_ids, ["second".to_owned(), "first".to_owned()]);
        } else {
            assert_eq!(pending_ids, ["second".to_owned()]);
            let claimed = claimed.unwrap();
            assert_eq!(claimed.id, "first");
            assert_eq!(claimed.status, "Analyzing");
        }
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn worker_claim_wins_over_cancel_and_returns_the_claimed_record() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let queued = DownloadRecord::queued(
            "queued".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        store.save_download(queued).await.unwrap();

        let claimed = store.claim_queued_download_if_next("queued", 20).await.unwrap().unwrap();
        assert_eq!(claimed.status, "Analyzing");
        assert_eq!(claimed.updated_at, 20);
        assert!(!store.cancel_queued_download("queued", 21).await.unwrap());
        assert!(store.claim_queued_download_if_next("queued", 22).await.unwrap().is_none());

        let persisted = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(persisted.status, "Analyzing");
        assert_eq!(persisted.updated_at, 20);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn cancel_and_claim_are_noops_for_missing_or_nonqueued_records() {
        let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
        let mut completed = DownloadRecord::queued(
            "completed".to_owned(),
            "https://example.test/media".to_owned(),
            10,
        );
        completed.status = "Completed".to_owned();
        store.save_download(completed).await.unwrap();

        assert!(!store.cancel_queued_download("missing", 20).await.unwrap());
        assert!(store.claim_queued_download_if_next("missing", 20).await.unwrap().is_none());
        assert!(!store.cancel_queued_download("completed", 21).await.unwrap());
        assert!(store.claim_queued_download_if_next("completed", 21).await.unwrap().is_none());
        assert_eq!(store.list_downloads().await.unwrap()[0].status, "Completed");
        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}
