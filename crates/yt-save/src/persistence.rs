use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, Row, named_params, types::Type};

use crate::model::{AudioFormat, AudioQuality, DownloadMode, DownloadRecord, VideoQuality};

#[derive(Clone)]
pub struct Store {
    path: PathBuf,
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
                    download_mode TEXT NOT NULL DEFAULT 'video',
                    audio_format TEXT NOT NULL DEFAULT 'original',
                    audio_quality TEXT NOT NULL DEFAULT 'best',
                    video_quality TEXT NOT NULL DEFAULT 'best'
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

    pub async fn save_download(&self, record: DownloadRecord) -> Result<()> {
        let downloaded_bytes = sqlite_integer(record.downloaded_bytes, "downloaded_bytes")?;
        let total_bytes = sqlite_integer(record.total_bytes, "total_bytes")?;
        let eta_seconds = sqlite_integer(record.eta_seconds, "eta_seconds")?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "INSERT INTO downloads (
                        id, url, title, source, status, progress, downloaded_bytes,
                        total_bytes, speed_bytes_per_second, eta_seconds, output_path,
                        error, created_at, updated_at, playlist_index,
                        download_mode, audio_format, audio_quality, video_quality
                    ) VALUES (
                        :id, :url, :title, :source, :status, :progress, :downloaded,
                        :total, :speed, :eta, :output, :error, :created, :updated,
                        :playlist_index, :download_mode, :audio_format, :audio_quality,
                        :video_quality
                    ) ON CONFLICT(id) DO UPDATE SET
                        url=excluded.url, title=excluded.title, source=excluded.source,
                        status=excluded.status, progress=excluded.progress,
                        downloaded_bytes=excluded.downloaded_bytes,
                        total_bytes=excluded.total_bytes,
                        speed_bytes_per_second=excluded.speed_bytes_per_second,
                        eta_seconds=excluded.eta_seconds, output_path=excluded.output_path,
                        error=excluded.error, updated_at=excluded.updated_at,
                        playlist_index=excluded.playlist_index,
                        download_mode=excluded.download_mode,
                        audio_format=excluded.audio_format,
                        audio_quality=excluded.audio_quality,
                        video_quality=excluded.video_quality",
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
                        ":download_mode": record.download_mode.as_database_value(),
                        ":audio_format": record.audio_format.as_database_value(),
                        ":audio_quality": record.audio_quality.as_database_value(),
                        ":video_quality": record.video_quality.as_database_value(),
                    },
                )?;
                Ok(())
            })
        })
        .await
        .context("join database write")??;
        Ok(())
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
                     SET status = 'Paused', error = NULL, updated_at = :updated
                     WHERE id = :id AND status = 'Queued'",
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
                     SET status = 'Canceled', error = NULL, updated_at = :updated
                     WHERE id = :id AND status IN ('Queued', 'Paused')",
                    named_params! { ":id": id, ":updated": updated_at },
                )?;
                Ok(affected > 0)
            })
        })
        .await
        .context("join database write")?
    }

    /// Claims a waiting job for its worker. Cancellation and claiming race through
    /// conditional updates, so only one transition out of `Queued` can succeed.
    pub async fn claim_queued_download(
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
                     SET status = 'Analyzing', error = NULL, updated_at = :updated
                     WHERE id = :id AND status = 'Queued'",
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
                     SET status = 'Queued', error = NULL, updated_at = :updated
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
                     SET status = 'Queued', error = NULL, updated_at = :updated
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
}

fn load_download_by_id(connection: &Connection, id: &str) -> Result<Option<DownloadRecord>> {
    let mut statement = connection.prepare(
        "SELECT id, url, title, source, status, progress, downloaded_bytes,
                total_bytes, speed_bytes_per_second, eta_seconds, output_path,
                error, created_at, updated_at, playlist_index,
                download_mode, audio_format, audio_quality, video_quality, rowid
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
                download_mode: download_mode_from_row(row, 15)?,
                audio_format: audio_format_from_row(row, 16)?,
                audio_quality: audio_quality_from_row(row, 17)?,
                video_quality: video_quality_from_row(row, 18)?,
                queue_order: row.get(19)?,
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
                download_mode, audio_format, audio_quality, video_quality, rowid
         FROM downloads ORDER BY created_at DESC, rowid DESC",
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
            download_mode: download_mode_from_row(row, 15)?,
            audio_format: audio_format_from_row(row, 16)?,
            audio_quality: audio_quality_from_row(row, 17)?,
            video_quality: video_quality_from_row(row, 18)?,
            queue_order: row.get(19)?,
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
                        'Completed', 100, 10, 20
                    );",
                )
                .unwrap();
        }

        let store = Store::open(db_path.clone()).unwrap();
        let legacy = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(legacy.id, "legacy-job");
        assert_eq!(legacy.playlist_index, None);
        assert_eq!(legacy.download_mode, DownloadMode::Video);
        assert_eq!(legacy.audio_format, AudioFormat::Original);
        assert_eq!(legacy.audio_quality, AudioQuality::Best);
        assert_eq!(legacy.video_quality, VideoQuality::Best);

        let mut selected = DownloadRecord::queued(
            "selected-job".to_owned(),
            "https://example.test/list".to_owned(),
            30,
        );
        selected.playlist_index = Some(7);
        selected.download_mode = DownloadMode::AudioOnly;
        selected.audio_format = AudioFormat::M4a;
        selected.audio_quality = AudioQuality::K192;
        selected.video_quality = VideoQuality::P720;
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
        assert_eq!(selected.download_mode, DownloadMode::AudioOnly);
        assert_eq!(selected.audio_format, AudioFormat::M4a);
        assert_eq!(selected.audio_quality, AudioQuality::K192);
        assert_eq!(selected.video_quality, VideoQuality::P720);
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
        queued.progress = 48.0;
        queued.downloaded_bytes = Some(480);
        queued.total_bytes = Some(1_000);
        queued.playlist_index = Some(4);
        store.save_download(queued).await.unwrap();

        assert!(store.pause_queued_download("queued", 20).await.unwrap());
        assert!(!store.pause_queued_download("queued", 21).await.unwrap());
        assert!(store.claim_queued_download("queued", 21).await.unwrap().is_none());

        let paused = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(paused.status, "Paused");
        assert_eq!(paused.progress, 48.0);
        assert_eq!(paused.downloaded_bytes, Some(480));
        assert_eq!(paused.audio_format, crate::model::AudioFormat::Mp3);
        assert_eq!(paused.audio_quality, crate::model::AudioQuality::K192);
        assert_eq!(paused.video_quality, crate::model::VideoQuality::P480);
        assert_eq!(paused.playlist_index, Some(4));

        let resumed = store.resume_download("queued", 22).await.unwrap().unwrap();
        assert_eq!(resumed.status, "Queued");
        assert_eq!(resumed.progress, 48.0);
        assert_eq!(resumed.downloaded_bytes, Some(480));
        assert_eq!(resumed.download_mode, crate::model::DownloadMode::AudioOnly);
        assert_eq!(resumed.audio_format, crate::model::AudioFormat::Mp3);
        assert_eq!(resumed.audio_quality, crate::model::AudioQuality::K192);
        assert_eq!(resumed.video_quality, crate::model::VideoQuality::P480);
        assert_eq!(resumed.playlist_index, Some(4));
        assert!(store.resume_download("queued", 23).await.unwrap().is_none());
        assert_eq!(
            store.claim_queued_download("queued", 24).await.unwrap().unwrap().status,
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
            store.claim_queued_download("queued", 21),
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
        assert!(store.claim_queued_download("queued", 21).await.unwrap().is_none());

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
            store.claim_queued_download("queued", 21),
        );
        let canceled = canceled.unwrap();
        let claimed = claimed.unwrap();
        assert_ne!(canceled, claimed.is_some());

        let persisted = store.list_downloads().await.unwrap().remove(0);
        assert_eq!(persisted.status, if canceled { "Canceled" } else { "Analyzing" });
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

        let claimed = store.claim_queued_download("queued", 20).await.unwrap().unwrap();
        assert_eq!(claimed.status, "Analyzing");
        assert_eq!(claimed.updated_at, 20);
        assert!(!store.cancel_queued_download("queued", 21).await.unwrap());
        assert!(store.claim_queued_download("queued", 22).await.unwrap().is_none());

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
        assert!(store.claim_queued_download("missing", 20).await.unwrap().is_none());
        assert!(!store.cancel_queued_download("completed", 21).await.unwrap());
        assert!(store.claim_queued_download("completed", 21).await.unwrap().is_none());
        assert_eq!(store.list_downloads().await.unwrap()[0].status, "Completed");
        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}
