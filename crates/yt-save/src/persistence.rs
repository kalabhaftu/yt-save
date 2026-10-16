use std::path::PathBuf;

use anyhow::{Context, Result};
use rusqlite::{Connection, Row, named_params, types::Type};

use crate::model::DownloadRecord;

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
                    updated_at INTEGER NOT NULL
                 );
                 CREATE INDEX IF NOT EXISTS idx_downloads_updated_at
                    ON downloads(updated_at DESC);
                 CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY NOT NULL,
                    value TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                 );",
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
                        error, created_at, updated_at
                    ) VALUES (
                        :id, :url, :title, :source, :status, :progress, :downloaded,
                        :total, :speed, :eta, :output, :error, :created, :updated
                    ) ON CONFLICT(id) DO UPDATE SET
                        url=excluded.url, title=excluded.title, source=excluded.source,
                        status=excluded.status, progress=excluded.progress,
                        downloaded_bytes=excluded.downloaded_bytes,
                        total_bytes=excluded.total_bytes,
                        speed_bytes_per_second=excluded.speed_bytes_per_second,
                        eta_seconds=excluded.eta_seconds, output_path=excluded.output_path,
                        error=excluded.error, updated_at=excluded.updated_at",
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

    fn with_connection<T>(&self, operation: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let connection = Connection::open(&self.path)
            .with_context(|| format!("open application database {}", self.path.display()))?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        operation(&connection)
    }
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
                error, created_at, updated_at
         FROM downloads ORDER BY created_at DESC",
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
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
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
}
