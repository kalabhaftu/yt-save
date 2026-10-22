use super::*;

impl Store {
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
                let speed_limit =
                    setting_value(connection, "new_download_speed_limit_bytes_per_second")?;
                Ok(DownloadPresets::from_database_values(
                    mode.as_deref(),
                    audio_format.as_deref(),
                    audio_quality.as_deref(),
                    video_quality.as_deref(),
                    speed_limit.as_deref(),
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

    /// Loads the persisted per-host queue concurrency limit; invalid or missing values use one.
    pub async fn load_max_concurrent_per_host(&self) -> Result<usize> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let parsed = setting_value(connection, "max_concurrent_per_host")?
                    .and_then(|value| value.parse::<usize>().ok())
                    .filter(|value| (1..=MAX_CONCURRENT_PER_HOST).contains(value));
                Ok(parsed.unwrap_or(DEFAULT_MAX_CONCURRENT_PER_HOST))
            })
        })
        .await
        .context("join per-host concurrency setting read")?
    }

    /// Saves the bounded per-host queue concurrency limit for subsequent dispatches.
    pub async fn set_max_concurrent_per_host(&self, limit: usize, updated_at: i64) -> Result<()> {
        let limit = limit.clamp(1, MAX_CONCURRENT_PER_HOST).to_string();
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "INSERT INTO app_settings (key, value, updated_at)
                     VALUES ('max_concurrent_per_host', :value, :updated)
                     ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value, updated_at = excluded.updated_at",
                    named_params! { ":value": limit, ":updated": updated_at },
                )?;
                Ok(())
            })
        })
        .await
        .context("join per-host concurrency setting write")??;
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

    /// Promotes scheduled jobs whose one-time run time has arrived.
    /// Paused jobs are left alone so their schedule remains available on resume.
    pub async fn promote_due_scheduled_downloads(&self, timestamp: i64) -> Result<usize> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let promoted = connection.execute(
                    "UPDATE downloads
                     SET status = 'Queued', scheduled_at = NULL, updated_at = :now
                     WHERE status = 'Waiting for schedule'
                       AND scheduled_at IS NOT NULL
                       AND scheduled_at <= :now",
                    named_params! { ":now": timestamp },
                )?;
                Ok(promoted)
            })
        })
        .await
        .context("join due scheduled download update")?
    }

    /// Returns the next active scheduled-job deadline for dispatcher timer scheduling.
    pub async fn next_scheduled_at(&self) -> Result<Option<i64>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT min(scheduled_at) FROM downloads
                         WHERE status = 'Waiting for schedule' AND scheduled_at IS NOT NULL",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(Into::into)
            })
        })
        .await
        .context("join next scheduled download read")?
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
    pub async fn load_app_update_channel(&self) -> Result<AppUpdateChannel> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                Ok(AppUpdateChannel::from_database_value(
                    setting_value(connection, "app_update_channel")?.as_deref(),
                ))
            })
        })
        .await
        .context("join application update channel read")?
    }

    pub async fn set_app_update_channel(
        &self,
        channel: AppUpdateChannel,
        updated_at: i64,
    ) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "INSERT INTO app_settings (key, value, updated_at)
                     VALUES ('app_update_channel', :value, :updated)
                     ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value, updated_at = excluded.updated_at",
                    named_params! {
                        ":value": channel.database_value(),
                        ":updated": updated_at,
                    },
                )?;
                Ok(())
            })
        })
        .await
        .context("join application update channel write")??;
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

    /// Updates New download format defaults atomically; the speed cap is persisted independently.
    pub async fn set_download_presets(
        &self,
        presets: DownloadPresets,
        updated_at: i64,
    ) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let [(_, mode), (_, audio_format), (_, audio_quality), (_, video_quality), _] =
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

    /// Saves the selected download-rate cap for future queue items.
    pub async fn set_download_speed_limit(
        &self,
        bytes_per_second: u64,
        updated_at: i64,
    ) -> Result<()> {
        let bytes_per_second = normalize_download_speed_limit(bytes_per_second).to_string();
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "INSERT INTO app_settings (key, value, updated_at)
                     VALUES ('new_download_speed_limit_bytes_per_second', :value, :updated)
                     ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value, updated_at = excluded.updated_at",
                    named_params! { ":value": bytes_per_second, ":updated": updated_at },
                )?;
                Ok(())
            })
        })
        .await
        .context("join download speed-limit setting write")??;
        Ok(())
    }
}
