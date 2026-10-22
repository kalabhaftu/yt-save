use super::*;

impl Store {
    pub async fn list_downloads(&self) -> Result<Vec<DownloadRecord>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.with_connection(load_downloads))
            .await
            .context("join database read")?
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

    #[cfg(test)]
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

    /// Returns the first queued item whose normalized host is below its active worker limit.
    ///
    /// Items that cannot produce an HTTP(S) host key share the empty-string bucket.
    pub async fn next_queued_download_with_host_limit(
        &self,
        active_host_counts: HashMap<String, usize>,
        per_host_limit: usize,
    ) -> Result<Option<DownloadRecord>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let id = first_eligible_queued_download_id(
                    connection,
                    &active_host_counts,
                    per_host_limit,
                )?;
                match id {
                    Some(id) => load_download_by_id(connection, &id),
                    None => Ok(None),
                }
            })
        })
        .await
        .context("join host-aware queue read")?
    }

    /// Claims only if `id` is still first, so a concurrent reorder is honored before launch.
    #[cfg(test)]
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
                transaction.execute(
                    "UPDATE downloads
                     SET status = 'Queued', scheduled_at = NULL, updated_at = :now
                     WHERE status = 'Waiting for schedule'
                       AND scheduled_at IS NOT NULL
                       AND scheduled_at <= :now",
                    named_params! { ":now": updated_at },
                )?;
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

    /// Claims only if `id` is still the first queued item eligible under the supplied host counts.
    ///
    /// Due scheduled rows are promoted and queue order, cancellation, and eligibility are checked
    /// in the same immediate transaction as the claim.
    pub async fn claim_queued_download_if_host_eligible(
        &self,
        id: &str,
        active_host_counts: HashMap<String, usize>,
        per_host_limit: usize,
        updated_at: i64,
    ) -> Result<Option<DownloadRecord>> {
        let store = self.clone();
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute(
                    "UPDATE downloads
                     SET status = 'Queued', scheduled_at = NULL, updated_at = :now
                     WHERE status = 'Waiting for schedule'
                       AND scheduled_at IS NOT NULL
                       AND scheduled_at <= :now",
                    named_params! { ":now": updated_at },
                )?;
                let next_id = first_eligible_queued_download_id(
                    &transaction,
                    &active_host_counts,
                    per_host_limit,
                )?;
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
        .context("join host-aware queue claim")?
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
                        "SELECT priority FROM downloads WHERE id = :id AND status IN ('Queued', 'Waiting for retry', 'Paused', 'Waiting for schedule')",
                        named_params! { ":id": id },
                        |row| row.get::<_, i32>(0),
                    )
                    .optional()?;
                let Some(priority) = selected else {
                    transaction.commit()?;
                    return Ok(false);
                };
                let mut statement = transaction.prepare(
                    "SELECT id FROM downloads WHERE status IN ('Queued', 'Waiting for retry', 'Paused', 'Waiting for schedule') AND priority = :priority
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
                            "UPDATE downloads SET queue_rank = :rank WHERE id = :id AND status IN ('Queued', 'Waiting for retry', 'Paused', 'Waiting for schedule')",
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
                     WHERE id = :id AND status IN ('Queued', 'Waiting for retry', 'Paused', 'Waiting for schedule')
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
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let status: Option<String> = transaction
                    .query_row(
                        "SELECT status FROM downloads WHERE id = :id",
                        named_params! { ":id": id },
                        |row| row.get(0),
                    )
                    .optional()?;
                let Some(status) = status else {
                    transaction.commit()?;
                    return Ok(false);
                };
                if !matches!(status.as_str(), "Completed" | "Failed" | "Canceled" | "Interrupted") {
                    transaction.commit()?;
                    return Ok(false);
                }
                if status == "Completed" {
                    transaction.execute(
                        "UPDATE playlist_subscription_entries
                         SET completed_at = COALESCE(completed_at, :updated_at),
                             state_updated_at = :updated_at
                         WHERE download_id = :id AND state = 'queued'",
                        named_params! { ":id": id, ":updated_at": chrono::Utc::now().timestamp() },
                    )?;
                } else {
                    transaction.execute(
                        "UPDATE playlist_subscription_entries
                         SET state = 'new', download_id = NULL, completed_at = NULL,
                             state_updated_at = :updated_at
                         WHERE download_id = :id AND state = 'queued'",
                        named_params! { ":id": id, ":updated_at": chrono::Utc::now().timestamp() },
                    )?;
                }
                let affected = transaction
                    .execute("DELETE FROM downloads WHERE id = :id", named_params! { ":id": id })?;
                transaction.commit()?;
                Ok(affected == 1)
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
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let cleared_at = chrono::Utc::now().timestamp();
                transaction.execute(
                    "UPDATE playlist_subscription_entries
                     SET completed_at = COALESCE(completed_at, :cleared_at),
                         state_updated_at = :cleared_at
                     WHERE state = 'queued'
                       AND download_id IN (SELECT id FROM downloads WHERE status = 'Completed')",
                    named_params! { ":cleared_at": cleared_at },
                )?;
                let affected =
                    transaction.execute("DELETE FROM downloads WHERE status = 'Completed'", [])?;
                transaction.commit()?;
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
                     WHERE id = :id AND status IN ('Queued', 'Waiting for retry', 'Waiting for schedule')",
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
                     SET status = 'Canceled', next_retry_at = NULL, scheduled_at = NULL,
                         error = NULL, updated_at = :updated
                     WHERE id = :id AND status IN ('Queued', 'Waiting for retry', 'Paused', 'Waiting for schedule')",
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
        let completed = record.status == "Completed";
        let completed_id = record.id.clone();
        let completed_at = record.updated_at;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let affected = transaction.execute(
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
                if affected == 1 && completed {
                    transaction.execute(
                        "UPDATE playlist_subscription_entries
                         SET completed_at = :completed_at, state_updated_at = :completed_at
                         WHERE download_id = :id AND state = 'queued'",
                        named_params! { ":id": completed_id, ":completed_at": completed_at },
                    )?;
                }
                transaction.commit()?;
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
                         next_retry_at = NULL, scheduled_at = NULL, updated_at = :updated,
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
                     SET status = CASE
                             WHEN scheduled_at IS NOT NULL AND scheduled_at > :updated
                                 THEN 'Waiting for schedule'
                             ELSE 'Queued'
                         END,
                         scheduled_at = CASE
                             WHEN scheduled_at IS NOT NULL AND scheduled_at > :updated
                                 THEN scheduled_at
                             ELSE NULL
                         END,
                         error = NULL, next_retry_at = NULL, updated_at = :updated
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
}
