use super::*;

impl Store {
    /// Adds a playlist to the check list with manual checks enabled by default.
    /// Both the id and URL must be unique.
    pub async fn create_playlist_subscription(
        &self,
        id: String,
        url: String,
        title: String,
        source: String,
        created_at: i64,
    ) -> Result<()> {
        anyhow::ensure!(!id.trim().is_empty(), "playlist subscription id is empty");
        anyhow::ensure!(!url.trim().is_empty(), "playlist subscription URL is empty");
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                connection.execute(
                    "INSERT INTO playlist_subscriptions
                        (id, url, title, source, created_at, check_interval_seconds,
                         next_check_at, auto_queue_new_entries)
                     VALUES (:id, :url, :title, :source, :created_at, 0, NULL, 0)",
                    named_params! {
                        ":id": id,
                        ":url": url,
                        ":title": title,
                        ":source": source,
                        ":created_at": created_at,
                    },
                )?;
                Ok(())
            })
        })
        .await
        .context("join playlist subscription write")??;
        Ok(())
    }

    /// Lists subscriptions with a count of entries still marked `new`.
    pub async fn list_playlist_subscriptions(&self) -> Result<Vec<PlaylistSubscription>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT s.id, s.url, s.title, s.source, s.created_at,
                            s.check_interval_seconds, s.next_check_at,
                            s.auto_queue_new_entries,
                            s.last_checked_at, s.last_check_error,
                            (SELECT count(*) FROM playlist_subscription_entries e
                             WHERE e.subscription_id = s.id AND e.state = 'new')
                     FROM playlist_subscriptions s
                     ORDER BY s.created_at DESC, s.id ASC",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok(PlaylistSubscription {
                        id: row.get(0)?,
                        url: row.get(1)?,
                        title: row.get(2)?,
                        source: row.get(3)?,
                        created_at: row.get(4)?,
                        check_interval_seconds: row.get(5)?,
                        next_check_at: row.get(6)?,
                        auto_queue_new_entries: row.get(7)?,
                        last_checked_at: row.get(8)?,
                        last_check_error: row.get(9)?,
                        pending_count: row.get::<_, i64>(10)? as usize,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
            })
        })
        .await
        .context("join playlist subscriptions read")?
    }

    /// Changes a playlist's interval and schedules its next check from `changed_at`.
    /// Interval zero disables scheduled checks and clears the next-check timestamp.
    pub async fn set_playlist_subscription_check_interval(
        &self,
        id: String,
        interval_seconds: i64,
        changed_at: i64,
    ) -> Result<()> {
        let next_check_at = playlist_next_check_at(interval_seconds, changed_at)?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let updated = connection.execute(
                    "UPDATE playlist_subscriptions
                     SET check_interval_seconds = :interval,
                         next_check_at = CASE
                             WHEN check_interval_seconds = :interval THEN next_check_at
                             ELSE :next_check_at
                         END
                     WHERE id = :id",
                    named_params! {
                        ":id": id,
                        ":interval": interval_seconds,
                        ":next_check_at": next_check_at,
                    },
                )?;
                anyhow::ensure!(updated == 1, "playlist subscription {id} does not exist");
                Ok(())
            })
        })
        .await
        .context("join playlist subscription schedule update")??;
        Ok(())
    }

    /// Enables or disables automatic queueing of newly discovered available entries.
    pub async fn set_playlist_subscription_auto_queue_new_entries(
        &self,
        id: String,
        enabled: bool,
    ) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let updated = connection.execute(
                    "UPDATE playlist_subscriptions
                     SET auto_queue_new_entries = :enabled
                     WHERE id = :id",
                    named_params! { ":id": id, ":enabled": enabled },
                )?;
                anyhow::ensure!(updated == 1, "playlist subscription {id} does not exist");
                Ok(())
            })
        })
        .await
        .context("join playlist subscription auto-queue update")??;
        Ok(())
    }

    /// Returns at most the requested number of due subscriptions in due-time order.
    /// The batch is capped so callers cannot accidentally load an unbounded schedule.
    pub async fn list_due_playlist_subscriptions(
        &self,
        now: i64,
        limit: usize,
    ) -> Result<Vec<PlaylistSubscription>> {
        let limit = limit.min(MAX_DUE_PLAYLIST_SUBSCRIPTIONS_PER_BATCH);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT s.id, s.url, s.title, s.source, s.created_at,
                            s.check_interval_seconds, s.next_check_at,
                            s.auto_queue_new_entries,
                            s.last_checked_at, s.last_check_error,
                            (SELECT count(*) FROM playlist_subscription_entries e
                             WHERE e.subscription_id = s.id AND e.state = 'new')
                     FROM playlist_subscriptions s
                     WHERE s.check_interval_seconds > 0 AND s.next_check_at <= :now
                     ORDER BY s.next_check_at ASC, s.id ASC
                     LIMIT :limit",
                )?;
                let rows = statement.query_map(
                    named_params! { ":now": now, ":limit": limit as i64 },
                    |row| {
                        Ok(PlaylistSubscription {
                            id: row.get(0)?,
                            url: row.get(1)?,
                            title: row.get(2)?,
                            source: row.get(3)?,
                            created_at: row.get(4)?,
                            check_interval_seconds: row.get(5)?,
                            next_check_at: row.get(6)?,
                            auto_queue_new_entries: row.get(7)?,
                            last_checked_at: row.get(8)?,
                            last_check_error: row.get(9)?,
                            pending_count: row.get::<_, i64>(10)? as usize,
                        })
                    },
                )?;
                rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
            })
        })
        .await
        .context("join due playlist subscriptions read")?
    }

    /// Deletes a subscription and its archive only; linked download rows and files are retained.
    pub async fn delete_playlist_subscription(&self, id: String) -> Result<bool> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute(
                    "DELETE FROM playlist_subscription_entries WHERE subscription_id = :id",
                    named_params! { ":id": id },
                )?;
                let deleted = transaction.execute(
                    "DELETE FROM playlist_subscriptions WHERE id = :id",
                    named_params! { ":id": id },
                )?;
                transaction.commit()?;
                Ok(deleted > 0)
            })
        })
        .await
        .context("join playlist subscription delete")?
    }

    /// Stores a successful playlist snapshot and returns the number of previously unseen keys.
    /// Existing archive state and first-seen timestamps are preserved across checks. This
    /// review-first API never auto-queues; use `record_playlist_subscription_check_with_queue_items`
    /// to apply a subscription's saved auto-queue preference.
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn record_playlist_subscription_check(
        &self,
        id: String,
        title: String,
        source: String,
        checked_at: i64,
        entries: Vec<PlaylistSubscriptionEntryInput>,
    ) -> Result<usize> {
        self.record_playlist_subscription_check_internal(
            PlaylistSubscriptionSnapshot { id, title, source, checked_at, entries },
            Vec::new(),
            false,
        )
        .await
        .map(|(new_archive_entries, _)| new_archive_entries)
    }

    /// Stores a successful playlist snapshot and, when the saved subscription setting is
    /// enabled, queues each newly discovered available entry in the same transaction.
    /// Every newly discovered available entry then needs one matching queued record.
    /// Existing archive state and first-seen timestamps are preserved across checks.
    pub async fn record_playlist_subscription_check_with_queue_items(
        &self,
        id: String,
        title: String,
        source: String,
        checked_at: i64,
        entries: Vec<PlaylistSubscriptionEntryInput>,
        queue_items: Vec<PlaylistSubscriptionQueueItem>,
    ) -> Result<(usize, usize)> {
        self.record_playlist_subscription_check_internal(
            PlaylistSubscriptionSnapshot { id, title, source, checked_at, entries },
            queue_items,
            true,
        )
        .await
    }

    async fn record_playlist_subscription_check_internal(
        &self,
        snapshot: PlaylistSubscriptionSnapshot,
        queue_items: Vec<PlaylistSubscriptionQueueItem>,
        use_saved_auto_queue_setting: bool,
    ) -> Result<(usize, usize)> {
        let PlaylistSubscriptionSnapshot { id, title, source, checked_at, entries } = snapshot;
        validate_subscription_snapshot(&entries)?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let (interval_seconds, subscription_auto_queue_new_entries) = transaction
                    .query_row(
                        "SELECT check_interval_seconds, auto_queue_new_entries
                         FROM playlist_subscriptions WHERE id = :id",
                        named_params! { ":id": id },
                        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, bool>(1)?)),
                    )
                    .optional()?
                    .with_context(|| format!("playlist subscription {id} does not exist"))?;
                let auto_queue_new_entries =
                    use_saved_auto_queue_setting && subscription_auto_queue_new_entries;

                if auto_queue_new_entries {
                    validate_queue_items(&queue_items)?;
                }
                let queue_items_by_key: HashMap<&str, &PlaylistSubscriptionQueueItem> =
                    queue_items.iter().map(|item| (item.media_key.as_str(), item)).collect();

                let next_check_at = playlist_next_check_at(interval_seconds, checked_at)?;
                let updated = transaction.execute(
                    "UPDATE playlist_subscriptions
                     SET title = :title, source = :source,
                         last_checked_at = :checked_at, last_check_error = NULL,
                         next_check_at = :next_check_at
                     WHERE id = :id",
                    named_params! {
                        ":id": id,
                        ":title": title,
                        ":source": source,
                        ":checked_at": checked_at,
                        ":next_check_at": next_check_at,
                    },
                )?;
                anyhow::ensure!(updated == 1, "playlist subscription {id} does not exist");

                // Entries omitted from this complete snapshot are no longer queueable, but stay
                // in the archive with their previous state and last-seen time intact.
                transaction.execute(
                    "UPDATE playlist_subscription_entries
                     SET available = 0 WHERE subscription_id = :id",
                    named_params! { ":id": id },
                )?;

                let mut inserted = 0usize;
                let mut auto_queued = 0usize;
                for entry in &entries {
                    let already_seen: bool = transaction.query_row(
                        "SELECT EXISTS(SELECT 1 FROM playlist_subscription_entries
                         WHERE subscription_id = :subscription_id AND media_key = :media_key)",
                        named_params! {
                            ":subscription_id": id,
                            ":media_key": entry.media_key,
                        },
                        |row| row.get(0),
                    )?;
                    transaction.execute(
                        "INSERT INTO playlist_subscription_entries (
                            subscription_id, media_key, media_id, media_url, title,
                            playlist_index, available, first_seen_at, last_seen_at,
                            state_updated_at, state
                         ) VALUES (
                            :subscription_id, :media_key, :media_id, :media_url, :title,
                            :playlist_index, :available, :checked_at, :checked_at,
                            :checked_at, 'new'
                         ) ON CONFLICT(subscription_id, media_key) DO UPDATE SET
                            media_id = excluded.media_id,
                            media_url = excluded.media_url,
                            title = excluded.title,
                            playlist_index = excluded.playlist_index,
                            available = excluded.available,
                            last_seen_at = excluded.last_seen_at",
                        named_params! {
                            ":subscription_id": id,
                            ":media_key": entry.media_key,
                            ":media_id": entry.media_id,
                            ":media_url": entry.media_url,
                            ":title": entry.title,
                            ":playlist_index": entry.playlist_index,
                            ":available": entry.available,
                            ":checked_at": checked_at,
                        },
                    )?;
                    if !already_seen {
                        inserted += 1;
                        if auto_queue_new_entries && entry.available {
                            let item = queue_items_by_key
                                .get(entry.media_key.as_str())
                                .with_context(|| {
                                    format!(
                                        "new available playlist entry {} has no queue record",
                                        entry.media_key
                                    )
                                })?;
                            anyhow::ensure!(
                                !entry.media_url.trim().is_empty(),
                                "new available playlist entry {} has no usable media URL",
                                entry.media_key
                            );
                            anyhow::ensure!(
                                item.record.url == entry.media_url,
                                "download URL does not match playlist entry {}",
                                entry.media_key
                            );
                            let id_exists: bool = transaction.query_row(
                                "SELECT EXISTS(SELECT 1 FROM downloads WHERE id = :id)",
                                named_params! { ":id": item.record.id },
                                |row| row.get(0),
                            )?;
                            anyhow::ensure!(
                                !id_exists,
                                "download id {} already exists",
                                item.record.id
                            );
                            save_download_record(&transaction, &item.record)?;
                            let changed = transaction.execute(
                                "UPDATE playlist_subscription_entries
                                 SET state = 'queued', download_id = :download_id,
                                     state_updated_at = :updated_at
                                 WHERE subscription_id = :subscription_id
                                   AND media_key = :media_key
                                   AND state = 'new' AND available = 1",
                                named_params! {
                                    ":subscription_id": id,
                                    ":media_key": entry.media_key,
                                    ":download_id": item.record.id,
                                    ":updated_at": item.record.updated_at,
                                },
                            )?;
                            anyhow::ensure!(
                                changed == 1,
                                "playlist entry {} changed while auto-queueing",
                                entry.media_key
                            );
                            auto_queued += 1;
                        }
                    }
                }
                transaction.commit()?;
                Ok((inserted, auto_queued))
            })
        })
        .await
        .context("join playlist subscription check write")?
    }

    /// Records a failed check attempt without changing the last successful archive snapshot.
    pub async fn record_playlist_subscription_error(
        &self,
        id: String,
        error: String,
        checked_at: i64,
    ) -> Result<()> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let interval_seconds = transaction
                    .query_row(
                        "SELECT check_interval_seconds FROM playlist_subscriptions WHERE id = :id",
                        named_params! { ":id": id },
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?
                    .with_context(|| format!("playlist subscription {id} does not exist"))?;
                let next_check_at = playlist_next_check_at(interval_seconds, checked_at)?;
                let updated = transaction.execute(
                    "UPDATE playlist_subscriptions
                     SET last_checked_at = :checked_at, last_check_error = :error,
                         next_check_at = :next_check_at
                     WHERE id = :id",
                    named_params! {
                        ":id": id,
                        ":error": error,
                        ":checked_at": checked_at,
                        ":next_check_at": next_check_at,
                    },
                )?;
                anyhow::ensure!(updated == 1, "playlist subscription {id} does not exist");
                transaction.commit()?;
                Ok(())
            })
        })
        .await
        .context("join playlist subscription error write")??;
        Ok(())
    }

    /// Returns the full archive in the latest playlist order, with unindexed entries last.
    pub async fn list_playlist_subscription_entries(
        &self,
        id: String,
    ) -> Result<Vec<PlaylistSubscriptionEntry>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT e.media_key, e.media_id, e.media_url, e.title, e.playlist_index,
                            e.available, e.first_seen_at, e.last_seen_at,
                            CASE WHEN e.completed_at IS NOT NULL OR d.status = 'Completed'
                                 THEN 'downloaded' ELSE e.state END,
                            e.download_id, e.completed_at
                     FROM playlist_subscription_entries e
                     LEFT JOIN downloads d ON d.id = e.download_id
                     WHERE e.subscription_id = :id
                     ORDER BY e.playlist_index IS NULL, e.playlist_index ASC, e.media_key ASC",
                )?;
                let rows = statement.query_map(named_params! { ":id": id }, |row| {
                    Ok(PlaylistSubscriptionEntry {
                        media_key: row.get(0)?,
                        media_id: row.get(1)?,
                        media_url: row.get(2)?,
                        title: row.get(3)?,
                        playlist_index: row.get(4)?,
                        available: row.get(5)?,
                        first_seen_at: row.get(6)?,
                        last_seen_at: row.get(7)?,
                        state: row.get(8)?,
                        download_id: row.get(9)?,
                        completed_at: row.get(10)?,
                    })
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
            })
        })
        .await
        .context("join playlist subscription archive read")?
    }

    /// Dismisses only entries still marked `new`; queued and ignored entries are unchanged.
    pub async fn dismiss_playlist_subscription_entries(
        &self,
        id: String,
        media_keys: Vec<String>,
        updated_at: i64,
    ) -> Result<usize> {
        validate_media_keys(&media_keys)?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let exists: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM playlist_subscriptions WHERE id = :id)",
                    named_params! { ":id": id },
                    |row| row.get(0),
                )?;
                anyhow::ensure!(exists, "playlist subscription {id} does not exist");
                let mut dismissed = 0usize;
                for media_key in &media_keys {
                    dismissed += transaction.execute(
                        "UPDATE playlist_subscription_entries
                         SET state = 'ignored', state_updated_at = :updated_at
                         WHERE subscription_id = :subscription_id
                           AND media_key = :media_key AND state = 'new'",
                        named_params! {
                            ":subscription_id": id,
                            ":media_key": media_key,
                            ":updated_at": updated_at,
                        },
                    )?;
                }
                transaction.commit()?;
                Ok(dismissed)
            })
        })
        .await
        .context("join playlist subscription dismiss")?
    }

    /// Atomically creates download rows and links selected available `new` archive entries.
    /// Reusing a media key or an existing download id is rejected before any row is saved.
    pub async fn queue_playlist_subscription_entries(
        &self,
        id: String,
        items: Vec<PlaylistSubscriptionQueueItem>,
    ) -> Result<usize> {
        validate_queue_items(&items)?;
        if items.is_empty() {
            return Ok(0);
        }
        let store = self.clone();
        tokio::task::spawn_blocking(move || {
            store.with_connection_mut(|connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let subscription_exists: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM playlist_subscriptions WHERE id = :id)",
                    named_params! { ":id": id },
                    |row| row.get(0),
                )?;
                anyhow::ensure!(subscription_exists, "playlist subscription {id} does not exist");

                // Validate the complete selection first. A stale/unavailable key must not leave
                // earlier records partially inserted into the durable queue.
                for item in &items {
                    let archived: Option<(String, bool, String)> = transaction
                        .query_row(
                            "SELECT media_url, available, state
                             FROM playlist_subscription_entries
                             WHERE subscription_id = :subscription_id AND media_key = :media_key",
                            named_params! {
                                ":subscription_id": id,
                                ":media_key": item.media_key,
                            },
                            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                        )
                        .optional()?;
                    let Some((media_url, available, state)) = archived else {
                        anyhow::bail!("playlist entry {} is not in the archive", item.media_key);
                    };
                    anyhow::ensure!(available, "playlist entry {} is unavailable", item.media_key);
                    anyhow::ensure!(
                        state == "new",
                        "playlist entry {} was already handled",
                        item.media_key
                    );
                    anyhow::ensure!(
                        !media_url.trim().is_empty(),
                        "playlist entry {} has no usable media URL",
                        item.media_key
                    );
                    anyhow::ensure!(
                        item.record.url == media_url,
                        "download URL does not match archived media URL for {}",
                        item.media_key
                    );
                    let id_exists: bool = transaction.query_row(
                        "SELECT EXISTS(SELECT 1 FROM downloads WHERE id = :id)",
                        named_params! { ":id": item.record.id },
                        |row| row.get(0),
                    )?;
                    anyhow::ensure!(!id_exists, "download id {} already exists", item.record.id);
                }

                for item in &items {
                    save_download_record(&transaction, &item.record)?;
                    let changed = transaction.execute(
                        "UPDATE playlist_subscription_entries
                         SET state = 'queued', download_id = :download_id,
                             state_updated_at = :updated_at
                         WHERE subscription_id = :subscription_id AND media_key = :media_key
                           AND state = 'new' AND available = 1",
                        named_params! {
                            ":subscription_id": id,
                            ":media_key": item.media_key,
                            ":download_id": item.record.id,
                            ":updated_at": item.record.updated_at,
                        },
                    )?;
                    anyhow::ensure!(
                        changed == 1,
                        "playlist entry {} changed while queuing",
                        item.media_key
                    );
                }
                transaction.commit()?;
                Ok(items.len())
            })
        })
        .await
        .context("join playlist subscription queue write")?
    }
}
