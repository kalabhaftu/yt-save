use super::super::*;

fn subscription_entry(
    media_key: &str,
    media_id: Option<&str>,
    media_url: &str,
    title: &str,
    playlist_index: Option<i64>,
    available: bool,
) -> PlaylistSubscriptionEntryInput {
    PlaylistSubscriptionEntryInput {
        media_key: media_key.to_owned(),
        media_id: media_id.map(str::to_owned),
        media_url: media_url.to_owned(),
        title: title.to_owned(),
        playlist_index,
        available,
    }
}

fn subscription_download(id: &str, url: &str, created_at: i64) -> DownloadRecord {
    DownloadRecord::queued(id.to_owned(), url.to_owned(), created_at)
}

async fn new_subscription_store() -> (PathBuf, Store) {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    store
        .create_playlist_subscription(
            "sub-1".to_owned(),
            "https://example.test/playlist".to_owned(),
            "Initial title".to_owned(),
            "Example".to_owned(),
            10,
        )
        .await
        .unwrap();
    (temp_dir, store)
}

async fn add_playlist_subscription(store: &Store, id: &str, created_at: i64) {
    store
        .create_playlist_subscription(
            id.to_owned(),
            format!("https://example.test/{id}"),
            format!("Playlist {id}"),
            "Example".to_owned(),
            created_at,
        )
        .await
        .unwrap();
}

#[test]
fn playlist_check_intervals_are_limited_to_supported_choices() {
    assert_eq!(
        PLAYLIST_CHECK_INTERVALS_SECONDS,
        &[0, 60 * 60, 6 * 60 * 60, 12 * 60 * 60, 24 * 60 * 60, 7 * 24 * 60 * 60,]
    );
    assert!(
        PLAYLIST_CHECK_INTERVALS_SECONDS
            .iter()
            .all(|interval| is_supported_playlist_check_interval_seconds(*interval))
    );
    assert!(!is_supported_playlist_check_interval_seconds(-1));
    assert!(!is_supported_playlist_check_interval_seconds(60));
}

#[tokio::test]
async fn subscription_interval_changes_schedule_or_clear_the_next_check() {
    let (temp_dir, store) = new_subscription_store().await;
    let created = store.list_playlist_subscriptions().await.unwrap().remove(0);
    assert_eq!(created.check_interval_seconds, PLAYLIST_CHECK_INTERVAL_MANUAL);
    assert_eq!(created.next_check_at, None);
    assert!(!created.auto_queue_new_entries);

    store
        .set_playlist_subscription_check_interval(
            "sub-1".to_owned(),
            PLAYLIST_CHECK_INTERVAL_1_HOUR,
            100,
        )
        .await
        .unwrap();
    let scheduled = store.list_playlist_subscriptions().await.unwrap().remove(0);
    assert_eq!(scheduled.check_interval_seconds, PLAYLIST_CHECK_INTERVAL_1_HOUR);
    assert_eq!(scheduled.next_check_at, Some(100 + PLAYLIST_CHECK_INTERVAL_1_HOUR));

    store
        .set_playlist_subscription_check_interval(
            "sub-1".to_owned(),
            PLAYLIST_CHECK_INTERVAL_1_HOUR,
            500,
        )
        .await
        .unwrap();
    let unchanged = store.list_playlist_subscriptions().await.unwrap().remove(0);
    assert_eq!(unchanged.next_check_at, scheduled.next_check_at);

    assert!(
        store.set_playlist_subscription_check_interval("sub-1".to_owned(), 60, 200).await.is_err()
    );
    assert!(
        store
            .set_playlist_subscription_check_interval(
                "missing".to_owned(),
                PLAYLIST_CHECK_INTERVAL_1_HOUR,
                200,
            )
            .await
            .is_err()
    );

    store
        .set_playlist_subscription_check_interval(
            "sub-1".to_owned(),
            PLAYLIST_CHECK_INTERVAL_MANUAL,
            300,
        )
        .await
        .unwrap();
    let manual = store.list_playlist_subscriptions().await.unwrap().remove(0);
    assert_eq!(manual.check_interval_seconds, PLAYLIST_CHECK_INTERVAL_MANUAL);
    assert_eq!(manual.next_check_at, None);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn subscription_auto_queue_setting_defaults_false_and_persists() {
    let (temp_dir, store) = new_subscription_store().await;
    let created = store.list_playlist_subscriptions().await.unwrap().remove(0);
    assert!(!created.auto_queue_new_entries);

    store.set_playlist_subscription_auto_queue_new_entries("sub-1".to_owned(), true).await.unwrap();
    assert!(store.list_playlist_subscriptions().await.unwrap()[0].auto_queue_new_entries);
    assert!(
        store
            .set_playlist_subscription_auto_queue_new_entries("missing".to_owned(), true)
            .await
            .is_err()
    );
    drop(store);

    let reopened = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let saved = reopened.list_playlist_subscriptions().await.unwrap().remove(0);
    assert!(saved.auto_queue_new_entries);
    reopened
        .set_playlist_subscription_check_interval(
            "sub-1".to_owned(),
            PLAYLIST_CHECK_INTERVAL_1_HOUR,
            100,
        )
        .await
        .unwrap();
    let due = reopened
        .list_due_playlist_subscriptions(100 + PLAYLIST_CHECK_INTERVAL_1_HOUR, 10)
        .await
        .unwrap();
    assert!(due[0].auto_queue_new_entries);
    reopened
        .set_playlist_subscription_auto_queue_new_entries("sub-1".to_owned(), false)
        .await
        .unwrap();
    assert!(!reopened.list_playlist_subscriptions().await.unwrap()[0].auto_queue_new_entries);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn due_subscription_list_is_ordered_bounded_and_survives_reopen() {
    let (temp_dir, store) = new_subscription_store().await;
    let now = 1_000_000;
    store
        .set_playlist_subscription_check_interval(
            "sub-1".to_owned(),
            PLAYLIST_CHECK_INTERVAL_1_HOUR,
            now - PLAYLIST_CHECK_INTERVAL_1_HOUR - 200,
        )
        .await
        .unwrap();
    add_playlist_subscription(&store, "sub-2", 20).await;
    store
        .set_playlist_subscription_check_interval(
            "sub-2".to_owned(),
            PLAYLIST_CHECK_INTERVAL_6_HOURS,
            now - PLAYLIST_CHECK_INTERVAL_6_HOURS - 1_000,
        )
        .await
        .unwrap();
    add_playlist_subscription(&store, "sub-3", 30).await;
    store
        .set_playlist_subscription_check_interval(
            "sub-3".to_owned(),
            PLAYLIST_CHECK_INTERVAL_24_HOURS,
            now - PLAYLIST_CHECK_INTERVAL_24_HOURS + 5,
        )
        .await
        .unwrap();
    add_playlist_subscription(&store, "manual", 40).await;

    let limited = store.list_due_playlist_subscriptions(now, 1).await.unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].id, "sub-2");
    assert_eq!(limited[0].next_check_at, Some(now - 1_000));
    assert!(store.list_due_playlist_subscriptions(now, 0).await.unwrap().is_empty());

    drop(store);
    let reopened = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let due = reopened.list_due_playlist_subscriptions(now, usize::MAX).await.unwrap();
    assert_eq!(
        due.iter().map(|subscription| subscription.id.as_str()).collect::<Vec<_>>(),
        ["sub-2", "sub-1"]
    );
    assert_eq!(due[1].next_check_at, Some(now - 200));
    assert!(due.iter().all(|subscription| subscription.check_interval_seconds > 0));
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn successful_and_failed_subscription_checks_reschedule_from_check_time() {
    let (temp_dir, store) = new_subscription_store().await;
    store
        .set_playlist_subscription_check_interval(
            "sub-1".to_owned(),
            PLAYLIST_CHECK_INTERVAL_6_HOURS,
            100,
        )
        .await
        .unwrap();

    store
        .record_playlist_subscription_check(
            "sub-1".to_owned(),
            "Updated title".to_owned(),
            "Example".to_owned(),
            200,
            Vec::new(),
        )
        .await
        .unwrap();
    let successful = store.list_playlist_subscriptions().await.unwrap().remove(0);
    assert_eq!(successful.last_checked_at, Some(200));
    assert_eq!(successful.last_check_error, None);
    assert_eq!(successful.next_check_at, Some(200 + PLAYLIST_CHECK_INTERVAL_6_HOURS));

    store
        .record_playlist_subscription_error("sub-1".to_owned(), "temporary failure".to_owned(), 300)
        .await
        .unwrap();
    let failed = store.list_playlist_subscriptions().await.unwrap().remove(0);
    assert_eq!(failed.last_checked_at, Some(300));
    assert_eq!(failed.last_check_error.as_deref(), Some("temporary failure"));
    assert_eq!(failed.next_check_at, Some(300 + PLAYLIST_CHECK_INTERVAL_6_HOURS));
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn subscription_tables_migrate_and_records_survive_reopen() {
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
                    CREATE TABLE playlist_subscriptions (
                        id TEXT PRIMARY KEY NOT NULL,
                        url TEXT NOT NULL UNIQUE,
                        title TEXT NOT NULL DEFAULT '',
                        source TEXT NOT NULL DEFAULT '',
                        created_at INTEGER NOT NULL,
                        last_checked_at INTEGER,
                        last_check_error TEXT
                    );
                    INSERT INTO playlist_subscriptions
                        (id, url, title, source, created_at, last_checked_at)
                    VALUES
                        ('legacy-sub', 'https://example.test/legacy', 'Old playlist', 'Example', 5, 7);",
                )
                .unwrap();
    }

    let store = Store::open(db_path.clone()).unwrap();
    store
        .create_playlist_subscription(
            "migrated-sub".to_owned(),
            "https://example.test/playlist".to_owned(),
            "Before reopen".to_owned(),
            "Example".to_owned(),
            10,
        )
        .await
        .unwrap();
    store
        .record_playlist_subscription_check(
            "migrated-sub".to_owned(),
            "After check".to_owned(),
            "Example Extractor".to_owned(),
            20,
            vec![subscription_entry(
                "example:one",
                Some("one"),
                "https://cdn.example.test/one.mp4",
                "One",
                Some(1),
                true,
            )],
        )
        .await
        .unwrap();
    drop(store);

    let reopened = Store::open(db_path).unwrap();
    let subscriptions = reopened.list_playlist_subscriptions().await.unwrap();
    assert_eq!(subscriptions.len(), 2);
    let migrated =
        subscriptions.iter().find(|subscription| subscription.id == "migrated-sub").unwrap();
    assert_eq!(migrated.title, "After check");
    assert_eq!(migrated.last_checked_at, Some(20));
    assert_eq!(migrated.pending_count, 1);
    assert_eq!(migrated.check_interval_seconds, PLAYLIST_CHECK_INTERVAL_MANUAL);
    assert_eq!(migrated.next_check_at, None);
    let legacy = subscriptions.iter().find(|subscription| subscription.id == "legacy-sub").unwrap();
    assert_eq!(legacy.title, "Old playlist");
    assert_eq!(legacy.last_checked_at, Some(7));
    assert_eq!(legacy.check_interval_seconds, PLAYLIST_CHECK_INTERVAL_MANUAL);
    assert_eq!(legacy.next_check_at, None);
    assert!(!legacy.auto_queue_new_entries);
    let entries =
        reopened.list_playlist_subscription_entries("migrated-sub".to_owned()).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].media_key, "example:one");
    assert_eq!(entries[0].state, "new");
    assert_eq!(entries[0].first_seen_at, 20);
    assert_eq!(entries[0].last_seen_at, 20);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn first_subscription_check_offers_all_seen_entries_as_new() {
    let (temp_dir, store) = new_subscription_store().await;
    let inserted = store
        .record_playlist_subscription_check(
            "sub-1".to_owned(),
            "My playlist".to_owned(),
            "Example extractor".to_owned(),
            20,
            vec![
                subscription_entry(
                    "example:one",
                    Some("one"),
                    "https://cdn.example.test/one.mp4",
                    "One",
                    Some(1),
                    true,
                ),
                subscription_entry(
                    "example:two",
                    Some("two"),
                    "https://cdn.example.test/two.mp4",
                    "Two",
                    Some(2),
                    true,
                ),
            ],
        )
        .await
        .unwrap();
    assert_eq!(inserted, 2);
    let listed = store.list_playlist_subscriptions().await.unwrap();
    assert_eq!(listed[0].pending_count, 2);
    assert_eq!(listed[0].last_check_error, None);
    let entries = store.list_playlist_subscription_entries("sub-1".to_owned()).await.unwrap();
    assert_eq!(
        entries.iter().map(|entry| entry.state.as_str()).collect::<Vec<_>>(),
        ["new", "new"]
    );
    assert!(entries.iter().all(|entry| entry.available));
    assert!(entries.iter().all(|entry| entry.first_seen_at == 20 && entry.last_seen_at == 20));
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn auto_queue_check_links_only_new_available_entries_and_ignores_repeats() {
    let (temp_dir, store) = new_subscription_store().await;
    store.set_playlist_subscription_auto_queue_new_entries("sub-1".to_owned(), true).await.unwrap();

    let entries = vec![
        subscription_entry(
            "example:available",
            Some("available"),
            "https://cdn.example.test/available.mp4",
            "Available",
            Some(1),
            true,
        ),
        subscription_entry(
            "example:unavailable",
            Some("unavailable"),
            "https://cdn.example.test/unavailable.mp4",
            "Unavailable",
            Some(2),
            false,
        ),
    ];
    let queue_items = vec![
        PlaylistSubscriptionQueueItem {
            media_key: "example:available".to_owned(),
            record: subscription_download(
                "auto-available",
                "https://cdn.example.test/available.mp4",
                20,
            ),
        },
        PlaylistSubscriptionQueueItem {
            media_key: "example:unavailable".to_owned(),
            record: subscription_download(
                "auto-unavailable",
                "https://cdn.example.test/unavailable.mp4",
                20,
            ),
        },
    ];
    let inserted = store
        .record_playlist_subscription_check_with_queue_items(
            "sub-1".to_owned(),
            "Playlist title".to_owned(),
            "Example".to_owned(),
            20,
            entries.clone(),
            queue_items.clone(),
        )
        .await
        .unwrap();
    assert_eq!(inserted, (2, 1));

    let downloads = store.list_downloads().await.unwrap();
    assert_eq!(downloads.len(), 1);
    assert_eq!(downloads[0].id, "auto-available");
    assert_eq!(downloads[0].status, "Queued");
    let archived = store.list_playlist_subscription_entries("sub-1".to_owned()).await.unwrap();
    let available = archived.iter().find(|entry| entry.media_key == "example:available").unwrap();
    assert_eq!(available.state, "queued");
    assert_eq!(available.download_id.as_deref(), Some("auto-available"));
    let unavailable =
        archived.iter().find(|entry| entry.media_key == "example:unavailable").unwrap();
    assert!(!unavailable.available);
    assert_eq!(unavailable.state, "new");
    assert_eq!(unavailable.download_id, None);
    assert_eq!(store.list_playlist_subscriptions().await.unwrap()[0].pending_count, 1);

    let repeated = store
        .record_playlist_subscription_check_with_queue_items(
            "sub-1".to_owned(),
            "Playlist title".to_owned(),
            "Example".to_owned(),
            30,
            entries,
            queue_items,
        )
        .await
        .unwrap();
    assert_eq!(repeated, (0, 0));
    assert_eq!(store.list_downloads().await.unwrap().len(), 1);
    let repeated_archive =
        store.list_playlist_subscription_entries("sub-1".to_owned()).await.unwrap();
    let available =
        repeated_archive.iter().find(|entry| entry.media_key == "example:available").unwrap();
    assert_eq!(available.state, "queued");
    assert_eq!(available.download_id.as_deref(), Some("auto-available"));
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn subscription_check_keeps_new_items_in_review_mode_by_default() {
    let (temp_dir, store) = new_subscription_store().await;
    let inserted = store
        .record_playlist_subscription_check_with_queue_items(
            "sub-1".to_owned(),
            "Playlist title".to_owned(),
            "Example".to_owned(),
            20,
            vec![subscription_entry(
                "example:one",
                Some("one"),
                "https://cdn.example.test/one.mp4",
                "One",
                Some(1),
                true,
            )],
            vec![PlaylistSubscriptionQueueItem {
                media_key: "example:one".to_owned(),
                record: subscription_download(
                    "review-only",
                    "https://cdn.example.test/one.mp4",
                    20,
                ),
            }],
        )
        .await
        .unwrap();

    assert_eq!(inserted, (1, 0));
    assert!(store.list_downloads().await.unwrap().is_empty());
    let archived = store.list_playlist_subscription_entries("sub-1".to_owned()).await.unwrap();
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].state, "new");
    assert_eq!(archived[0].download_id, None);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn legacy_subscription_check_api_remains_review_first_when_opt_in_is_enabled() {
    let (temp_dir, store) = new_subscription_store().await;
    store.set_playlist_subscription_auto_queue_new_entries("sub-1".to_owned(), true).await.unwrap();

    let inserted = store
        .record_playlist_subscription_check(
            "sub-1".to_owned(),
            "Playlist title".to_owned(),
            "Example".to_owned(),
            20,
            vec![subscription_entry(
                "example:one",
                Some("one"),
                "https://cdn.example.test/one.mp4",
                "One",
                Some(1),
                true,
            )],
        )
        .await
        .unwrap();

    assert_eq!(inserted, 1);
    assert!(store.list_downloads().await.unwrap().is_empty());
    let archived = store.list_playlist_subscription_entries("sub-1".to_owned()).await.unwrap();
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].state, "new");
    assert_eq!(archived[0].download_id, None);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn failed_auto_queue_batch_rolls_back_archive_check_and_queue_rows() {
    let (temp_dir, store) = new_subscription_store().await;
    store.set_playlist_subscription_auto_queue_new_entries("sub-1".to_owned(), true).await.unwrap();
    let mut existing =
        subscription_download("collision", "https://cdn.example.test/existing.mp4", 5);
    existing.status = "Completed".to_owned();
    store.save_download(existing).await.unwrap();

    let result = store
        .record_playlist_subscription_check_with_queue_items(
            "sub-1".to_owned(),
            "Should roll back".to_owned(),
            "Example".to_owned(),
            20,
            vec![
                subscription_entry(
                    "example:first",
                    Some("first"),
                    "https://cdn.example.test/first.mp4",
                    "First",
                    Some(1),
                    true,
                ),
                subscription_entry(
                    "example:second",
                    Some("second"),
                    "https://cdn.example.test/second.mp4",
                    "Second",
                    Some(2),
                    true,
                ),
            ],
            vec![
                PlaylistSubscriptionQueueItem {
                    media_key: "example:first".to_owned(),
                    record: subscription_download(
                        "fresh-first",
                        "https://cdn.example.test/first.mp4",
                        20,
                    ),
                },
                PlaylistSubscriptionQueueItem {
                    media_key: "example:second".to_owned(),
                    record: subscription_download(
                        "collision",
                        "https://cdn.example.test/second.mp4",
                        20,
                    ),
                },
            ],
        )
        .await;
    assert!(result.is_err());

    let subscription = store.list_playlist_subscriptions().await.unwrap().remove(0);
    assert_eq!(subscription.title, "Initial title");
    assert_eq!(subscription.last_checked_at, None);
    assert_eq!(subscription.pending_count, 0);
    let archived = store.list_playlist_subscription_entries("sub-1".to_owned()).await.unwrap();
    assert!(archived.is_empty());
    let downloads = store.list_downloads().await.unwrap();
    assert_eq!(downloads.len(), 1);
    assert_eq!(downloads[0].id, "collision");
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn reordered_and_renamed_checks_preserve_archive_and_handled_state() {
    let (temp_dir, store) = new_subscription_store().await;
    store
        .record_playlist_subscription_check(
            "sub-1".to_owned(),
            "First title".to_owned(),
            "First source".to_owned(),
            20,
            vec![
                subscription_entry(
                    "example:one",
                    Some("one"),
                    "https://cdn.example.test/one.mp4",
                    "One",
                    Some(1),
                    true,
                ),
                subscription_entry(
                    "example:two",
                    Some("two"),
                    "https://cdn.example.test/two.mp4",
                    "Two",
                    Some(2),
                    true,
                ),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .dismiss_playlist_subscription_entries(
                "sub-1".to_owned(),
                vec!["example:two".to_owned()],
                21,
            )
            .await
            .unwrap(),
        1
    );
    let queued_download = subscription_download("job-one", "https://cdn.example.test/one.mp4", 22);
    assert_eq!(
        store
            .queue_playlist_subscription_entries(
                "sub-1".to_owned(),
                vec![PlaylistSubscriptionQueueItem {
                    media_key: "example:one".to_owned(),
                    record: queued_download,
                }],
            )
            .await
            .unwrap(),
        1
    );

    assert_eq!(
        store
            .record_playlist_subscription_check(
                "sub-1".to_owned(),
                "Renamed playlist".to_owned(),
                "Renamed source".to_owned(),
                30,
                vec![
                    subscription_entry(
                        "example:two",
                        Some("two"),
                        "https://cdn.example.test/two-v2.mp4",
                        "Two renamed",
                        Some(1),
                        true
                    ),
                    subscription_entry(
                        "example:one",
                        Some("one"),
                        "https://cdn.example.test/one.mp4",
                        "One renamed",
                        Some(2),
                        true
                    ),
                ],
            )
            .await
            .unwrap(),
        0
    );

    let subscription =
        store.list_playlist_subscriptions().await.unwrap().into_iter().next().unwrap();
    assert_eq!(subscription.title, "Renamed playlist");
    assert_eq!(subscription.source, "Renamed source");
    assert_eq!(subscription.last_checked_at, Some(30));
    assert_eq!(subscription.pending_count, 0);
    let entries = store.list_playlist_subscription_entries("sub-1".to_owned()).await.unwrap();
    assert_eq!(entries[0].media_key, "example:two");
    assert_eq!(entries[0].title, "Two renamed");
    assert_eq!(entries[0].state, "ignored");
    assert_eq!(entries[0].first_seen_at, 20);
    assert_eq!(entries[0].last_seen_at, 30);
    assert_eq!(entries[1].media_key, "example:one");
    assert_eq!(entries[1].state, "queued");
    assert_eq!(entries[1].download_id.as_deref(), Some("job-one"));
    assert_eq!(entries[1].first_seen_at, 20);
    assert_eq!(entries[1].last_seen_at, 30);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn unavailable_items_are_archived_but_cannot_be_queued() {
    let (temp_dir, store) = new_subscription_store().await;
    store
        .record_playlist_subscription_check(
            "sub-1".to_owned(),
            "Playlist".to_owned(),
            "Example".to_owned(),
            20,
            vec![subscription_entry(
                "example:private",
                Some("private"),
                "",
                "Private item",
                Some(1),
                false,
            )],
        )
        .await
        .unwrap();
    let entry = store
        .list_playlist_subscription_entries("sub-1".to_owned())
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(!entry.available);
    assert_eq!(entry.state, "new");
    assert_eq!(store.list_playlist_subscriptions().await.unwrap()[0].pending_count, 1);
    let result = store
        .queue_playlist_subscription_entries(
            "sub-1".to_owned(),
            vec![PlaylistSubscriptionQueueItem {
                media_key: "example:private".to_owned(),
                record: subscription_download("private-job", "", 21),
            }],
        )
        .await;
    assert!(result.is_err());
    assert!(store.list_downloads().await.unwrap().is_empty());

    store
        .record_playlist_subscription_check(
            "sub-1".to_owned(),
            "Playlist".to_owned(),
            "Example".to_owned(),
            30,
            vec![],
        )
        .await
        .unwrap();
    let entry = store
        .list_playlist_subscription_entries("sub-1".to_owned())
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(!entry.available);
    assert_eq!(entry.last_seen_at, 20);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn subscription_queue_is_atomic_and_duplicate_queue_is_rejected() {
    let (temp_dir, store) = new_subscription_store().await;
    store
        .record_playlist_subscription_check(
            "sub-1".to_owned(),
            "Playlist".to_owned(),
            "Example".to_owned(),
            20,
            vec![
                subscription_entry(
                    "example:one",
                    Some("one"),
                    "https://cdn.example.test/one.mp4",
                    "One",
                    Some(1),
                    true,
                ),
                subscription_entry("example:unavailable", None, "", "Unavailable", Some(2), false),
            ],
        )
        .await
        .unwrap();
    let atomic_failure = store
        .queue_playlist_subscription_entries(
            "sub-1".to_owned(),
            vec![
                PlaylistSubscriptionQueueItem {
                    media_key: "example:one".to_owned(),
                    record: subscription_download(
                        "job-one",
                        "https://cdn.example.test/one.mp4",
                        21,
                    ),
                },
                PlaylistSubscriptionQueueItem {
                    media_key: "example:unavailable".to_owned(),
                    record: subscription_download("job-unavailable", "", 21),
                },
            ],
        )
        .await;
    assert!(atomic_failure.is_err());
    assert!(store.list_downloads().await.unwrap().is_empty());
    assert!(
        store
            .list_playlist_subscription_entries("sub-1".to_owned())
            .await
            .unwrap()
            .iter()
            .all(|entry| entry.state == "new" && entry.download_id.is_none())
    );

    let selected = PlaylistSubscriptionQueueItem {
        media_key: "example:one".to_owned(),
        record: subscription_download("job-one", "https://cdn.example.test/one.mp4", 22),
    };
    assert_eq!(
        store
            .queue_playlist_subscription_entries("sub-1".to_owned(), vec![selected.clone()])
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .queue_playlist_subscription_entries("sub-1".to_owned(), vec![selected])
            .await
            .is_err()
    );
    assert_eq!(store.list_downloads().await.unwrap().len(), 1);
    let entry = store
        .list_playlist_subscription_entries("sub-1".to_owned())
        .await
        .unwrap()
        .into_iter()
        .find(|entry| entry.media_key == "example:one")
        .unwrap();
    assert_eq!(entry.state, "queued");
    assert_eq!(entry.download_id.as_deref(), Some("job-one"));
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn deleting_subscription_leaves_linked_download_record_intact() {
    let (temp_dir, store) = new_subscription_store().await;
    store
        .record_playlist_subscription_check(
            "sub-1".to_owned(),
            "Playlist".to_owned(),
            "Example".to_owned(),
            20,
            vec![subscription_entry(
                "example:one",
                Some("one"),
                "https://cdn.example.test/one.mp4",
                "One",
                Some(1),
                true,
            )],
        )
        .await
        .unwrap();
    let record = subscription_download("job-one", "https://cdn.example.test/one.mp4", 21);
    store
        .queue_playlist_subscription_entries(
            "sub-1".to_owned(),
            vec![PlaylistSubscriptionQueueItem { media_key: "example:one".to_owned(), record }],
        )
        .await
        .unwrap();

    store
        .with_connection(|connection| {
            connection
                .execute("UPDATE downloads SET status = 'Completed' WHERE id = 'job-one'", [])?;
            Ok(())
        })
        .unwrap();
    let archived = store.list_playlist_subscription_entries("sub-1".to_owned()).await.unwrap();
    assert_eq!(archived[0].state, "downloaded");

    assert!(store.delete_playlist_subscription("sub-1".to_owned()).await.unwrap());
    assert!(!store.delete_playlist_subscription("sub-1".to_owned()).await.unwrap());
    assert!(store.list_playlist_subscriptions().await.unwrap().is_empty());
    assert!(store.list_playlist_subscription_entries("sub-1".to_owned()).await.unwrap().is_empty());
    let downloads = store.list_downloads().await.unwrap();
    assert_eq!(downloads.len(), 1);
    assert_eq!(downloads[0].id, "job-one");
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn subscription_download_state_survives_history_removal_and_clear() {
    let (temp_dir, store) = new_subscription_store().await;
    let first_file = temp_dir.join("first.mp4");
    let second_file = temp_dir.join("second.mp4");
    std::fs::write(&first_file, b"first media").unwrap();
    std::fs::write(&second_file, b"second media").unwrap();
    store
        .record_playlist_subscription_check(
            "sub-1".to_owned(),
            "Playlist".to_owned(),
            "Example".to_owned(),
            20,
            vec![
                subscription_entry(
                    "example:first",
                    Some("first"),
                    "https://cdn.example.test/first.mp4",
                    "First",
                    Some(1),
                    true,
                ),
                subscription_entry(
                    "example:second",
                    Some("second"),
                    "https://cdn.example.test/second.mp4",
                    "Second",
                    Some(2),
                    true,
                ),
            ],
        )
        .await
        .unwrap();
    let mut first = subscription_download("job-first", "https://cdn.example.test/first.mp4", 21);
    first.output_path = Some(first_file.to_string_lossy().into_owned());
    let mut second = subscription_download("job-second", "https://cdn.example.test/second.mp4", 22);
    second.output_path = Some(second_file.to_string_lossy().into_owned());
    store
        .queue_playlist_subscription_entries(
            "sub-1".to_owned(),
            vec![
                PlaylistSubscriptionQueueItem {
                    media_key: "example:first".to_owned(),
                    record: first,
                },
                PlaylistSubscriptionQueueItem {
                    media_key: "example:second".to_owned(),
                    record: second,
                },
            ],
        )
        .await
        .unwrap();

    let mut first = store.claim_queued_download_if_next("job-first", 23).await.unwrap().unwrap();
    first.status = "Completed".to_owned();
    first.updated_at = 24;
    store.finish_download(first).await.unwrap();
    let mut second = store.claim_queued_download_if_next("job-second", 25).await.unwrap().unwrap();
    second.status = "Completed".to_owned();
    second.updated_at = 26;
    store.finish_download(second).await.unwrap();
    assert!(
        store
            .list_playlist_subscription_entries("sub-1".to_owned())
            .await
            .unwrap()
            .iter()
            .all(|entry| entry.state == "downloaded")
    );

    assert!(store.remove_download("job-first").await.unwrap());
    assert_eq!(store.clear_completed_downloads().await.unwrap(), 1);
    let entries = store.list_playlist_subscription_entries("sub-1".to_owned()).await.unwrap();
    assert!(entries.iter().all(|entry| entry.state == "downloaded"));
    assert_eq!(
        entries.iter().map(|entry| entry.completed_at.is_some()).collect::<Vec<_>>(),
        [true, true]
    );
    assert!(first_file.is_file());
    assert!(second_file.is_file());
    assert!(store.list_downloads().await.unwrap().is_empty());
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn reopened_connections_enforce_subscription_foreign_keys() {
    let (temp_dir, store) = new_subscription_store().await;
    store
        .record_playlist_subscription_check(
            "sub-1".to_owned(),
            "Playlist".to_owned(),
            "Example".to_owned(),
            20,
            vec![subscription_entry(
                "example:one",
                Some("one"),
                "https://cdn.example.test/one.mp4",
                "One",
                Some(1),
                true,
            )],
        )
        .await
        .unwrap();
    store
        .with_connection_mut(|connection| {
            connection.execute("DELETE FROM playlist_subscriptions WHERE id = 'sub-1'", [])?;
            Ok(())
        })
        .unwrap();
    assert!(store.list_playlist_subscription_entries("sub-1".to_owned()).await.unwrap().is_empty());
    std::fs::remove_dir_all(temp_dir).unwrap();
}
