use super::super::*;

#[tokio::test]
async fn download_state_survives_reopening_the_database() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let db_path = temp_dir.join("state.sqlite");
    let store = Store::open(db_path.clone()).unwrap();
    let mut record =
        DownloadRecord::queued("job-1".to_owned(), "https://example.test/media".to_owned(), 10);
    record.playlist_index = Some(4);
    record.download_mode = DownloadMode::SubtitlesOnly;
    record.subtitle_languages = "en,fr".to_owned();
    record.subtitle_format = SubtitleFormat::Srt;
    record.include_auto_subtitles = true;
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
    assert_eq!(rows[0].download_mode, DownloadMode::SubtitlesOnly);
    assert_eq!(rows[0].subtitle_languages, "en,fr");
    assert_eq!(rows[0].subtitle_format, SubtitleFormat::Srt);
    assert!(rows[0].include_auto_subtitles);
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
async fn queue_batch_write_rolls_back_when_a_later_record_is_invalid() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let first =
        DownloadRecord::queued("first".to_owned(), "https://example.test/first".to_owned(), 10);
    let mut invalid =
        DownloadRecord::queued("invalid".to_owned(), "https://example.test/invalid".to_owned(), 11);
    invalid.downloaded_bytes = Some(u64::MAX);
    let last =
        DownloadRecord::queued("last".to_owned(), "https://example.test/last".to_owned(), 12);

    assert!(store.save_downloads(vec![first, invalid, last]).await.is_err());
    assert!(store.list_downloads().await.unwrap().is_empty());

    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn reordering_pending_rows_does_not_reuse_active_queue_ranks() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let first =
        DownloadRecord::queued("first".to_owned(), "https://example.test/first".to_owned(), 10);
    let mut active =
        DownloadRecord::queued("active".to_owned(), "https://example.test/active".to_owned(), 11);
    active.status = "Downloading".to_owned();
    let second =
        DownloadRecord::queued("second".to_owned(), "https://example.test/second".to_owned(), 12);
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
async fn video_only_mode_and_quality_survive_reopen_and_retry() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let db_path = temp_dir.join("state.sqlite");
    let store = Store::open(db_path.clone()).unwrap();
    let mut record = DownloadRecord::queued(
        "video-retry".to_owned(),
        "https://example.test/video".to_owned(),
        10,
    );
    record.status = "Failed".to_owned();
    record.download_mode = DownloadMode::VideoOnly;
    record.video_quality = VideoQuality::P1440;
    record.selected_format_id = Some("video-137".to_owned());
    store.save_download(record).await.unwrap();
    drop(store);

    let reopened = Store::open(db_path).unwrap();
    let retried = reopened.retry_download("video-retry", 20).await.unwrap().unwrap();
    assert_eq!(retried.download_mode, DownloadMode::VideoOnly);
    assert_eq!(retried.video_quality, VideoQuality::P1440);
    assert_eq!(retried.selected_format_id.as_deref(), Some("video-137"));
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
    assert_eq!(legacy.subtitle_languages, "en");
    assert_eq!(legacy.subtitle_format, SubtitleFormat::Vtt);
    assert!(!legacy.include_auto_subtitles);
    assert_eq!(legacy.playlist_group_title, None);
    assert_eq!(legacy.download_mode, DownloadMode::Video);
    assert_eq!(legacy.audio_format, AudioFormat::Original);
    assert_eq!(legacy.audio_quality, AudioQuality::Best);
    assert_eq!(legacy.video_quality, VideoQuality::Best);
    assert_eq!(legacy.selected_format_id, None);
    assert_eq!(legacy.download_speed_limit_bytes_per_second, 0);
    assert_eq!(legacy.download_directory, legacy_default);
    assert_eq!(legacy.retry_attempts, 0);
    assert_eq!(legacy.next_retry_at, None);
    assert_eq!(legacy.scheduled_at, None);

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
    selected.download_speed_limit_bytes_per_second = 5_000_000;
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
    assert_eq!(selected.download_speed_limit_bytes_per_second, 5_000_000);
    assert_eq!(selected.selected_format_id, None);
    assert_eq!(
        selected.download_directory,
        temp_dir.join("Selected").to_string_lossy().into_owned()
    );
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

    let mut completed =
        DownloadRecord::queued("completed".to_owned(), "https://example.test/media".to_owned(), 10);
    completed.status = "Completed".to_owned();
    completed.output_path = Some(output_path.to_string_lossy().into_owned());
    store.save_download(completed).await.unwrap();

    let mut active =
        DownloadRecord::queued("active".to_owned(), "https://example.test/active".to_owned(), 11);
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
    let mut completed =
        DownloadRecord::queued("completed".to_owned(), "https://example.test/media".to_owned(), 10);
    completed.status = "Completed".to_owned();
    completed.output_path = Some("C:\\old-location\\media.mp4".to_owned());
    store.save_download(completed).await.unwrap();
    let active =
        DownloadRecord::queued("active".to_owned(), "https://example.test/active".to_owned(), 11);
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

    let queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/queued".to_owned(), 11);
    store.save_download(queued).await.unwrap();

    let mut failed =
        DownloadRecord::queued("failed".to_owned(), "https://example.test/failed".to_owned(), 12);
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
    let mut failed =
        DownloadRecord::queued("failed".to_owned(), "https://example.test/media".to_owned(), 10);
    failed.status = "Failed".to_owned();
    failed.progress = 34.5;
    failed.downloaded_bytes = Some(345);
    failed.total_bytes = Some(1_000);
    failed.retry_attempts = 4;
    failed.next_retry_at = Some(100);
    failed.scheduled_at = Some(200);
    failed.error = Some("temporary network error".to_owned());
    failed.playlist_index = Some(5);
    failed.download_mode = DownloadMode::AudioOnly;
    failed.audio_format = AudioFormat::Mp3;
    failed.audio_quality = AudioQuality::K128;
    failed.video_quality = VideoQuality::P1080;
    store.save_download(failed).await.unwrap();

    let queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/queued".to_owned(), 11);
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
    assert_eq!(retried.scheduled_at, None);
    assert!(store.retry_download("failed", 21).await.unwrap().is_none());
    assert!(store.retry_download("queued", 22).await.unwrap().is_none());

    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn scheduled_rows_survive_reopen_and_only_become_claimable_when_due() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let db_path = temp_dir.join("state.sqlite3");
    let store = Store::open(db_path.clone()).unwrap();
    for (id, scheduled_at) in [("future", 101), ("due", 100)] {
        let mut scheduled = DownloadRecord::queued(
            id.to_owned(),
            "https://example.test/media".to_owned(),
            scheduled_at - 90,
        );
        scheduled.status = "Waiting for schedule".to_owned();
        scheduled.scheduled_at = Some(scheduled_at);
        store.save_download(scheduled).await.unwrap();
    }
    assert!(store.next_queued_download().await.unwrap().is_none());
    assert_eq!(store.next_scheduled_at().await.unwrap(), Some(100));
    drop(store);

    let reopened = Store::open(db_path).unwrap();
    let restored = reopened.list_downloads().await.unwrap();
    for (id, scheduled_at) in [("future", 101), ("due", 100)] {
        let row = restored.iter().find(|row| row.id == id).unwrap();
        assert_eq!(row.status, "Waiting for schedule");
        assert!(row.scheduled_at.is_some());
        assert_eq!(row.scheduled_at, Some(scheduled_at));
    }
    assert_eq!(reopened.promote_due_scheduled_downloads(99).await.unwrap(), 0);
    assert!(reopened.claim_queued_download_if_next("due", 99).await.unwrap().is_none());

    assert_eq!(reopened.promote_due_scheduled_downloads(100).await.unwrap(), 1);
    let claimed_due = reopened.claim_queued_download_if_next("due", 100).await.unwrap().unwrap();
    assert_eq!(claimed_due.status, "Analyzing");
    assert_eq!(claimed_due.scheduled_at, None);
    assert!(reopened.claim_queued_download_if_next("future", 100).await.unwrap().is_none());

    assert_eq!(reopened.promote_due_scheduled_downloads(101).await.unwrap(), 1);
    let claimed_future =
        reopened.claim_queued_download_if_next("future", 101).await.unwrap().unwrap();
    assert_eq!(claimed_future.status, "Analyzing");
    assert_eq!(claimed_future.scheduled_at, None);
    assert_eq!(reopened.next_scheduled_at().await.unwrap(), None);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn scheduled_pause_preserves_deadline_until_resume_and_cancel_clears_it() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    for (id, scheduled_at) in [("resumable", 200), ("overdue", 200), ("cancel-me", 300)] {
        let mut scheduled =
            DownloadRecord::queued(id.to_owned(), "https://example.test/media".to_owned(), 10);
        scheduled.status = "Waiting for schedule".to_owned();
        scheduled.scheduled_at = Some(scheduled_at);
        store.save_download(scheduled).await.unwrap();
    }

    assert!(store.pause_queued_download("resumable", 20).await.unwrap());
    let paused = store
        .list_downloads()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id == "resumable")
        .unwrap();
    assert_eq!(paused.status, "Paused");
    assert_eq!(paused.scheduled_at, Some(200));
    assert_eq!(store.promote_due_scheduled_downloads(250).await.unwrap(), 1);
    let still_paused = store
        .list_downloads()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id == "resumable")
        .unwrap();
    assert_eq!(still_paused.status, "Paused");
    assert_eq!(still_paused.scheduled_at, Some(200));

    let resumed_before_due = store.resume_download("resumable", 100).await.unwrap().unwrap();
    assert_eq!(resumed_before_due.status, "Waiting for schedule");
    assert_eq!(resumed_before_due.scheduled_at, Some(200));
    assert!(store.pause_queued_download("resumable", 101).await.unwrap());
    let resumed_after_due = store.resume_download("resumable", 201).await.unwrap().unwrap();
    assert_eq!(resumed_after_due.status, "Queued");
    assert_eq!(resumed_after_due.scheduled_at, None);

    assert!(store.pause_queued_download("cancel-me", 30).await.unwrap());
    assert!(store.cancel_queued_download("cancel-me", 31).await.unwrap());
    let canceled = store
        .list_downloads()
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id == "cancel-me")
        .unwrap();
    assert_eq!(canceled.status, "Canceled");
    assert_eq!(canceled.scheduled_at, None);

    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn active_cancel_blocks_stale_updates_and_canceled_rows_retry_with_partial_progress() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let mut active =
        DownloadRecord::queued("active".to_owned(), "https://example.test/media".to_owned(), 10);
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
    let mut queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/media".to_owned(), 10);
    queued.download_mode = crate::model::DownloadMode::VideoOnly;
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
    assert_eq!(resumed.download_mode, crate::model::DownloadMode::VideoOnly);
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
    let queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/media".to_owned(), 10);
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
    let mut active =
        DownloadRecord::queued("active".to_owned(), "https://example.test/media".to_owned(), 10);
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
    let mut active =
        DownloadRecord::queued("active".to_owned(), "https://example.test/media".to_owned(), 10);
    active.status = "Downloading".to_owned();
    store.save_download(active.clone()).await.unwrap();

    let mut completed = active;
    completed.status = "Completed".to_owned();
    completed.progress = 100.0;
    let (pause_requested, terminal_saved) =
        tokio::join!(store.request_active_pause("active", 20), store.finish_download(completed),);
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
    let mut active =
        DownloadRecord::queued("active".to_owned(), "https://example.test/media".to_owned(), 10);
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
    let mut active =
        DownloadRecord::queued("racing".to_owned(), "https://example.test/media".to_owned(), 10);
    active.status = "Downloading".to_owned();
    store.save_download(active.clone()).await.unwrap();

    let mut completed = active;
    completed.status = "Completed".to_owned();
    completed.progress = 100.0;
    let (cancel_requested, terminal_saved) =
        tokio::join!(store.request_active_cancel("racing", 20), store.finish_download(completed),);
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
    let mut active =
        DownloadRecord::queued("active".to_owned(), "https://example.test/media".to_owned(), 10);
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
    assert_eq!(persisted.error.as_deref(), Some("process-tree termination could not be confirmed"));

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
    let queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/media".to_owned(), 10);
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
    let queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/media".to_owned(), 10);
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
    let queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/media".to_owned(), 10);
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
                    DownloadRecord::queued(id.to_owned(), format!("https://example.test/{id}"), 10)
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
async fn host_aware_queue_selection_skips_saturated_hosts_and_claim_rechecks_eligibility() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();

    let mut same_host_first = DownloadRecord::queued(
        "same-host-first".to_owned(),
        "https://example.test/first".to_owned(),
        10,
    );
    same_host_first.priority = 1;
    let mut other_host_later = DownloadRecord::queued(
        "other-host-later".to_owned(),
        "https://other.test/later".to_owned(),
        12,
    );
    other_host_later.priority = 1;
    let same_host_second = DownloadRecord::queued(
        "same-host-second".to_owned(),
        "http://EXAMPLE.test:8080/second".to_owned(),
        11,
    );
    let other_host_earlier = DownloadRecord::queued(
        "other-host-earlier".to_owned(),
        "https://other.test/earlier".to_owned(),
        13,
    );
    store
        .save_downloads(vec![
            same_host_first,
            same_host_second,
            other_host_later,
            other_host_earlier,
        ])
        .await
        .unwrap();

    let active_hosts = HashMap::from([("example.test".to_owned(), 1)]);
    let next =
        store.next_queued_download_with_host_limit(active_hosts.clone(), 1).await.unwrap().unwrap();
    assert_eq!(next.id, "other-host-later");

    assert!(
        store
            .claim_queued_download_if_host_eligible("same-host-first", active_hosts.clone(), 1, 20,)
            .await
            .unwrap()
            .is_none()
    );

    let claimed = store
        .claim_queued_download_if_host_eligible("other-host-later", active_hosts.clone(), 1, 21)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.status, "Analyzing");

    let all_hosts_saturated =
        HashMap::from([("example.test".to_owned(), 1), ("other.test".to_owned(), 1)]);
    assert!(
        store.next_queued_download_with_host_limit(all_hosts_saturated, 1).await.unwrap().is_none()
    );
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn host_aware_queue_claim_rechecks_reordering_and_uses_empty_host_bucket() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    store
        .save_downloads(vec![
            DownloadRecord::queued("first".to_owned(), "https://example.test/first".to_owned(), 10),
            DownloadRecord::queued("second".to_owned(), "https://other.test/second".to_owned(), 11),
        ])
        .await
        .unwrap();

    let no_active_hosts = HashMap::new();
    assert_eq!(
        store
            .next_queued_download_with_host_limit(no_active_hosts.clone(), 1)
            .await
            .unwrap()
            .unwrap()
            .id,
        "first"
    );
    assert!(store.move_download("second", QueueMove::Top).await.unwrap());
    assert!(
        store
            .claim_queued_download_if_host_eligible("first", no_active_hosts.clone(), 1, 20)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .claim_queued_download_if_host_eligible("second", no_active_hosts, 1, 21)
            .await
            .unwrap()
            .unwrap()
            .status,
        "Analyzing"
    );
    assert!(store.cancel_queued_download("first", 22).await.unwrap());

    let invalid_host =
        DownloadRecord::queued("invalid-host".to_owned(), "not a URL".to_owned(), 23);
    store.save_download(invalid_host).await.unwrap();
    let empty_host_saturated = HashMap::from([(String::new(), 1)]);
    assert!(
        store
            .next_queued_download_with_host_limit(empty_host_saturated, 1)
            .await
            .unwrap()
            .is_none()
    );
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn worker_claim_wins_over_cancel_and_returns_the_claimed_record() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/media".to_owned(), 10);
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
    let mut completed =
        DownloadRecord::queued("completed".to_owned(), "https://example.test/media".to_owned(), 10);
    completed.status = "Completed".to_owned();
    store.save_download(completed).await.unwrap();

    assert!(!store.cancel_queued_download("missing", 20).await.unwrap());
    assert!(store.claim_queued_download_if_next("missing", 20).await.unwrap().is_none());
    assert!(!store.cancel_queued_download("completed", 21).await.unwrap());
    assert!(store.claim_queued_download_if_next("completed", 21).await.unwrap().is_none());
    assert_eq!(store.list_downloads().await.unwrap()[0].status, "Completed");
    std::fs::remove_dir_all(temp_dir).unwrap();
}
