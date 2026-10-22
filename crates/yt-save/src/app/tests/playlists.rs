use super::*;

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
fn auto_queue_records_snapshot_current_download_choices_and_folder() {
    let entries = vec![
        PlaylistSubscriptionEntryInput {
            media_key: "example:first".to_owned(),
            media_id: Some("first".to_owned()),
            media_url: "https://example.test/first".to_owned(),
            title: "First item".to_owned(),
            playlist_index: Some(1),
            available: true,
        },
        PlaylistSubscriptionEntryInput {
            media_key: "example:unavailable".to_owned(),
            media_id: Some("unavailable".to_owned()),
            media_url: String::new(),
            title: "Unavailable item".to_owned(),
            playlist_index: Some(2),
            available: false,
        },
    ];
    let presets = DownloadPresets {
        mode: DownloadMode::AudioOnly,
        audio_format: crate::model::AudioFormat::Opus,
        audio_quality: crate::model::AudioQuality::K192,
        video_quality: crate::model::VideoQuality::P720,
        download_speed_limit_bytes_per_second: 2_000_000,
    };

    let items = playlist_subscription_auto_queue_items(
        "Example extractor",
        "Example playlist",
        &entries,
        presets,
        "C:\\Media\\Current",
        123,
    );

    assert_eq!(items.len(), 1);
    let record = &items[0].record;
    assert_eq!(items[0].media_key, "example:first");
    assert_eq!(record.url, "https://example.test/first");
    assert_eq!(record.title, "First item");
    assert_eq!(record.source, "Example extractor");
    assert_eq!(record.status, "Queued");
    assert_eq!(record.created_at, 123);
    assert_eq!(record.download_mode, DownloadMode::AudioOnly);
    assert_eq!(record.audio_format, crate::model::AudioFormat::Opus);
    assert_eq!(record.audio_quality, crate::model::AudioQuality::K192);
    assert_eq!(record.video_quality, crate::model::VideoQuality::P720);
    assert_eq!(record.download_speed_limit_bytes_per_second, 2_000_000);
    assert_eq!(record.download_directory, "C:\\Media\\Current");
    assert_eq!(record.playlist_group_title.as_deref(), Some("Example playlist"));
    assert!(record.playlist_group_id.is_some());
}

#[test]
fn subscription_playlist_folder_names_are_safe_and_disambiguated() {
    let first = playlist_folder_component("../CON: clips. ", "abc12345-more");
    assert!(!first.chars().any(|character| matches!(character, '/' | '\\' | ':')));
    assert!(first.ends_with("[abc12345more]"));
    assert_eq!(playlist_folder_component("...", "subscription-2"), "Playlist [subscription2]");
    assert_ne!(
        playlist_folder_component("Same title", "alpha-one"),
        playlist_folder_component("Same title", "beta-two")
    );
}

#[test]
fn auto_queue_status_reports_queued_and_review_entries_separately() {
    assert_eq!(
        playlist_subscription_check_status(PlaylistSubscriptionCheckResult {
            new_entries: 0,
            auto_queued_entries: 0,
            auto_queue_skipped_for_subtitle_mode: false,
        }),
        "Playlist checked. No new entries were found."
    );
    assert!(
        playlist_subscription_check_status(PlaylistSubscriptionCheckResult {
            new_entries: 3,
            auto_queued_entries: 0,
            auto_queue_skipped_for_subtitle_mode: false,
        })
        .contains("Review them before queueing")
    );
    let mixed = playlist_subscription_check_status(PlaylistSubscriptionCheckResult {
        new_entries: 3,
        auto_queued_entries: 2,
        auto_queue_skipped_for_subtitle_mode: false,
    });
    assert!(mixed.contains("auto-queued 2 available entries"));
    assert!(mixed.contains("Unavailable entries remain for review"));

    let subtitles_only = playlist_subscription_check_status(PlaylistSubscriptionCheckResult {
        new_entries: 3,
        auto_queued_entries: 0,
        auto_queue_skipped_for_subtitle_mode: true,
    });
    assert!(subtitles_only.contains("remain in review"));
    assert!(subtitles_only.contains("subtitle-only mode"));
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
