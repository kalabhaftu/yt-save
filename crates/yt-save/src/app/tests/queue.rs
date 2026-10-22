use super::*;

#[test]
fn active_host_counts_release_on_worker_exit() {
    let counts: ActiveHostCounts = Arc::new(Mutex::new(HashMap::new()));
    let first = ActiveHostRegistration::register("media.example".to_owned(), counts.clone());
    let second = ActiveHostRegistration::register("media.example".to_owned(), counts.clone());
    let other = ActiveHostRegistration::register("other.example".to_owned(), counts.clone());

    assert_eq!(active_host_counts_snapshot(&counts).get("media.example"), Some(&2));
    assert_eq!(active_host_counts_snapshot(&counts).get("other.example"), Some(&1));

    drop(first);
    assert_eq!(active_host_counts_snapshot(&counts).get("media.example"), Some(&1));
    drop(second);
    drop(other);
    assert!(active_host_counts_snapshot(&counts).is_empty());
}

#[test]
fn automatic_retry_backoff_doubles_and_caps_at_four_minutes() {
    assert_eq!(automatic_retry_delay_secs(0), 15);
    assert_eq!(automatic_retry_delay_secs(1), 30);
    assert_eq!(automatic_retry_delay_secs(2), 60);
    assert_eq!(automatic_retry_delay_secs(4), AUTO_RETRY_MAX_DELAY_SECS);
    assert_eq!(automatic_retry_delay_secs(20), AUTO_RETRY_MAX_DELAY_SECS);
    assert_eq!(automatic_retry_delay_secs(u32::MAX), AUTO_RETRY_MAX_DELAY_SECS);
}

#[tokio::test]
async fn orderly_shutdown_keeps_retry_waits_but_pause_all_pauses_them() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let mut retry = DownloadRecord::queued(
        "retry-wait".to_owned(),
        "https://example.test/media".to_owned(),
        10,
    );
    retry.status = "Waiting for retry".to_owned();
    retry.retry_attempts = 1;
    retry.next_retry_at = Some(50);
    retry.error = Some("A temporary network error interrupted the request.".to_owned());
    store.save_download(retry).await.unwrap();
    let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::new()));

    assert_eq!(pause_all_downloads(&store, &active_jobs, false).await.unwrap().0, 0);
    let waiting = store.list_downloads().await.unwrap().remove(0);
    assert_eq!(waiting.status, "Waiting for retry");
    assert_eq!(waiting.next_retry_at, Some(50));

    assert_eq!(pause_all_downloads(&store, &active_jobs, true).await.unwrap().0, 1);
    let paused = store.list_downloads().await.unwrap().remove(0);
    assert_eq!(paused.status, "Paused");
    assert_eq!(paused.next_retry_at, None);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn scheduled_items_stay_waiting_on_shutdown_and_pause_explicitly() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let deadline = now() + 3600;
    let mut scheduled = DownloadRecord::queued(
        "scheduled-job".to_owned(),
        "https://example.test/media".to_owned(),
        now(),
    );
    scheduled.status = "Waiting for schedule".to_owned();
    scheduled.scheduled_at = Some(deadline);
    store.save_download(scheduled).await.unwrap();
    let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::new()));

    assert_eq!(pause_all_downloads(&store, &active_jobs, false).await.unwrap().0, 0);
    let waiting = store.list_downloads().await.unwrap().remove(0);
    assert_eq!(waiting.status, "Waiting for schedule");
    assert_eq!(waiting.scheduled_at, Some(deadline));

    assert_eq!(pause_all_downloads(&store, &active_jobs, true).await.unwrap().0, 1);
    let paused = store.list_downloads().await.unwrap().remove(0);
    assert_eq!(paused.status, "Paused");
    assert_eq!(paused.scheduled_at, Some(deadline));

    let resumed = store.resume_download("scheduled-job", now()).await.unwrap().unwrap();
    assert_eq!(resumed.status, "Waiting for schedule");
    assert_eq!(resumed.scheduled_at, Some(deadline));
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn cancel_all_cancels_pending_rows_and_signals_active_workers() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/queued".to_owned(), 10);
    let mut retry_wait = DownloadRecord::queued(
        "retry-wait".to_owned(),
        "https://example.test/retry".to_owned(),
        11,
    );
    retry_wait.status = "Waiting for retry".to_owned();
    let mut paused =
        DownloadRecord::queued("paused".to_owned(), "https://example.test/paused".to_owned(), 12);
    paused.status = "Paused".to_owned();
    let mut active =
        DownloadRecord::queued("active".to_owned(), "https://example.test/active".to_owned(), 13);
    active.status = "Downloading".to_owned();
    let mut failed =
        DownloadRecord::queued("failed".to_owned(), "https://example.test/failed".to_owned(), 14);
    failed.status = "Failed".to_owned();
    let mut completed = DownloadRecord::queued(
        "completed".to_owned(),
        "https://example.test/completed".to_owned(),
        15,
    );
    completed.status = "Completed".to_owned();
    store
        .save_downloads(vec![queued, retry_wait, paused, active, failed, completed])
        .await
        .unwrap();

    let (active_sender, active_signal) = watch::channel(JobSignal::Continue);
    let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::from([(
        "active".to_owned(),
        Arc::new(active_sender),
    )])));

    assert_eq!(cancel_all_downloads(&store, &active_jobs).await.unwrap(), (3, 1));
    assert_eq!(*active_signal.borrow(), JobSignal::Cancel);
    let rows = store.list_downloads().await.unwrap();
    for id in ["queued", "retry-wait", "paused"] {
        assert_eq!(rows.iter().find(|row| row.id == id).unwrap().status, "Canceled");
    }
    assert_eq!(rows.iter().find(|row| row.id == "active").unwrap().status, "Canceling");
    assert_eq!(rows.iter().find(|row| row.id == "failed").unwrap().status, "Failed");
    assert_eq!(rows.iter().find(|row| row.id == "completed").unwrap().status, "Completed");
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn retry_all_failed_requeues_only_failed_and_interrupted_rows() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let mut failed =
        DownloadRecord::queued("failed".to_owned(), "https://example.test/failed".to_owned(), 10);
    failed.status = "Failed".to_owned();
    failed.error = Some("Temporary failure".to_owned());
    let mut interrupted = DownloadRecord::queued(
        "interrupted".to_owned(),
        "https://example.test/interrupted".to_owned(),
        11,
    );
    interrupted.status = "Interrupted".to_owned();
    let mut canceled = DownloadRecord::queued(
        "canceled".to_owned(),
        "https://example.test/canceled".to_owned(),
        12,
    );
    canceled.status = "Canceled".to_owned();
    let mut completed = DownloadRecord::queued(
        "completed".to_owned(),
        "https://example.test/completed".to_owned(),
        13,
    );
    completed.status = "Completed".to_owned();
    store.save_downloads(vec![failed, interrupted, canceled, completed]).await.unwrap();

    assert_eq!(retry_all_failed_downloads(&store).await.unwrap(), 2);
    let rows = store.list_downloads().await.unwrap();
    for id in ["failed", "interrupted"] {
        let row = rows.iter().find(|row| row.id == id).unwrap();
        assert_eq!(row.status, "Queued");
        assert_eq!(row.error, None);
    }
    assert_eq!(rows.iter().find(|row| row.id == "canceled").unwrap().status, "Canceled");
    assert_eq!(rows.iter().find(|row| row.id == "completed").unwrap().status, "Completed");
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn dispatcher_capacity_tracks_raised_and_lowered_limits_without_stopping_workers() {
    assert!(can_dispatch_more(1, 3));
    assert!(!can_dispatch_more(3, 2));
    assert!(!can_dispatch_more(2, 2));
    assert!(can_dispatch_more(1, 2));
    assert!(!can_dispatch_more(1, 1));
}

#[tokio::test]
async fn shutdown_worker_drain_resumes_when_shutdown_is_rolled_back() {
    let shutdown_started = Arc::new(AtomicBool::new(true));
    let queue_wake = Arc::new(Notify::new());
    let mut workers = JoinSet::new();
    workers.spawn(async { std::future::pending::<()>().await });

    let rollback_shutdown = shutdown_started.clone();
    let rollback_wake = queue_wake.clone();
    let rollback = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        rollback_shutdown.store(false, Ordering::Release);
        rollback_wake.notify_one();
    });

    let shutdown_committed = AtomicBool::new(false);
    let should_exit = tokio::time::timeout(
        Duration::from_secs(1),
        drain_workers_during_shutdown(
            &mut workers,
            &queue_wake,
            &shutdown_started,
            &shutdown_committed,
        ),
    )
    .await
    .expect("the shutdown rollback should wake the dispatcher");
    rollback.await.unwrap();

    assert!(!should_exit);
    assert_eq!(workers.len(), 1, "an active worker must not be discarded");
    workers.abort_all();
    while workers.join_next().await.is_some() {}
}

#[tokio::test]
async fn shutdown_worker_drain_waits_for_rollback_after_last_worker_finishes() {
    let shutdown_started = Arc::new(AtomicBool::new(true));
    let queue_wake = Arc::new(Notify::new());
    let mut workers = JoinSet::new();
    workers.spawn(async { tokio::time::sleep(Duration::from_millis(10)).await });

    let rollback_shutdown = shutdown_started.clone();
    let rollback_wake = queue_wake.clone();
    let rollback = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        rollback_shutdown.store(false, Ordering::Release);
        rollback_wake.notify_one();
    });

    let shutdown_committed = AtomicBool::new(false);
    let should_exit = tokio::time::timeout(
        Duration::from_secs(1),
        drain_workers_during_shutdown(
            &mut workers,
            &queue_wake,
            &shutdown_started,
            &shutdown_committed,
        ),
    )
    .await
    .expect("the shutdown rollback should wake an empty dispatcher");
    rollback.await.unwrap();
    assert!(!should_exit);
    assert!(workers.is_empty());
}

#[tokio::test]
async fn shutdown_worker_drain_exits_after_shutdown_is_committed() {
    let shutdown_started = AtomicBool::new(true);
    let shutdown_committed = Arc::new(AtomicBool::new(false));
    let queue_wake = Arc::new(Notify::new());
    let mut workers = JoinSet::new();

    let commit = shutdown_committed.clone();
    let wake = queue_wake.clone();
    let commit_shutdown = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        commit.store(true, Ordering::Release);
        wake.notify_one();
    });

    let should_exit = tokio::time::timeout(
        Duration::from_secs(1),
        drain_workers_during_shutdown(
            &mut workers,
            &queue_wake,
            &shutdown_started,
            &shutdown_committed,
        ),
    )
    .await
    .expect("the committed shutdown should wake the dispatcher");
    commit_shutdown.await.unwrap();
    assert!(should_exit);
}
use crate::model::{AudioFormat, AudioQuality, VideoQuality};

#[test]
fn tray_summary_counts_only_active_and_paused_states() {
    assert!(is_active_status("Analyzing"));
    assert!(is_active_status("Downloading"));
    assert!(is_active_status("Post-processing"));
    assert!(is_active_status("Pausing"));
    assert!(is_active_status("Canceling"));
    assert!(!is_active_status("Queued"));
    assert!(!is_active_status("Paused"));
    assert!(!is_active_status("Completed"));
    assert_eq!(tray_tooltip(0, 0), "Yt Save");
    assert_eq!(tray_tooltip(1, 2), "Yt Save — 1 active, 2 paused");
}

#[test]
fn queue_action_state_enables_only_actions_with_matching_rows() {
    assert_eq!(queue_action_state(&[]), QueueActionState::default());

    let mut queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/queued".to_owned(), 10);
    let mut paused =
        DownloadRecord::queued("paused".to_owned(), "https://example.test/paused".to_owned(), 11);
    paused.status = "Paused".to_owned();
    let mut failed =
        DownloadRecord::queued("failed".to_owned(), "https://example.test/failed".to_owned(), 12);
    failed.status = "Failed".to_owned();

    assert_eq!(
        queue_action_state(&[queued.clone(), paused.clone(), failed.clone()]),
        QueueActionState {
            can_pause_all: true,
            can_resume_all: true,
            can_cancel_all: true,
            can_retry_failed: true,
        }
    );

    queued.status = "Completed".to_owned();
    paused.status = "Completed".to_owned();
    failed.status = "Canceled".to_owned();
    assert_eq!(queue_action_state(&[queued, paused, failed]), QueueActionState::default());
}

#[test]
fn queue_rows_show_execution_positions_and_only_report_known_remaining_sizes() {
    let mut first =
        DownloadRecord::queued("first".to_owned(), "https://example.test/first".to_owned(), 10);
    first.total_bytes = Some(1_000);
    first.downloaded_bytes = Some(200);
    let mut paused =
        DownloadRecord::queued("paused".to_owned(), "https://example.test/paused".to_owned(), 11);
    paused.status = "Paused".to_owned();
    let second =
        DownloadRecord::queued("second".to_owned(), "https://example.test/second".to_owned(), 12);

    let rows = row_views(&[first.clone(), paused.clone(), second.clone()]);
    assert_eq!(rows[0].queue_position.as_str(), "#1");
    assert_eq!(rows[1].queue_position.as_str(), "Paused");
    assert_eq!(rows[2].queue_position.as_str(), "#2");
    assert_eq!(
        queue_summary_text(&[first, paused, second]),
        "2 queued · 1 paused · 800.0 B known; 2 sizes unknown"
    );
    assert_eq!(queue_summary_text(&[]), "No pending work");
}

#[test]
fn row_view_shows_terminal_error_before_saved_progress() {
    let mut record =
        DownloadRecord::queued("canceling".to_owned(), "https://example.test/media".to_owned(), 10);
    record.status = "Failed".to_owned();
    record.error = Some(
        "Check that no media process is still using the partial file before retrying.".to_owned(),
    );
    record.downloaded_bytes = Some(22);
    record.total_bytes = Some(100);

    let row = row_view_with_queue_position(&record, "", "", "");
    assert!(row.detail.as_str().contains(record.error.as_deref().unwrap()));
    assert!(row.detail.as_str().starts_with("Video · "));
}

#[tokio::test]
async fn queue_worker_downloads_fixture_and_persists_completed_media() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-e2e-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let fixture_source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("fake_yt_dlp.rs");
    let fake_yt_dlp = temp_dir.join("yt-dlp.exe");
    let compilation = Command::new("rustc")
        .arg("--edition=2024")
        .arg(&fixture_source)
        .arg("-o")
        .arg(&fake_yt_dlp)
        .output()
        .expect("launch rustc for the deterministic yt-dlp fixture");
    assert!(
        compilation.status.success(),
        "could not compile fake yt-dlp: {}",
        String::from_utf8_lossy(&compilation.stderr)
    );

    let output_directory = temp_dir.join("downloads");
    std::fs::create_dir_all(&output_directory).unwrap();
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let mut queued = DownloadRecord::queued(
        "fixture-job".to_owned(),
        "https://example.test/media".to_owned(),
        10,
    );
    queued.download_directory = output_directory.to_string_lossy().into_owned();
    store.save_download(queued).await.unwrap();
    let active = store
        .claim_queued_download_if_next("fixture-job", 11)
        .await
        .unwrap()
        .expect("fixture job should be claimed");

    let (signal_sender, signal_receiver) = watch::channel(JobSignal::Continue);
    let signal_sender = Arc::new(signal_sender);
    let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::from([(
        active.id.clone(),
        signal_sender.clone(),
    )])));
    let registration =
        ActiveJobRegistration { id: active.id.clone(), jobs: active_jobs, sender: signal_sender };
    std::fs::write(temp_dir.join("ffmpeg.exe"), b"fixture ffmpeg placeholder").unwrap();
    let dependencies = DependencyPaths {
        directory: temp_dir.clone(),
        yt_dlp: fake_yt_dlp.clone(),
        deno: temp_dir.join("deno.exe"),
        ffmpeg: temp_dir.join("ffmpeg.exe"),
        ffprobe: temp_dir.join("ffprobe.exe"),
    };
    let engine = DownloadEngine::new(dependencies, output_directory.clone());
    let (events, _receiver) = mpsc::unbounded_channel();
    run_download_job(active, store.clone(), engine, events, signal_receiver, registration).await;

    let completed = store
        .list_downloads()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.id == "fixture-job")
        .unwrap();
    let expected_path = output_directory.join("Fixture media [fixture-123].mp4");
    assert_eq!(completed.status, "Completed");
    assert_eq!(completed.title, "Fixture media");
    assert_eq!(completed.source, "Fixture");
    assert_eq!(completed.progress, 100.0);
    assert_eq!(completed.output_path.as_deref(), Some(expected_path.to_str().unwrap()));
    assert_eq!(std::fs::read(expected_path).unwrap(), b"tiny deterministic media fixture");

    let mut subtitle_queued = DownloadRecord::queued(
        "fixture-subtitle-job".to_owned(),
        "https://example.test/media-with-captions".to_owned(),
        12,
    );
    subtitle_queued.download_directory = output_directory.to_string_lossy().into_owned();
    subtitle_queued.download_mode = DownloadMode::SubtitlesOnly;
    subtitle_queued.subtitle_languages = "en".to_owned();
    subtitle_queued.subtitle_format = SubtitleFormat::Srt;
    subtitle_queued.include_auto_subtitles = true;
    store.save_download(subtitle_queued).await.unwrap();
    let subtitle_active = store
        .claim_queued_download_if_next("fixture-subtitle-job", 13)
        .await
        .unwrap()
        .expect("subtitle fixture job should be claimed");
    let (subtitle_sender, subtitle_receiver) = watch::channel(JobSignal::Continue);
    let subtitle_sender = Arc::new(subtitle_sender);
    let subtitle_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::from([(
        subtitle_active.id.clone(),
        subtitle_sender.clone(),
    )])));
    let subtitle_registration = ActiveJobRegistration {
        id: subtitle_active.id.clone(),
        jobs: subtitle_jobs,
        sender: subtitle_sender,
    };
    let subtitle_dependencies = DependencyPaths {
        directory: temp_dir.clone(),
        yt_dlp: fake_yt_dlp,
        deno: temp_dir.join("deno.exe"),
        ffmpeg: temp_dir.join("ffmpeg.exe"),
        ffprobe: temp_dir.join("ffprobe.exe"),
    };
    let subtitle_engine = DownloadEngine::new(subtitle_dependencies, output_directory.clone());
    let (subtitle_events, _subtitle_event_receiver) = mpsc::unbounded_channel();
    run_download_job(
        subtitle_active,
        store.clone(),
        subtitle_engine,
        subtitle_events,
        subtitle_receiver,
        subtitle_registration,
    )
    .await;

    let subtitle_completed = store
        .list_downloads()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.id == "fixture-subtitle-job")
        .unwrap();
    let expected_subtitle_path = output_directory.join("Fixture media [fixture-123].en.srt");
    assert_eq!(subtitle_completed.status, "Completed");
    assert_eq!(subtitle_completed.download_mode, DownloadMode::SubtitlesOnly);
    assert_eq!(subtitle_completed.subtitle_languages, "en");
    assert_eq!(subtitle_completed.subtitle_format, SubtitleFormat::Srt);
    assert!(subtitle_completed.include_auto_subtitles);
    assert_eq!(
        subtitle_completed.output_path.as_deref(),
        Some(expected_subtitle_path.to_str().unwrap())
    );
    assert_eq!(
        std::fs::read(expected_subtitle_path).unwrap(),
        b"1\n00:00:00,000 --> 00:00:01,000\nfixture subtitle\n"
    );
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
#[ignore = "requires packaged Windows media tools and live YouTube access"]
async fn packaged_yt_dlp_deno_ffmpeg_complete_a_youtube_queue_job() {
    let package_bin = std::env::var_os("YT_SAVE_DEPENDENCY_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("..")
                    .join("..")
                    .join("target")
                    .join("package")
                    .join("bin")
            })
            .canonicalize()
            .expect("build package dependencies or set YT_SAVE_DEPENDENCY_DIR before running this acceptance test");
    for dependency in ["yt-dlp.exe", "deno.exe", "ffmpeg.exe", "ffprobe.exe"] {
        assert!(
            package_bin.join(dependency).is_file(),
            "packaged dependency is missing: {}",
            package_bin.join(dependency).display()
        );
    }
    let temp_dir = std::env::temp_dir().join(format!("yt-save-live-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let output_directory = temp_dir.join("downloads");
    std::fs::create_dir_all(&output_directory).unwrap();
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let mut queued = DownloadRecord::queued(
        "youtube-live-job".to_owned(),
        "https://www.youtube.com/watch?v=jNQXAC9IVRw".to_owned(),
        now(),
    );
    queued.download_directory = output_directory.to_string_lossy().into_owned();
    queued.video_quality = crate::model::VideoQuality::Lowest;
    store.save_download(queued).await.unwrap();
    let active = store
        .claim_queued_download_if_next("youtube-live-job", now())
        .await
        .unwrap()
        .expect("YouTube queue job should be claimed");

    let (signal_sender, signal_receiver) = watch::channel(JobSignal::Continue);
    let signal_sender = Arc::new(signal_sender);
    let active_jobs: ActiveJobs = Arc::new(Mutex::new(std::collections::HashMap::from([(
        active.id.clone(),
        signal_sender.clone(),
    )])));
    let registration =
        ActiveJobRegistration { id: active.id.clone(), jobs: active_jobs, sender: signal_sender };
    let dependencies = DependencyPaths {
        directory: package_bin.clone(),
        yt_dlp: package_bin.join("yt-dlp.exe"),
        deno: package_bin.join("deno.exe"),
        ffmpeg: package_bin.join("ffmpeg.exe"),
        ffprobe: package_bin.join("ffprobe.exe"),
    };
    let engine = DownloadEngine::new(dependencies, output_directory);
    let (events, _receiver) = mpsc::unbounded_channel();
    run_download_job(active, store.clone(), engine, events, signal_receiver, registration).await;

    let completed = store
        .list_downloads()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.id == "youtube-live-job")
        .unwrap();
    assert_eq!(completed.status, "Completed", "{:?}", completed.error);
    assert!(completed.title.contains("zoo"), "unexpected title: {}", completed.title);
    let output_path = PathBuf::from(completed.output_path.as_deref().unwrap());
    assert!(output_path.is_file(), "missing downloaded file: {}", output_path.display());
    assert!(std::fs::metadata(&output_path).unwrap().len() > 0);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn retry_wait_has_a_visible_deadline_and_counts_as_pending_work() {
    let mut record =
        DownloadRecord::queued("retrying".to_owned(), "https://example.test/media".to_owned(), 10);
    record.status = "Waiting for retry".to_owned();
    record.retry_attempts = 2;
    record.next_retry_at = Some(1_700_000_000);
    record.error = Some("A temporary network issue interrupted this media request.".to_owned());

    let row = row_view_with_queue_position(&record, "Retry", "", "");
    assert_eq!(row.status.as_str(), "Waiting for retry");
    assert_eq!(row.queue_position.as_str(), "Retry");
    assert!(row.detail.as_str().contains("Retry 2 scheduled for"));
    assert!(queue_summary_text(&[record]).contains("1 waiting for retry"));
}

#[test]
fn scheduled_queue_rows_show_local_start_time_and_bulk_actions() {
    let mut record = DownloadRecord::queued(
        "scheduled".to_owned(),
        "https://example.test/media".to_owned(),
        now(),
    );
    record.title = "Example media title".to_owned();
    record.status = "Waiting for schedule".to_owned();
    record.scheduled_at = Some(now() + 3600);

    let row = row_view_with_queue_position(&record, "Scheduled", "", "");
    assert_eq!(row.title.as_str(), "Example media title");
    assert_eq!(row.status.as_str(), "Waiting for schedule");
    assert_eq!(row.queue_position.as_str(), "Scheduled");
    assert!(row.detail.as_str().contains("local time"));
    assert!(row.detail.as_str().contains("app must be running"));
    assert!(queue_summary_text(&[record.clone()]).contains("1 scheduled"));
    let actions = queue_action_state(&[record.clone()]);
    assert!(actions.can_pause_all);
    assert!(actions.can_cancel_all);
    record.status = "Paused".to_owned();
    let paused_row = row_view_with_queue_position(&record, "Paused", "", "");
    assert!(paused_row.detail.as_str().contains("Paused · planned for"));
    assert!(paused_row.detail.as_str().contains("resume manually"));
    assert!(!paused_row.detail.as_str().contains("Resumes"));
}

#[tokio::test]
async fn startup_requeues_saved_and_interrupted_downloads() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
    let store = Store::open(temp_dir.join("state.sqlite3")).unwrap();
    let saved_presets = DownloadPresets {
        mode: DownloadMode::VideoOnly,
        audio_format: AudioFormat::Flac,
        audio_quality: AudioQuality::K320,
        video_quality: VideoQuality::P1440,
        download_speed_limit_bytes_per_second: 2_000_000,
    };
    store
        .set_download_speed_limit(saved_presets.download_speed_limit_bytes_per_second, 1)
        .await
        .unwrap();
    store.set_download_presets(saved_presets, 1).await.unwrap();
    let queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/queued".to_owned(), 10);
    let mut active =
        DownloadRecord::queued("active".to_owned(), "https://example.test/active".to_owned(), 20);
    active.status = "Downloading".to_owned();
    active.playlist_index = Some(3);
    active.download_mode = DownloadMode::VideoOnly;
    active.audio_format = AudioFormat::Opus;
    active.audio_quality = AudioQuality::K192;
    active.video_quality = VideoQuality::P720;
    active.download_directory = "C:\\Media\\Archive".to_owned();
    active.downloaded_bytes = Some(75);
    active.total_bytes = Some(100);
    let mut later_playlist_item = DownloadRecord::queued(
        "selected-later".to_owned(),
        "https://example.test/playlist".to_owned(),
        30,
    );
    later_playlist_item.playlist_index = Some(9);
    let mut earlier_playlist_item = DownloadRecord::queued(
        "selected-earlier".to_owned(),
        "https://example.test/playlist".to_owned(),
        30,
    );
    earlier_playlist_item.playlist_index = Some(2);
    let mut canceling = DownloadRecord::queued(
        "canceling".to_owned(),
        "https://example.test/canceling".to_owned(),
        40,
    );
    canceling.status = "Canceling".to_owned();
    canceling.progress = 22.0;
    canceling.downloaded_bytes = Some(22);
    canceling.total_bytes = Some(100);
    let mut pausing =
        DownloadRecord::queued("pausing".to_owned(), "https://example.test/pausing".to_owned(), 41);
    pausing.status = "Pausing".to_owned();
    pausing.downloaded_bytes = Some(15);
    let mut paused =
        DownloadRecord::queued("paused".to_owned(), "https://example.test/paused".to_owned(), 42);
    paused.status = "Paused".to_owned();
    paused.progress = 51.0;
    paused.downloaded_bytes = Some(510);
    store.save_download(queued.clone()).await.unwrap();
    store.save_download(active.clone()).await.unwrap();
    store.save_download(later_playlist_item).await.unwrap();
    store.save_download(earlier_playlist_item).await.unwrap();
    store.save_download(canceling).await.unwrap();
    store.save_download(pausing).await.unwrap();
    store.save_download(paused).await.unwrap();

    let pending = recover_pending(&store).await.unwrap();
    assert_eq!(store.load_download_presets().await.unwrap(), saved_presets);
    assert_eq!(pending.len(), 4);
    assert!(pending.iter().all(|record| record.status == "Queued"));
    assert_eq!(
        pending.iter().map(|record| record.id.as_str()).collect::<Vec<_>>(),
        ["queued", "active", "selected-later", "selected-earlier"]
    );
    let rows = store.list_downloads().await.unwrap();
    let recovered_active = rows.iter().find(|record| record.id == "active").unwrap();
    assert_eq!(recovered_active.status, "Queued");
    assert_eq!(recovered_active.downloaded_bytes, Some(75));
    assert_eq!(recovered_active.playlist_index, Some(3));
    assert_eq!(recovered_active.download_mode, DownloadMode::VideoOnly);
    assert_eq!(recovered_active.audio_format, AudioFormat::Opus);
    assert_eq!(recovered_active.audio_quality, AudioQuality::K192);
    assert_eq!(recovered_active.video_quality, VideoQuality::P720);
    assert_eq!(recovered_active.download_directory, "C:\\Media\\Archive");
    let recovered_canceling = rows.iter().find(|record| record.id == "canceling").unwrap();
    assert_eq!(recovered_canceling.status, "Failed");
    assert_eq!(recovered_canceling.progress, 22.0);
    assert_eq!(recovered_canceling.downloaded_bytes, Some(22));
    assert!(recovered_canceling.error.as_deref().unwrap().contains("process-tree cancellation"));
    assert!(!pending.iter().any(|record| record.id == "canceling"));
    let recovered_pausing = rows.iter().find(|record| record.id == "pausing").unwrap();
    assert_eq!(recovered_pausing.status, "Failed");
    assert_eq!(recovered_pausing.downloaded_bytes, Some(15));
    assert!(recovered_pausing.error.as_deref().unwrap().contains("process-tree pause"));
    let recovered_paused = rows.iter().find(|record| record.id == "paused").unwrap();
    assert_eq!(recovered_paused.status, "Paused");
    assert_eq!(recovered_paused.progress, 51.0);
    assert_eq!(recovered_paused.downloaded_bytes, Some(510));
    assert!(!pending.iter().any(|record| record.id == "paused"));
    std::fs::remove_dir_all(temp_dir).unwrap();
}
