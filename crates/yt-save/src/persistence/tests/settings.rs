use super::super::*;

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
async fn max_concurrent_per_host_defaults_bounds_and_persists() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let db_path = temp_dir.join("state.sqlite3");
    let store = Store::open(db_path.clone()).unwrap();

    assert_eq!(
        store.load_max_concurrent_per_host().await.unwrap(),
        DEFAULT_MAX_CONCURRENT_PER_HOST
    );
    store.set_max_concurrent_per_host(6, 11).await.unwrap();
    assert_eq!(store.load_max_concurrent_per_host().await.unwrap(), 6);
    store.set_max_concurrent_per_host(usize::MAX, 12).await.unwrap();
    assert_eq!(store.load_max_concurrent_per_host().await.unwrap(), MAX_CONCURRENT_PER_HOST);
    store.set_max_concurrent_per_host(0, 13).await.unwrap();
    assert_eq!(
        store.load_max_concurrent_per_host().await.unwrap(),
        DEFAULT_MAX_CONCURRENT_PER_HOST
    );
    store.set_max_concurrent_per_host(5, 14).await.unwrap();
    drop(store);

    let store = Store::open(db_path.clone()).unwrap();
    assert_eq!(store.load_max_concurrent_per_host().await.unwrap(), 5);
    store
        .with_connection(|connection| {
            connection.execute(
                "UPDATE app_settings SET value = 'invalid' WHERE key = 'max_concurrent_per_host'",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        store.load_max_concurrent_per_host().await.unwrap(),
        DEFAULT_MAX_CONCURRENT_PER_HOST
    );
    drop(store);

    let reopened = Store::open(db_path).unwrap();
    assert_eq!(
        reopened.load_max_concurrent_per_host().await.unwrap(),
        DEFAULT_MAX_CONCURRENT_PER_HOST
    );
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
    assert_eq!(reopened.load_concurrent_fragments().await.unwrap(), DEFAULT_CONCURRENT_FRAGMENTS);
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
    assert!(over_limit.error.as_deref().unwrap_or_default().contains("retry limit was lowered"));

    assert_eq!(store.set_auto_retry_attempts(0, 61).await.unwrap(), 1);
    let records = store.list_downloads().await.unwrap();
    assert_eq!(records.iter().find(|record| record.id == "within-limit").unwrap().status, "Failed");
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
    let mut active = store.claim_queued_download_if_next("retry-job", 11).await.unwrap().unwrap();
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
async fn download_presets_round_trip_and_unknown_settings_fall_back_independently() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let db_path = temp_dir.join("state.sqlite");
    let store = Store::open(db_path.clone()).unwrap();
    assert_eq!(store.load_download_presets().await.unwrap(), DownloadPresets::default());
    let audio_presets = DownloadPresets {
        mode: DownloadMode::AudioOnly,
        audio_format: AudioFormat::Flac,
        audio_quality: AudioQuality::K192,
        video_quality: VideoQuality::P1440,
        download_speed_limit_bytes_per_second: 500_000,
    };
    store.set_download_presets(audio_presets, 10).await.unwrap();
    store.set_download_speed_limit(2_000_000, 15).await.unwrap();
    drop(store);

    let store = Store::open(db_path.clone()).unwrap();
    let audio_presets_with_speed =
        DownloadPresets { download_speed_limit_bytes_per_second: 2_000_000, ..audio_presets };
    assert_eq!(store.load_download_presets().await.unwrap(), audio_presets_with_speed);
    let presets = DownloadPresets {
        mode: DownloadMode::VideoOnly,
        audio_format: AudioFormat::Flac,
        audio_quality: AudioQuality::K192,
        video_quality: VideoQuality::P1440,
        download_speed_limit_bytes_per_second: 500_000,
    };
    store.set_download_presets(presets, 20).await.unwrap();
    drop(store);

    let reopened = Store::open(db_path).unwrap();
    assert_eq!(
        reopened.load_download_presets().await.unwrap(),
        DownloadPresets { download_speed_limit_bytes_per_second: 2_000_000, ..presets }
    );
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
                connection.execute(
                    "UPDATE app_settings SET value = '5000001' WHERE key = 'new_download_speed_limit_bytes_per_second'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
    let recovered = reopened.load_download_presets().await.unwrap();
    assert_eq!(recovered.mode, DownloadMode::VideoOnly);
    assert_eq!(recovered.audio_format, AudioFormat::Flac);
    assert_eq!(recovered.audio_quality, AudioQuality::Best);
    assert_eq!(recovered.video_quality, VideoQuality::Best);
    assert_eq!(recovered.download_speed_limit_bytes_per_second, 0);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn older_preset_save_does_not_overwrite_newer_speed_limit_or_job_snapshot() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let db_path = temp_dir.join("state.sqlite");
    let store = Store::open(db_path.clone()).unwrap();
    let newer_speed_limit = 10_000_000;
    let queued_speed_limit = 2_000_000;
    store.set_download_speed_limit(newer_speed_limit, 20).await.unwrap();

    let older_enqueue_presets = DownloadPresets {
        mode: DownloadMode::AudioOnly,
        audio_format: AudioFormat::Mp3,
        audio_quality: AudioQuality::K192,
        video_quality: VideoQuality::P720,
        download_speed_limit_bytes_per_second: queued_speed_limit,
    };
    store.set_download_presets(older_enqueue_presets, 10).await.unwrap();

    let mut queued = DownloadRecord::queued(
        "older-speed-snapshot".to_owned(),
        "https://example.test/media".to_owned(),
        30,
    );
    queued.download_speed_limit_bytes_per_second = queued_speed_limit;
    store.save_download(queued).await.unwrap();

    let expected_current_presets = DownloadPresets {
        download_speed_limit_bytes_per_second: newer_speed_limit,
        ..older_enqueue_presets
    };
    assert_eq!(store.load_download_presets().await.unwrap(), expected_current_presets);
    let saved_job = store
        .list_downloads()
        .await
        .unwrap()
        .into_iter()
        .find(|job| job.id == "older-speed-snapshot")
        .unwrap();
    assert_eq!(saved_job.download_speed_limit_bytes_per_second, queued_speed_limit);
    drop(store);

    let reopened = Store::open(db_path).unwrap();
    assert_eq!(reopened.load_download_presets().await.unwrap(), expected_current_presets);
    let reopened_job = reopened
        .list_downloads()
        .await
        .unwrap()
        .into_iter()
        .find(|job| job.id == "older-speed-snapshot")
        .unwrap();
    assert_eq!(reopened_job.download_speed_limit_bytes_per_second, queued_speed_limit);
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
async fn application_update_channel_defaults_to_stable_and_persists_preview_choice() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let db_path = temp_dir.join("state.sqlite");
    let store = Store::open(db_path.clone()).unwrap();
    assert_eq!(store.load_app_update_channel().await.unwrap(), AppUpdateChannel::Stable);

    store.set_app_update_channel(AppUpdateChannel::Preview, 10).await.unwrap();
    assert_eq!(store.load_app_update_channel().await.unwrap(), AppUpdateChannel::Preview);
    drop(store);

    let reopened = Store::open(db_path).unwrap();
    assert_eq!(reopened.load_app_update_channel().await.unwrap(), AppUpdateChannel::Preview);
    reopened
        .with_connection(|connection| {
            connection.execute(
                "UPDATE app_settings SET value = 'unknown' WHERE key = 'app_update_channel'",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(reopened.load_app_update_channel().await.unwrap(), AppUpdateChannel::Stable);
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[tokio::test]
async fn tray_preferences_default_off_and_persist_independently() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", uuid::Uuid::new_v4()));
    let db_path = temp_dir.join("state.sqlite");
    let store = Store::open(db_path.clone()).unwrap();
    assert_eq!(store.load_tray_preferences().await.unwrap(), TrayPreferences::default());

    store
        .set_tray_preferences(TrayPreferences { close_to_tray: true, minimize_to_tray: false }, 10)
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
        download_speed_limit_bytes_per_second: 5_000_000,
    };
    let mut job =
        DownloadRecord::queued("older-job".to_owned(), "https://example.test/media".to_owned(), 10);
    job.status = "Failed".to_owned();
    job.download_mode = job_presets.mode;
    job.audio_format = job_presets.audio_format;
    job.audio_quality = job_presets.audio_quality;
    job.video_quality = job_presets.video_quality;
    job.download_speed_limit_bytes_per_second = job_presets.download_speed_limit_bytes_per_second;
    store.save_download(job).await.unwrap();
    let resume_presets = DownloadPresets {
        mode: DownloadMode::AudioOnly,
        audio_format: AudioFormat::M4a,
        audio_quality: AudioQuality::K192,
        video_quality: VideoQuality::P720,
        download_speed_limit_bytes_per_second: 10_000_000,
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
    paused_job.download_speed_limit_bytes_per_second =
        resume_presets.download_speed_limit_bytes_per_second;
    store.save_download(paused_job).await.unwrap();

    let current_form = DownloadPresets {
        mode: DownloadMode::Video,
        audio_format: AudioFormat::Aac,
        audio_quality: AudioQuality::K320,
        video_quality: VideoQuality::P2160,
        download_speed_limit_bytes_per_second: 25_000_000,
    };
    store
        .set_download_speed_limit(current_form.download_speed_limit_bytes_per_second, 19)
        .await
        .unwrap();
    store.set_download_presets(current_form, 20).await.unwrap();
    let retried = store.retry_download("older-job", 30).await.unwrap().unwrap();
    assert_eq!(retried.download_mode, job_presets.mode);
    assert_eq!(retried.audio_format, job_presets.audio_format);
    assert_eq!(retried.audio_quality, job_presets.audio_quality);
    assert_eq!(retried.video_quality, job_presets.video_quality);
    assert_eq!(retried.download_speed_limit_bytes_per_second, 5_000_000);
    let resumed = store.resume_download("paused-job", 31).await.unwrap().unwrap();
    assert_eq!(resumed.download_mode, resume_presets.mode);
    assert_eq!(resumed.audio_format, resume_presets.audio_format);
    assert_eq!(resumed.audio_quality, resume_presets.audio_quality);
    assert_eq!(resumed.video_quality, resume_presets.video_quality);
    assert_eq!(resumed.download_speed_limit_bytes_per_second, 10_000_000);
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
    assert_eq!(saved_job.download_speed_limit_bytes_per_second, 5_000_000);
    let saved_paused_job = saved_jobs.iter().find(|job| job.id == "paused-job").unwrap();
    assert_eq!(saved_paused_job.download_mode, resume_presets.mode);
    assert_eq!(saved_paused_job.audio_format, resume_presets.audio_format);
    assert_eq!(saved_paused_job.audio_quality, resume_presets.audio_quality);
    assert_eq!(saved_paused_job.video_quality, resume_presets.video_quality);
    assert_eq!(saved_paused_job.download_speed_limit_bytes_per_second, 10_000_000);
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
        store.get_or_initialize_download_directory("ignored-fallback".to_owned()).await.unwrap(),
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
        reopened.get_or_initialize_download_directory("ignored-fallback".to_owned()).await.unwrap(),
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
