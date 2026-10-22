use super::*;

#[test]
fn history_search_is_case_insensitive_and_preserves_full_queue_positions() {
    let first =
        DownloadRecord::queued("first".to_owned(), "https://example.test/first".to_owned(), 10);
    let mut second = DownloadRecord::queued(
        "second".to_owned(),
        "https://media.example.test/featured".to_owned(),
        11,
    );
    second.title = "Featured clip".to_owned();
    second.source = "Example Site".to_owned();

    let rows = row_views_filtered(&[first, second], "MEDIA.EXAMPLE.TEST");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id.as_str(), "second");
    assert_eq!(rows[0].url.as_str(), "https://media.example.test/featured");
    assert_eq!(rows[0].queue_position.as_str(), "#2");
    assert!(row_views_filtered(&[], "no match").is_empty());
}

#[test]
fn download_destination_must_be_an_existing_directory_and_is_canonicalized() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let selected = temp_dir.join("selected");
    std::fs::create_dir_all(&selected).unwrap();
    let file = temp_dir.join("not-a-folder");
    std::fs::write(&file, b"x").unwrap();

    assert_eq!(validate_download_directory(&selected).unwrap(), selected.canonicalize().unwrap());
    assert!(validate_download_directory(&file).is_err());
    assert!(validate_download_directory(&temp_dir.join("missing")).is_err());
    assert!(validate_download_directory(Path::new("")).is_err());
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn local_schedule_requires_a_valid_future_time() {
    let current_timestamp = now();
    let future_local =
        (chrono::Local::now() + chrono::Duration::hours(4)).format("%Y-%m-%d %H:%M").to_string();
    let scheduled_at = parse_local_schedule(&future_local, current_timestamp).unwrap();
    assert!(scheduled_at > current_timestamp);
    assert!(parse_local_schedule("next Tuesday", current_timestamp).is_err());

    let past_local =
        (chrono::Local::now() - chrono::Duration::hours(4)).format("%Y-%m-%d %H:%M").to_string();
    assert!(parse_local_schedule(&past_local, current_timestamp).is_err());
}

#[test]
fn accepts_http_urls_without_restricting_sites() {
    assert_eq!(
        validate_url(" https://peertube.example/videos/watch/abc ").unwrap(),
        "https://peertube.example/videos/watch/abc"
    );
    assert_eq!(
        validate_url("https://www.youtube.com/watch?v=abc").unwrap(),
        "https://www.youtube.com/watch?v=abc"
    );
    assert!(validate_url("file:///C:/private.mp4").is_err());
    assert!(validate_url("not a URL").is_err());
}

#[test]
fn batch_url_parser_handles_crlf_blanks_duplicates_and_line_errors() {
    let parsed = parse_batch_urls(
            "\r\n https://Media.example/watch/one \r\nhttps://media.example/watch/one\nftp://media.example/audio\nnot a url\nhttps://peer.example/video\n",
        )
        .unwrap();

    assert_eq!(
        parsed.urls,
        [
            BatchUrl { line_number: 2, url: "https://media.example/watch/one".to_owned() },
            BatchUrl { line_number: 6, url: "https://peer.example/video".to_owned() },
        ]
    );
    assert_eq!(
        parsed.issues,
        [
            "line 3: duplicate URL skipped",
            "line 4: Only http and https media URLs are accepted.",
            "line 5: Enter a complete http or https URL.",
        ]
    );
}

#[test]
fn batch_url_parser_rejects_empty_and_oversized_batches() {
    assert!(parse_batch_urls("\n  \r\n").is_err());
    assert!(parse_batch_urls("file:///C:/private.mp4\nnot a URL").unwrap_err().contains("line 1"));

    let oversized = std::iter::repeat_n("https://media.example/watch", MAX_BATCH_URLS + 1)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(parse_batch_urls(&oversized).unwrap_err().contains("no more than 100"));
}

#[test]
fn queued_batch_records_snapshot_download_settings_and_schedule() {
    let settings = BatchDownloadSettings {
        presets: DownloadPresets::from_ui_values(true, false, "mp3", "192k", "720p")
            .with_download_speed_limit_label("2 MB/s"),
        subtitle_languages: "en,fr".to_owned(),
        subtitle_format: SubtitleFormat::Srt,
        include_auto_subtitles: true,
        scheduled_at: Some(2_000_000_000),
        download_directory: "C:\\Users\\Example\\Downloads".to_owned(),
    };

    let record = queued_batch_record(
        "https://media.example/audio".to_owned(),
        "Audio title".to_owned(),
        "Media example".to_owned(),
        1_900_000_000,
        &settings,
    );

    assert_eq!(record.url, "https://media.example/audio");
    assert_eq!(record.title, "Audio title");
    assert_eq!(record.source, "Media example");
    assert_eq!(record.download_mode, settings.presets.mode);
    assert_eq!(record.audio_format, settings.presets.audio_format);
    assert_eq!(record.audio_quality, settings.presets.audio_quality);
    assert_eq!(record.video_quality, settings.presets.video_quality);
    assert_eq!(record.download_speed_limit_bytes_per_second, 2_000_000);
    assert_eq!(record.subtitle_languages, settings.subtitle_languages);
    assert_eq!(record.subtitle_format, settings.subtitle_format);
    assert!(record.include_auto_subtitles);
    assert_eq!(record.scheduled_at, settings.scheduled_at);
    assert_eq!(record.status, "Waiting for schedule");
    assert_eq!(record.download_directory, settings.download_directory);
}
