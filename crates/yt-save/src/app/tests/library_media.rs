use super::*;

#[test]
fn media_library_filters_real_files_and_surfaces_missing_paths() {
    let temp_dir = std::env::temp_dir().join(format!("yt-save-test-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let video_path = temp_dir.join("tutorial.mp4");
    let audio_path = temp_dir.join("track.mp3");
    let subtitle_path = temp_dir.join("captions.srt");
    std::fs::write(&video_path, b"video-bytes").unwrap();
    std::fs::write(&audio_path, b"audio").unwrap();
    std::fs::write(&subtitle_path, b"subs").unwrap();

    let mut video =
        DownloadRecord::queued("video".to_owned(), "https://video.example/watch/1".to_owned(), 10);
    video.title = "Rust tutorial".to_owned();
    video.source = "Video Example".to_owned();
    video.status = "Completed".to_owned();
    video.output_path = Some(video_path.to_string_lossy().into_owned());
    video.updated_at = 20;

    let mut audio =
        DownloadRecord::queued("audio".to_owned(), "https://audio.example/track/2".to_owned(), 11);
    audio.title = "Quiet track".to_owned();
    audio.source = "Audio Example".to_owned();
    audio.status = "Completed".to_owned();
    audio.download_mode = DownloadMode::AudioOnly;
    audio.output_path = Some(audio_path.to_string_lossy().into_owned());
    audio.updated_at = 10;

    let mut subtitles = DownloadRecord::queued(
        "subtitles".to_owned(),
        "https://video.example/watch/4".to_owned(),
        12,
    );
    subtitles.title = "English captions".to_owned();
    subtitles.source = "Video Example".to_owned();
    subtitles.status = "Completed".to_owned();
    subtitles.download_mode = DownloadMode::SubtitlesOnly;
    subtitles.output_path = Some(subtitle_path.to_string_lossy().into_owned());
    subtitles.updated_at = 15;

    let mut missing = DownloadRecord::queued(
        "missing".to_owned(),
        "https://video.example/watch/3".to_owned(),
        12,
    );
    missing.title = "Moved tutorial".to_owned();
    missing.status = "Completed".to_owned();
    missing.output_path = Some(temp_dir.join("moved.mp4").to_string_lossy().into_owned());
    missing.updated_at = 30;

    let records = [video, audio, subtitles, missing];
    let all = library_row_views(&records, "", "All files", "Newest first");
    assert_eq!(all.len(), 4);
    assert_eq!(all[0].id.as_str(), "missing");
    assert!(!all[0].path_available);
    assert_eq!(all[0].file_status.as_str(), "Unavailable · locate moved file");
    assert_eq!(all[1].id.as_str(), "video");
    assert!(all[1].path_available);
    assert!(all[1].size_label.as_str().contains("on disk"));
    assert_eq!(all[2].id.as_str(), "subtitles");
    assert_eq!(all[2].kind.as_str(), "Subtitles");

    let audio_only = library_row_views(&records, "audio.example", "Audio", "Title A–Z");
    assert_eq!(audio_only.len(), 1);
    assert_eq!(audio_only[0].id.as_str(), "audio");
    let subtitle_only = library_row_views(&records, "", "Subtitles", "Newest first");
    assert_eq!(subtitle_only.len(), 1);
    assert_eq!(subtitle_only[0].id.as_str(), "subtitles");
    let missing_only = library_row_views(&records, "", "Missing files", "Newest first");
    assert_eq!(missing_only.len(), 1);
    assert_eq!(missing_only[0].id.as_str(), "missing");

    let largest_first = library_row_views(&records, "", "All files", "Largest first");
    assert_eq!(largest_first[0].id.as_str(), "video");
    std::fs::remove_dir_all(temp_dir).unwrap();
}

#[test]
fn library_relink_accepts_media_extensions_and_rejects_unrelated_files() {
    assert!(has_supported_media_extension(Path::new("lesson.MP4")));
    assert!(has_supported_media_extension(Path::new("recording.opus")));
    assert!(has_supported_media_extension(Path::new("captions.SRT")));
    assert!(has_supported_media_extension(Path::new("captions.vtt")));
    assert!(!has_supported_media_extension(Path::new("notes.txt")));
    assert!(!has_supported_media_extension(Path::new("extensionless")));
}

#[test]
fn library_cache_signature_ignores_queue_progress_but_tracks_completed_paths() {
    let mut completed =
        DownloadRecord::queued("completed".to_owned(), "https://example.test/media".to_owned(), 10);
    completed.status = "Completed".to_owned();
    completed.output_path = Some("C:/Media/old.mp4".to_owned());
    let mut queued =
        DownloadRecord::queued("queued".to_owned(), "https://example.test/queued".to_owned(), 11);
    let baseline = library_cache_signature_for(&[completed.clone(), queued.clone()]);

    queued.progress = 0.5;
    completed.progress = 1.0;
    assert_eq!(baseline, library_cache_signature_for(&[completed.clone(), queued.clone()]));

    completed.output_path = Some("C:/Media/moved.mp4".to_owned());
    assert_ne!(baseline, library_cache_signature_for(&[completed.clone(), queued.clone()]));
    completed.status = "Failed".to_owned();
    assert_ne!(baseline, library_cache_signature_for(&[completed, queued]));
}

#[test]
fn row_view_labels_audio_output_and_video_only_selection() {
    let mut record =
        DownloadRecord::queued("audio-job".to_owned(), "https://example.test/media".to_owned(), 10);
    record.download_mode = DownloadMode::AudioOnly;
    record.audio_format = AudioFormat::Mp3;

    let row = row_view_with_queue_position(&record, "", "", "");

    assert!(row.detail.as_str().starts_with("Audio · MP3 · Best · "));

    let mut video = DownloadRecord::queued(
        "video-only-job".to_owned(),
        "https://example.test/video".to_owned(),
        11,
    );
    video.download_mode = DownloadMode::VideoOnly;
    video.video_quality = crate::model::VideoQuality::P720;
    video.selected_format_id = Some("video-137".to_owned());
    let row = row_view_with_queue_position(&video, "", "", "");
    assert!(row.detail.as_str().starts_with("Video only · Format video-137 · "));
}

#[test]
fn format_choices_include_only_formats_compatible_with_the_analyzed_mode() {
    let value: serde_json::Value = serde_json::from_str(
            r#"{"id":"sample","formats":[{"format_id":"av","ext":"mp4","vcodec":"avc1","acodec":"none"},{"format_id":"aa","ext":"m4a","vcodec":"none","acodec":"aac"},{"format_id":"muxed","ext":"mp4","vcodec":"avc1","acodec":"aac"}]}"#,
        )
        .unwrap();
    let metadata = MediaMetadata::from_yt_dlp_json_for_playlist_item(
        &value,
        "https://example.test/watch",
        None,
    );
    let video_only = media_format_choice_labels(&metadata.formats, DownloadMode::VideoOnly);
    let video = media_format_choice_labels(&metadata.formats, DownloadMode::Video);
    let audio = media_format_choice_labels(&metadata.formats, DownloadMode::AudioOnly);

    assert_eq!(video_only.len(), 2);
    assert!(video_only[0].as_str().starts_with("Automatic"));
    assert!(video_only[1].as_str().starts_with("av · "));
    assert_eq!(video.len(), 2);
    assert!(video[1].as_str().starts_with("muxed · "));
    assert_eq!(audio.len(), 2);
    assert!(audio[1].as_str().starts_with("aa · "));
}

#[test]
fn exact_format_queue_choice_requires_the_current_url_and_mode() {
    let selection = "137 · mp4 · 1080p · video only · avc1";
    assert_eq!(
        selected_format_id_from_ui(
            selection,
            "https://example.test/watch",
            "video_only",
            "https://example.test/watch",
            DownloadMode::VideoOnly,
        )
        .unwrap()
        .as_deref(),
        Some("137")
    );
    assert!(
        selected_format_id_from_ui(
            selection,
            "https://example.test/old",
            "video_only",
            "https://example.test/watch",
            DownloadMode::VideoOnly,
        )
        .is_err()
    );
    assert!(
        selected_format_id_from_ui(
            selection,
            "https://example.test/watch",
            "video",
            "https://example.test/watch",
            DownloadMode::VideoOnly,
        )
        .is_err()
    );
    assert!(
        selected_format_id_from_ui(
            "137/140 · combined",
            "https://example.test/watch",
            "video",
            "https://example.test/watch",
            DownloadMode::Video,
        )
        .is_err()
    );
    assert_eq!(
        selected_format_id_from_ui(
            AUTOMATIC_FORMAT_CHOICE,
            "https://example.test/old",
            "audio",
            "https://example.test/watch",
            DownloadMode::Video,
        )
        .unwrap(),
        None
    );
}

#[test]
fn exact_format_menu_exposes_all_parsed_choices_up_to_the_display_cap() {
    let formats = (0..120)
        .map(|index| MediaFormatOption {
            id: format!("video-{index}"),
            extension: "mp4".to_owned(),
            width: Some(1920),
            height: Some(1080),
            fps: None,
            size_bytes: None,
            size_is_estimate: false,
            video_codec: Some("avc1".to_owned()),
            audio_codec: None,
        })
        .collect::<Vec<_>>();

    let choices = media_format_choice_labels(&formats, DownloadMode::VideoOnly);

    assert_eq!(choices.len(), 101);
    assert!(choices[100].as_str().starts_with("video-99 · "));
}

#[test]
fn clipboard_popup_position_accounts_for_dpi_and_monitor_origin() {
    let area = WorkArea { left: -1920, top: 80, right: 0, bottom: 1160 };

    assert_eq!(clipboard_popup::initial_popup_position(area, 1.5, 440, 214), (-676, 823));
}

#[test]
fn clipboard_popup_displays_the_source_host_for_any_extractor_site() {
    assert_eq!(
        clipboard_popup::clipboard_source_host("https://media.example/watch/1"),
        "media.example"
    );
    assert_eq!(clipboard_popup::clipboard_source_host("https://[invalid"), "Web media");
}

#[test]
fn missing_output_file_fails_before_launching_a_handler() {
    let path = std::env::temp_dir().join(format!("yt-save-missing-{}.mp4", Uuid::new_v4()));
    assert!(open_file(&path).is_err());
}

#[test]
fn existing_media_file_passes_validation_without_launching_a_handler() {
    let path = std::env::temp_dir().join(format!("yt-save-media {}.mp4", Uuid::new_v4()));
    std::fs::write(&path, b"fixture media").unwrap();

    let result = validate_output_file(&path);

    std::fs::remove_file(&path).unwrap();
    assert!(result.is_ok());
}

#[test]
fn a_directory_is_not_accepted_as_a_completed_media_file() {
    let path = std::env::temp_dir().join(format!("yt-save-directory-{}", Uuid::new_v4()));
    std::fs::create_dir(&path).unwrap();

    let result = open_file(&path);

    std::fs::remove_dir(&path).unwrap();
    assert!(result.is_err());
}

#[cfg(windows)]
#[test]
fn shell_open_arguments_preserve_spaces_and_unicode_without_shell_quoting() {
    use std::os::windows::ffi::OsStringExt;

    let path = std::env::temp_dir().join("Yt Save media Ω.mp4");
    let encoded_path = wide_null_terminated(path.as_os_str());
    let decoded_path = std::ffi::OsString::from_wide(&encoded_path[..encoded_path.len() - 1]);
    let encoded_operation = wide_null_terminated(std::ffi::OsStr::new("open"));

    assert_eq!(decoded_path, path.as_os_str());
    assert_eq!(encoded_path.last(), Some(&0));
    assert_eq!(
        std::ffi::OsString::from_wide(&encoded_operation[..encoded_operation.len() - 1])
            .as_os_str(),
        std::ffi::OsStr::new("open")
    );
}
