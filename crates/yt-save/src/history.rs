use crate::model::DownloadRecord;

const HISTORY_COLUMNS: [&str; 29] = [
    "id",
    "url",
    "queue_order",
    "priority",
    "playlist_index",
    "download_mode",
    "subtitle_languages",
    "subtitle_format",
    "include_auto_subtitles",
    "audio_format",
    "audio_quality",
    "video_quality",
    "exact_format_id",
    "download_directory",
    "title",
    "source",
    "status",
    "progress_percent",
    "downloaded_bytes",
    "total_bytes",
    "speed_bytes_per_second",
    "eta_seconds",
    "output_path",
    "error",
    "created_at_unix",
    "updated_at_unix",
    "automatic_retry_attempts",
    "next_retry_at_unix",
    "scheduled_at_unix",
];

pub(crate) fn download_history_csv(records: &[DownloadRecord]) -> String {
    let mut output = String::new();
    append_csv_row(&mut output, HISTORY_COLUMNS.iter().copied());
    for record in records {
        let fields = [
            record.id.clone(),
            record.url.clone(),
            record.queue_order.to_string(),
            record.priority.to_string(),
            optional_display(record.playlist_index),
            record.download_mode.as_database_value().to_owned(),
            record.subtitle_languages.clone(),
            record.subtitle_format.as_database_value().to_owned(),
            record.include_auto_subtitles.to_string(),
            record.audio_format.as_database_value().to_owned(),
            record.audio_quality.as_database_value().to_owned(),
            record.video_quality.as_database_value().to_owned(),
            record.selected_format_id.clone().unwrap_or_default(),
            record.download_directory.clone(),
            record.title.clone(),
            record.source.clone(),
            record.status.clone(),
            format!("{:.2}", record.progress),
            optional_display(record.downloaded_bytes),
            optional_display(record.total_bytes),
            record.speed_bytes_per_second.map_or_else(String::new, |value| format!("{value:.2}")),
            optional_display(record.eta_seconds),
            record.output_path.clone().unwrap_or_default(),
            record.error.clone().unwrap_or_default(),
            record.created_at.to_string(),
            record.updated_at.to_string(),
            record.retry_attempts.to_string(),
            optional_display(record.next_retry_at),
            optional_display(record.scheduled_at),
        ];
        append_csv_row(&mut output, fields.iter().map(String::as_str));
    }
    output
}

fn optional_display<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn append_csv_row<'a>(output: &mut String, fields: impl IntoIterator<Item = &'a str>) {
    for (index, field) in fields.into_iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push('"');
        for character in field.chars() {
            if character == '"' {
                output.push('"');
            }
            output.push(character);
        }
        output.push('"');
    }
    output.push_str("\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::DownloadRecord;

    #[test]
    fn empty_history_exports_headers_only() {
        let csv = download_history_csv(&[]);

        assert!(csv.starts_with("\"id\",\"url\",\"queue_order\""));
        assert!(csv.ends_with(
            "\"created_at_unix\",\"updated_at_unix\",\"automatic_retry_attempts\",\"next_retry_at_unix\",\"scheduled_at_unix\"\r\n"
        ));
        assert_eq!(csv.matches("\r\n").count(), 1);
    }

    #[test]
    fn history_export_quotes_special_text_and_maps_persisted_fields() {
        let mut record = DownloadRecord::queued(
            "job-1".to_owned(),
            "https://example.test/watch?id=1".to_owned(),
            1_700_000_000,
        );
        record.queue_order = 3;
        record.priority = 1;
        record.playlist_index = Some(7);
        record.title = "A \"quoted\", title\r\ncontinued".to_owned();
        record.source = "Example, site".to_owned();
        record.status = "Completed".to_owned();
        record.progress = 100.0;
        record.downloaded_bytes = Some(1024);
        record.total_bytes = Some(2048);
        record.speed_bytes_per_second = Some(32.5);
        record.eta_seconds = Some(5);
        record.scheduled_at = Some(1_700_000_030);
        record.selected_format_id = Some("format-137".to_owned());
        record.output_path = Some("C:\\Media\\item.mp4".to_owned());
        record.updated_at = 1_700_000_060;

        let csv = download_history_csv(&[record]);

        assert!(csv.contains("\"job-1\",\"https://example.test/watch?id=1\",\"3\",\"1\",\"7\""));
        assert!(csv.contains("\"video_quality\",\"exact_format_id\""));
        assert!(csv.contains("\"best\",\"format-137\""));
        assert!(csv.contains("\"A \"\"quoted\"\", title\r\ncontinued\""));
        assert!(csv.contains(
            "\"Example, site\",\"Completed\",\"100.00\",\"1024\",\"2048\",\"32.50\",\"5\""
        ));
        assert!(
            csv.contains(
                "\"C:\\Media\\item.mp4\",\"\",\"1700000000\",\"1700000060\",\"0\",\"\",\"1700000030\"\r\n"
            )
        );
    }
}
