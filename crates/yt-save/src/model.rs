use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MediaMetadata {
    pub id: String,
    pub title: String,
    pub webpage_url: String,
    pub source: String,
    pub duration_seconds: Option<f64>,
    pub thumbnail: Option<String>,
    pub format_count: usize,
    pub is_playlist: bool,
    pub playlist_count: Option<usize>,
}

impl MediaMetadata {
    pub fn from_yt_dlp_json(value: &serde_json::Value, submitted_url: &str) -> Self {
        let entries = value.get("entries").and_then(serde_json::Value::as_array);
        let first_entry = entries.and_then(|items| items.first()).unwrap_or(value);
        let is_playlist = entries.is_some();

        Self {
            id: text(first_entry, "id").unwrap_or_default(),
            title: text(value, "title")
                .or_else(|| text(first_entry, "title"))
                .unwrap_or_else(|| "Untitled media".to_owned()),
            webpage_url: text(first_entry, "webpage_url")
                .or_else(|| text(value, "webpage_url"))
                .unwrap_or_else(|| submitted_url.to_owned()),
            source: text(value, "extractor_key")
                .or_else(|| text(value, "extractor"))
                .unwrap_or_else(|| "Supported site".to_owned()),
            duration_seconds: first_entry.get("duration").and_then(serde_json::Value::as_f64),
            thumbnail: text(first_entry, "thumbnail"),
            format_count: first_entry
                .get("formats")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len),
            is_playlist,
            playlist_count: entries.map(Vec::len),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DownloadRecord {
    pub id: String,
    pub url: String,
    pub title: String,
    pub source: String,
    pub status: String,
    pub progress: f32,
    pub downloaded_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub speed_bytes_per_second: Option<f64>,
    pub eta_seconds: Option<u64>,
    pub output_path: Option<String>,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl DownloadRecord {
    pub fn queued(id: String, url: String, created_at: i64) -> Self {
        Self {
            id,
            url,
            title: "Waiting for media details".to_owned(),
            source: String::new(),
            status: "Queued".to_owned(),
            progress: 0.0,
            downloaded_bytes: None,
            total_bytes: None,
            speed_bytes_per_second: None,
            eta_seconds: None,
            output_path: None,
            error: None,
            created_at,
            updated_at: created_at,
        }
    }
}

fn text(value: &serde_json::Value, key: &str) -> Option<String> {
    value.get(key).and_then(serde_json::Value::as_str).map(str::to_owned)
}

pub fn format_bytes(bytes: Option<u64>) -> String {
    let Some(bytes) = bytes else {
        return "Size not reported".to_owned();
    };
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

pub fn format_duration(seconds: Option<f64>) -> String {
    let Some(seconds) = seconds.filter(|value| value.is_finite() && *value >= 0.0) else {
        return "Duration unavailable".to_owned();
    };
    let seconds = seconds as u64;
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;
    if hours > 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes}:{seconds:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_parser_handles_single_item_and_playlist_json() {
        let single: serde_json::Value = serde_json::from_str(
            r#"{"id":"x1","title":"A clip","webpage_url":"https://example.test/x1","extractor_key":"Example","duration":61,"formats":[{},{}]}"#,
        )
        .unwrap();
        let parsed = MediaMetadata::from_yt_dlp_json(&single, "https://example.test/x1");
        assert_eq!(parsed.title, "A clip");
        assert_eq!(parsed.source, "Example");
        assert_eq!(parsed.format_count, 2);
        assert!(!parsed.is_playlist);

        let playlist: serde_json::Value = serde_json::from_str(
            r#"{"title":"Playlist","entries":[{"id":"x1","title":"First"},{"id":"x2","title":"Second"}]}"#,
        )
        .unwrap();
        let parsed = MediaMetadata::from_yt_dlp_json(&playlist, "https://example.test/list");
        assert!(parsed.is_playlist);
        assert_eq!(parsed.playlist_count, Some(2));
        assert_eq!(parsed.title, "Playlist");
    }

    #[test]
    fn duration_and_size_formatting_do_not_invent_values() {
        assert_eq!(format_duration(None), "Duration unavailable");
        assert_eq!(format_duration(Some(3661.0)), "1:01:01");
        assert_eq!(format_bytes(None), "Size not reported");
        assert_eq!(format_bytes(Some(1_500_000)), "1.5 MB");
    }
}
