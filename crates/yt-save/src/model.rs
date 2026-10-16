use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DownloadMode {
    #[default]
    Video,
    AudioOnly,
}

impl DownloadMode {
    pub fn as_database_value(self) -> &'static str {
        match self {
            Self::Video => "video",
            Self::AudioOnly => "audio",
        }
    }

    pub fn from_database_value(value: &str) -> Option<Self> {
        match value {
            "video" => Some(Self::Video),
            "audio" => Some(Self::AudioOnly),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AudioFormat {
    #[default]
    Original,
    Mp3,
    M4a,
    Opus,
    Aac,
    Alac,
    Flac,
    Vorbis,
    Wav,
}

impl AudioFormat {
    pub fn as_database_value(self) -> &'static str {
        match self {
            Self::Original => "original",
            Self::Mp3 => "mp3",
            Self::M4a => "m4a",
            Self::Opus => "opus",
            Self::Aac => "aac",
            Self::Alac => "alac",
            Self::Flac => "flac",
            Self::Vorbis => "vorbis",
            Self::Wav => "wav",
        }
    }

    pub fn from_database_value(value: &str) -> Option<Self> {
        match value {
            "original" => Some(Self::Original),
            "mp3" => Some(Self::Mp3),
            "m4a" => Some(Self::M4a),
            "opus" => Some(Self::Opus),
            "aac" => Some(Self::Aac),
            "alac" => Some(Self::Alac),
            "flac" => Some(Self::Flac),
            "vorbis" => Some(Self::Vorbis),
            "wav" => Some(Self::Wav),
            _ => None,
        }
    }

    pub fn yt_dlp_name(self) -> Option<&'static str> {
        match self {
            Self::Original => None,
            Self::Mp3 => Some("mp3"),
            Self::M4a => Some("m4a"),
            Self::Opus => Some("opus"),
            Self::Aac => Some("aac"),
            Self::Alac => Some("alac"),
            Self::Flac => Some("flac"),
            Self::Vorbis => Some("vorbis"),
            Self::Wav => Some("wav"),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Original => "Original",
            Self::Mp3 => "MP3",
            Self::M4a => "M4A",
            Self::Opus => "Opus",
            Self::Aac => "AAC",
            Self::Alac => "ALAC",
            Self::Flac => "FLAC",
            Self::Vorbis => "Vorbis",
            Self::Wav => "WAV",
        }
    }

    pub fn supports_quality_setting(self) -> bool {
        matches!(self, Self::Mp3 | Self::M4a | Self::Opus | Self::Aac | Self::Vorbis)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AudioQuality {
    #[default]
    Best,
    K320,
    K192,
    K128,
}

impl AudioQuality {
    pub fn as_database_value(self) -> &'static str {
        match self {
            Self::Best => "best",
            Self::K320 => "320k",
            Self::K192 => "192k",
            Self::K128 => "128k",
        }
    }

    pub fn from_database_value(value: &str) -> Option<Self> {
        match value {
            "best" => Some(Self::Best),
            "320k" => Some(Self::K320),
            "192k" => Some(Self::K192),
            "128k" => Some(Self::K128),
            _ => None,
        }
    }

    pub fn yt_dlp_argument(self) -> &'static str {
        match self {
            Self::Best => "0",
            Self::K320 => "320K",
            Self::K192 => "192K",
            Self::K128 => "128K",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Best => "Best",
            Self::K320 => "320 kbps",
            Self::K192 => "192 kbps",
            Self::K128 => "128 kbps",
        }
    }
}

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
    #[serde(default)]
    pub playlist_entries: Vec<PlaylistEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlaylistEntry {
    pub playlist_index: Option<i64>,
    pub title: String,
}

impl MediaMetadata {
    pub fn from_yt_dlp_json_for_playlist_item(
        value: &serde_json::Value,
        submitted_url: &str,
        selected_playlist_index: Option<i64>,
    ) -> Self {
        let entries = value.get("entries").and_then(serde_json::Value::as_array);
        let selected_entry = selected_playlist_index.and_then(|selected_index| {
            entries.and_then(|items| {
                items.iter().find(|entry| playlist_index(entry) == Some(selected_index))
            })
        });
        let first_entry =
            selected_entry.or_else(|| entries.and_then(|items| items.first())).unwrap_or(value);
        let is_playlist = entries.is_some();
        let playlist_entries = entries.map_or_else(Vec::new, |items| {
            items
                .iter()
                .filter(|entry| entry.is_object())
                .map(|entry| PlaylistEntry {
                    playlist_index: playlist_index(entry),
                    title: text(entry, "title")
                        .unwrap_or_else(|| "Untitled playlist item".to_owned()),
                })
                .collect()
        });
        let title = if selected_playlist_index.is_some() {
            text(first_entry, "title").or_else(|| text(value, "title"))
        } else {
            text(value, "title").or_else(|| text(first_entry, "title"))
        };

        Self {
            id: text(first_entry, "id").unwrap_or_default(),
            title: title.unwrap_or_else(|| "Untitled media".to_owned()),
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
            playlist_entries,
        }
    }
}

fn playlist_index(value: &serde_json::Value) -> Option<i64> {
    value.get("playlist_index").and_then(serde_json::Value::as_i64).filter(|index| *index > 0)
}

#[derive(Clone, Debug)]
pub struct DownloadRecord {
    pub id: String,
    pub url: String,
    /// SQLite rowid assigned when this queue item was first inserted.
    pub queue_order: i64,
    pub playlist_index: Option<i64>,
    pub download_mode: DownloadMode,
    pub audio_format: AudioFormat,
    pub audio_quality: AudioQuality,
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
            queue_order: 0,
            playlist_index: None,
            download_mode: DownloadMode::Video,
            audio_format: AudioFormat::Original,
            audio_quality: AudioQuality::Best,
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
        let parsed = MediaMetadata::from_yt_dlp_json_for_playlist_item(
            &single,
            "https://example.test/x1",
            None,
        );
        assert_eq!(parsed.title, "A clip");
        assert_eq!(parsed.source, "Example");
        assert_eq!(parsed.format_count, 2);
        assert!(!parsed.is_playlist);

        let playlist: serde_json::Value = serde_json::from_str(
            r#"{"title":"Playlist","entries":[{"id":"x1","title":"First"},{"id":"x2","title":"Second"}]}"#,
        )
        .unwrap();
        let parsed = MediaMetadata::from_yt_dlp_json_for_playlist_item(
            &playlist,
            "https://example.test/list",
            None,
        );
        assert!(parsed.is_playlist);
        assert_eq!(parsed.playlist_count, Some(2));
        assert_eq!(parsed.title, "Playlist");
        assert_eq!(parsed.playlist_entries.len(), 2);
        assert_eq!(parsed.playlist_entries[0].playlist_index, None);
    }

    #[test]
    fn playlist_entries_are_selectable_only_with_positive_integer_indexes() {
        let playlist: serde_json::Value = serde_json::from_str(
            r#"{"title":"Playlist","entries":[{"title":"First","playlist_index":1},{"title":"Zero","playlist_index":0},{"title":"String","playlist_index":"3"},{"title":"Missing"},null,{"title":"Sixth","playlist_index":6}]}"#,
        )
        .unwrap();

        let parsed = MediaMetadata::from_yt_dlp_json_for_playlist_item(
            &playlist,
            "https://example.test/list",
            None,
        );

        assert_eq!(parsed.playlist_count, Some(6));
        assert_eq!(parsed.playlist_entries.len(), 5);
        assert_eq!(parsed.playlist_entries[0].playlist_index, Some(1));
        assert_eq!(parsed.playlist_entries[1].playlist_index, None);
        assert_eq!(parsed.playlist_entries[2].playlist_index, None);
        assert_eq!(parsed.playlist_entries[3].playlist_index, None);
        assert_eq!(parsed.playlist_entries[4].playlist_index, Some(6));
    }

    #[test]
    fn selected_playlist_item_uses_its_title_instead_of_the_playlist_title() {
        let playlist: serde_json::Value = serde_json::from_str(
            r#"{"title":"Playlist","entries":[{"title":"Second item","playlist_index":2}]}"#,
        )
        .unwrap();

        let parsed = MediaMetadata::from_yt_dlp_json_for_playlist_item(
            &playlist,
            "https://example.test/list",
            Some(2),
        );

        assert_eq!(parsed.title, "Second item");
    }

    #[test]
    fn duration_and_size_formatting_do_not_invent_values() {
        assert_eq!(format_duration(None), "Duration unavailable");
        assert_eq!(format_duration(Some(3661.0)), "1:01:01");
        assert_eq!(format_bytes(None), "Size not reported");
        assert_eq!(format_bytes(Some(1_500_000)), "1.5 MB");
    }

    #[test]
    fn persisted_audio_choices_have_stable_database_values() {
        for (mode, value) in [(DownloadMode::Video, "video"), (DownloadMode::AudioOnly, "audio")] {
            assert_eq!(DownloadMode::from_database_value(value), Some(mode));
            assert_eq!(mode.as_database_value(), value);
        }
        for (format, value) in [
            (AudioFormat::Original, "original"),
            (AudioFormat::Mp3, "mp3"),
            (AudioFormat::M4a, "m4a"),
            (AudioFormat::Opus, "opus"),
            (AudioFormat::Aac, "aac"),
            (AudioFormat::Alac, "alac"),
            (AudioFormat::Flac, "flac"),
            (AudioFormat::Vorbis, "vorbis"),
            (AudioFormat::Wav, "wav"),
        ] {
            assert_eq!(AudioFormat::from_database_value(value), Some(format));
            assert_eq!(format.as_database_value(), value);
        }
        assert_eq!(AudioFormat::Mp3.yt_dlp_name(), Some("mp3"));
        assert_eq!(AudioFormat::Original.yt_dlp_name(), None);
        assert_eq!(DownloadMode::from_database_value("other"), None);
        assert!(AudioFormat::Aac.supports_quality_setting());
        assert!(!AudioFormat::Alac.supports_quality_setting());
        assert!(!AudioFormat::Flac.supports_quality_setting());
        assert!(AudioFormat::Vorbis.supports_quality_setting());
    }

    #[test]
    fn audio_quality_has_stable_persistence_and_yt_dlp_values() {
        for (quality, stored, argument) in [
            (AudioQuality::Best, "best", "0"),
            (AudioQuality::K320, "320k", "320K"),
            (AudioQuality::K192, "192k", "192K"),
            (AudioQuality::K128, "128k", "128K"),
        ] {
            assert_eq!(AudioQuality::from_database_value(stored), Some(quality));
            assert_eq!(quality.as_database_value(), stored);
            assert_eq!(quality.yt_dlp_argument(), argument);
        }
        assert_eq!(AudioQuality::from_database_value("256k"), None);
    }
}
