use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DownloadMode {
    #[default]
    Video,
    VideoOnly,
    AudioOnly,
    SubtitlesOnly,
}

impl DownloadMode {
    pub fn as_database_value(self) -> &'static str {
        match self {
            Self::Video => "video",
            Self::VideoOnly => "video_only",
            Self::AudioOnly => "audio",
            Self::SubtitlesOnly => "subtitles",
        }
    }

    pub fn from_database_value(value: &str) -> Option<Self> {
        match value {
            "video" => Some(Self::Video),
            "video_only" => Some(Self::VideoOnly),
            "audio" => Some(Self::AudioOnly),
            "subtitles" => Some(Self::SubtitlesOnly),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SubtitleFormat {
    Srt,
    #[default]
    Vtt,
}

impl SubtitleFormat {
    pub fn as_database_value(self) -> &'static str {
        match self {
            Self::Srt => "srt",
            Self::Vtt => "vtt",
        }
    }

    pub fn from_database_value(value: &str) -> Option<Self> {
        match value {
            "srt" => Some(Self::Srt),
            "vtt" => Some(Self::Vtt),
            _ => None,
        }
    }
}

pub fn normalize_subtitle_languages(value: &str) -> Option<String> {
    let mut languages = Vec::new();
    for language in value.split(',').map(str::trim) {
        if language.is_empty()
            || language.len() > 32
            || !language.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
            || languages.iter().any(|existing: &String| existing.eq_ignore_ascii_case(language))
        {
            return None;
        }
        languages.push(language.to_owned());
    }
    if languages.is_empty() || languages.len() > 8 {
        return None;
    }
    let includes_all = languages.iter().any(|language| language.eq_ignore_ascii_case("all"));
    if includes_all && languages.len() != 1 {
        return None;
    }
    Some(languages.join(","))
}

pub fn subtitle_languages_available(requested: &str, available: &[String]) -> bool {
    if available.is_empty() {
        return false;
    }
    requested.eq_ignore_ascii_case("all")
        || requested.split(',').map(str::trim).all(|language| {
            available.iter().any(|candidate| candidate.eq_ignore_ascii_case(language))
        })
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VideoQuality {
    #[default]
    Best,
    P2160,
    P1440,
    P1080,
    P720,
    P480,
    P360,
    Lowest,
}

impl VideoQuality {
    pub fn as_database_value(self) -> &'static str {
        match self {
            Self::Best => "best",
            Self::P2160 => "2160p",
            Self::P1440 => "1440p",
            Self::P1080 => "1080p",
            Self::P720 => "720p",
            Self::P480 => "480p",
            Self::P360 => "360p",
            Self::Lowest => "lowest",
        }
    }

    pub fn from_database_value(value: &str) -> Option<Self> {
        match value {
            "best" => Some(Self::Best),
            "2160p" => Some(Self::P2160),
            "1440p" => Some(Self::P1440),
            "1080p" => Some(Self::P1080),
            "720p" => Some(Self::P720),
            "480p" => Some(Self::P480),
            "360p" => Some(Self::P360),
            "lowest" => Some(Self::Lowest),
            _ => None,
        }
    }

    pub fn from_ui_label(value: &str) -> Option<Self> {
        match value {
            "Best" => Some(Self::Best),
            "2160p / 4K" => Some(Self::P2160),
            "1440p" => Some(Self::P1440),
            "1080p" => Some(Self::P1080),
            "720p" => Some(Self::P720),
            "480p" => Some(Self::P480),
            "360p" => Some(Self::P360),
            "Lowest" => Some(Self::Lowest),
            _ => None,
        }
    }

    pub fn yt_dlp_format(self) -> &'static str {
        match self {
            Self::Best => "bv*+ba/b",
            Self::P2160 => "bestvideo[height<=2160]+bestaudio/best[height<=2160]",
            Self::P1440 => "bestvideo[height<=1440]+bestaudio/best[height<=1440]",
            Self::P1080 => "bestvideo[height<=1080]+bestaudio/best[height<=1080]",
            Self::P720 => "bestvideo[height<=720]+bestaudio/best[height<=720]",
            Self::P480 => "bestvideo[height<=480]+bestaudio/best[height<=480]",
            Self::P360 => "bestvideo[height<=360]+bestaudio/best[height<=360]",
            Self::Lowest => "worstvideo+worstaudio/worst",
        }
    }

    /// Selects a video stream without an audio stream, preserving the requested height cap.
    pub fn yt_dlp_video_only_format(self) -> &'static str {
        match self {
            Self::Best => "bestvideo",
            Self::P2160 => "bestvideo[height<=2160]",
            Self::P1440 => "bestvideo[height<=1440]",
            Self::P1080 => "bestvideo[height<=1080]",
            Self::P720 => "bestvideo[height<=720]",
            Self::P480 => "bestvideo[height<=480]",
            Self::P360 => "bestvideo[height<=360]",
            Self::Lowest => "worstvideo",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Best => "Best",
            Self::P2160 => "2160p / 4K",
            Self::P1440 => "1440p",
            Self::P1080 => "1080p",
            Self::P720 => "720p",
            Self::P480 => "480p",
            Self::P360 => "360p",
            Self::Lowest => "Lowest",
        }
    }
}

/// Defaults shown in the New download form. Per-job choices remain persisted on each queue row.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DownloadPresets {
    pub mode: DownloadMode,
    pub audio_format: AudioFormat,
    pub audio_quality: AudioQuality,
    pub video_quality: VideoQuality,
}

impl DownloadPresets {
    pub fn from_database_values(
        mode: Option<&str>,
        audio_format: Option<&str>,
        audio_quality: Option<&str>,
        video_quality: Option<&str>,
    ) -> Self {
        Self {
            mode: mode.and_then(DownloadMode::from_database_value).unwrap_or_default(),
            audio_format: audio_format
                .and_then(AudioFormat::from_database_value)
                .unwrap_or_default(),
            audio_quality: audio_quality
                .and_then(AudioQuality::from_database_value)
                .unwrap_or_default(),
            video_quality: video_quality
                .and_then(VideoQuality::from_database_value)
                .unwrap_or_default(),
        }
    }

    pub fn from_ui_values(
        audio_only: bool,
        video_only: bool,
        audio_format: &str,
        audio_quality: &str,
        video_quality: &str,
    ) -> Self {
        Self {
            mode: if audio_only {
                DownloadMode::AudioOnly
            } else if video_only {
                DownloadMode::VideoOnly
            } else {
                DownloadMode::Video
            },
            audio_format: AudioFormat::from_database_value(audio_format).unwrap_or_default(),
            audio_quality: AudioQuality::from_database_value(audio_quality).unwrap_or_default(),
            video_quality: VideoQuality::from_ui_label(video_quality).unwrap_or_default(),
        }
    }

    pub fn database_values(self) -> [(&'static str, &'static str); 4] {
        [
            ("new_download_mode", self.mode.as_database_value()),
            ("new_download_audio_format", self.audio_format.as_database_value()),
            ("new_download_audio_quality", self.audio_quality.as_database_value()),
            ("new_download_video_quality", self.video_quality.as_database_value()),
        ]
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
    #[serde(default)]
    pub formats: Vec<MediaFormatOption>,
    pub is_playlist: bool,
    pub playlist_count: Option<usize>,
    #[serde(default)]
    pub playlist_entries: Vec<PlaylistEntry>,
    #[serde(default)]
    pub subscription_entries: Vec<PlaylistMediaEntry>,
    #[serde(default)]
    pub manual_subtitle_languages: Vec<String>,
    #[serde(default)]
    pub automatic_subtitle_languages: Vec<String>,
}

/// Safe, displayable metadata for one yt-dlp format. Stream URLs and request details are omitted.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MediaFormatOption {
    pub id: String,
    pub extension: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub fps: Option<f64>,
    pub size_bytes: Option<u64>,
    pub size_is_estimate: bool,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
}

impl MediaFormatOption {
    pub fn supports_mode(&self, mode: DownloadMode) -> bool {
        let has_video = self.video_codec.is_some();
        let has_audio = self.audio_codec.is_some();
        match mode {
            DownloadMode::Video => has_video && has_audio,
            DownloadMode::VideoOnly => has_video && !has_audio,
            DownloadMode::AudioOnly => has_audio && !has_video,
            DownloadMode::SubtitlesOnly => false,
        }
    }

    pub fn choice_label(&self) -> String {
        let extension = if self.extension.is_empty() { "unknown" } else { &self.extension };
        let resolution = match (self.width, self.height) {
            (Some(width), Some(height)) => format!("{width}×{height}"),
            (_, Some(height)) => format!("{height}p"),
            _ => "resolution unknown".to_owned(),
        };
        let streams = match (self.video_codec.is_some(), self.audio_codec.is_some()) {
            (true, true) => "video + audio",
            (true, false) => "video only",
            (false, true) => "audio only",
            (false, false) => "no media streams",
        };
        let codecs = match (&self.video_codec, &self.audio_codec) {
            (Some(video), Some(audio)) => {
                format!("{} + {}", compact_codec(video), compact_codec(audio))
            }
            (Some(video), None) => compact_codec(video),
            (None, Some(audio)) => compact_codec(audio),
            (None, None) => "codec unknown".to_owned(),
        };
        let fps = self.fps.map(|fps| format!(" · {fps} fps")).unwrap_or_default();
        let size = self.size_bytes.map_or_else(String::new, |bytes| {
            format!(
                " · {}{}",
                if self.size_is_estimate { "≈" } else { "" },
                format_bytes(Some(bytes))
            )
        });
        format!(
            "{} · {} · {} · {} · {}{fps}{size}",
            self.id, extension, resolution, streams, codecs
        )
    }
}

pub fn format_is_available(
    formats: &[MediaFormatOption],
    format_id: &str,
    mode: DownloadMode,
) -> bool {
    formats.iter().any(|format| format.id == format_id && format.supports_mode(mode))
}

fn compact_codec(value: &str) -> String {
    let compact = value.chars().take(18).collect::<String>();
    if compact.len() < value.len() { format!("{compact}…") } else { compact }
}

pub fn is_safe_format_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
        })
}

fn parse_media_format(value: &serde_json::Value) -> Option<MediaFormatOption> {
    let id = text(value, "format_id")?;
    if !is_safe_format_id(&id) {
        return None;
    }
    let codec = |field: &str| {
        text(value, field)
            .filter(|codec| !codec.trim().is_empty() && !codec.eq_ignore_ascii_case("none"))
            .map(|codec| {
                codec.chars().filter(|character| !character.is_control()).take(64).collect()
            })
    };
    let size_bytes = value
        .get("filesize")
        .and_then(serde_json::Value::as_u64)
        .or_else(|| value.get("filesize_approx").and_then(serde_json::Value::as_u64));
    let size_is_estimate =
        value.get("filesize").and_then(serde_json::Value::as_u64).is_none() && size_bytes.is_some();
    Some(MediaFormatOption {
        id,
        extension: text(value, "ext")
            .map(|extension| extension.to_ascii_lowercase())
            .filter(|extension| {
                !extension.is_empty()
                    && extension.len() <= 16
                    && extension.chars().all(|character| character.is_ascii_alphanumeric())
            })
            .unwrap_or_default(),
        width: value
            .get("width")
            .and_then(serde_json::Value::as_u64)
            .and_then(|v| u32::try_from(v).ok()),
        height: value
            .get("height")
            .and_then(serde_json::Value::as_u64)
            .and_then(|v| u32::try_from(v).ok()),
        fps: value
            .get("fps")
            .and_then(serde_json::Value::as_f64)
            .filter(|fps| fps.is_finite() && (0.0..=1000.0).contains(fps)),
        size_bytes,
        size_is_estimate,
        video_codec: codec("vcodec"),
        audio_codec: codec("acodec"),
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlaylistEntry {
    pub playlist_index: Option<i64>,
    pub title: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlaylistMediaEntry {
    pub media_id: Option<String>,
    pub media_url: String,
    pub playlist_index: Option<i64>,
    pub title: String,
    pub available: bool,
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
        let subscription_entries = entries.map_or_else(Vec::new, |items| {
            items
                .iter()
                .filter(|entry| entry.is_object())
                .map(|entry| {
                    let media_url = ["webpage_url", "original_url", "url"]
                        .into_iter()
                        .filter_map(|field| text(entry, field))
                        .find(|candidate| is_http_url(candidate))
                        .unwrap_or_default();
                    let explicitly_unavailable =
                        text(entry, "availability").is_some_and(|availability| {
                            matches!(
                                availability.to_ascii_lowercase().as_str(),
                                "unavailable" | "private" | "needs_auth" | "premium_only"
                            )
                        });
                    PlaylistMediaEntry {
                        media_id: text(entry, "id"),
                        media_url: media_url.clone(),
                        playlist_index: playlist_index(entry),
                        title: text(entry, "title")
                            .unwrap_or_else(|| "Untitled playlist item".to_owned()),
                        available: !media_url.is_empty() && !explicitly_unavailable,
                    }
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
            formats: first_entry
                .get("formats")
                .and_then(serde_json::Value::as_array)
                .map(|formats| formats.iter().take(100).filter_map(parse_media_format).collect())
                .unwrap_or_default(),
            is_playlist,
            playlist_count: entries.map(Vec::len),
            playlist_entries,
            subscription_entries,
            manual_subtitle_languages: subtitle_languages(first_entry, "subtitles"),
            automatic_subtitle_languages: subtitle_languages(first_entry, "automatic_captions"),
        }
    }
}

fn is_http_url(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|parsed| {
        matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some()
    })
}

fn subtitle_languages(value: &serde_json::Value, field: &str) -> Vec<String> {
    let mut languages = value
        .get(field)
        .and_then(serde_json::Value::as_object)
        .map(|subtitles| subtitles.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    languages.sort_unstable();
    languages
}

fn playlist_index(value: &serde_json::Value) -> Option<i64> {
    value.get("playlist_index").and_then(serde_json::Value::as_i64).filter(|index| *index > 0)
}

#[derive(Clone, Debug)]
pub struct DownloadRecord {
    pub id: String,
    pub url: String,
    /// Persisted queue position; equal ranks are resolved by SQLite rowid.
    pub queue_order: i64,
    /// A value of 1 runs ahead of normal priority (0) items.
    pub priority: i32,
    pub playlist_index: Option<i64>,
    /// Shared durable identifier for entries queued from the same playlist selection.
    pub playlist_group_id: Option<String>,
    /// Playlist title shown in queue controls for this selection group.
    pub playlist_group_title: Option<String>,
    pub download_mode: DownloadMode,
    pub subtitle_languages: String,
    pub subtitle_format: SubtitleFormat,
    pub include_auto_subtitles: bool,
    pub audio_format: AudioFormat,
    pub audio_quality: AudioQuality,
    pub video_quality: VideoQuality,
    /// Media-specific yt-dlp format ID; never stores its signed stream URL.
    pub selected_format_id: Option<String>,
    /// Destination directory captured when this queue item was added.
    pub download_directory: String,
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
    /// Number of automatic retries already scheduled for transient failures.
    pub retry_attempts: u32,
    /// Unix timestamp when a scheduled automatic retry becomes eligible.
    pub next_retry_at: Option<i64>,
    /// Unix timestamp when a one-time scheduled download becomes eligible.
    /// Present only while the scheduled item is waiting or paused before its run time.
    pub scheduled_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl DownloadRecord {
    pub fn queued(id: String, url: String, created_at: i64) -> Self {
        Self {
            id,
            url,
            queue_order: 0,
            priority: 0,
            playlist_index: None,
            playlist_group_id: None,
            playlist_group_title: None,
            download_mode: DownloadMode::Video,
            subtitle_languages: "en".to_owned(),
            subtitle_format: SubtitleFormat::Vtt,
            include_auto_subtitles: false,
            audio_format: AudioFormat::Original,
            audio_quality: AudioQuality::Best,
            video_quality: VideoQuality::Best,
            selected_format_id: None,
            download_directory: String::new(),
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
            retry_attempts: 0,
            next_retry_at: None,
            scheduled_at: None,
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
    fn format_parser_keeps_display_metadata_and_discards_signed_urls() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"id":"format-sample","title":"Formats","formats":[{"format_id":"137","ext":"mp4","width":1920,"height":1080,"fps":30,"filesize_approx":12000000,"vcodec":"avc1.640028","acodec":"none","url":"https://cdn.example.test/media?token=secret-signed-token"},{"format_id":"140","ext":"m4a","vcodec":"none","acodec":"mp4a.40.2","filesize":4000000,"url":"https://cdn.example.test/audio?token=another-secret"},{"format_id":"bad/id","ext":"mp4","vcodec":"avc1","acodec":"none"}]}"#,
        )
        .unwrap();
        let parsed = MediaMetadata::from_yt_dlp_json_for_playlist_item(
            &value,
            "https://example.test/watch",
            None,
        );

        assert_eq!(parsed.format_count, 3);
        assert_eq!(parsed.formats.len(), 2);
        assert_eq!(parsed.formats[0].id, "137");
        assert!(parsed.formats[0].size_is_estimate);
        assert!(parsed.formats[0].supports_mode(DownloadMode::VideoOnly));
        assert!(!parsed.formats[0].supports_mode(DownloadMode::Video));
        assert!(parsed.formats[1].supports_mode(DownloadMode::AudioOnly));
        assert!(!parsed.formats[1].supports_mode(DownloadMode::VideoOnly));
        let serialized = serde_json::to_string(&parsed.formats).unwrap();
        assert!(!serialized.contains("cdn.example.test"));
        assert!(!serialized.contains("signed-token"));
        assert!(!serialized.contains("another-secret"));
    }

    #[test]
    fn format_id_validation_rejects_argument_syntax() {
        for valid in ["137", "dash-1", "format_audio.2"] {
            assert!(is_safe_format_id(valid), "expected {valid:?} to be safe");
        }
        assert!(is_safe_format_id(&"a".repeat(128)));
        for invalid in ["", "-", "137/140", "137+best", "--exec", "a b", "x\n--format"] {
            assert!(!is_safe_format_id(invalid), "expected {invalid:?} to be rejected");
        }
        assert!(!is_safe_format_id(&"a".repeat(129)));
    }

    #[test]
    fn exact_format_must_still_exist_and_match_the_download_mode() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"id":"format-sample","formats":[{"format_id":"video-1","vcodec":"avc1","acodec":"none"},{"format_id":"audio-1","vcodec":"none","acodec":"opus"}]}"#,
        )
        .unwrap();
        let metadata = MediaMetadata::from_yt_dlp_json_for_playlist_item(
            &value,
            "https://example.test/watch",
            None,
        );
        assert!(format_is_available(&metadata.formats, "video-1", DownloadMode::VideoOnly));
        assert!(!format_is_available(&metadata.formats, "video-1", DownloadMode::Video));
        assert!(!format_is_available(&metadata.formats, "retired-id", DownloadMode::VideoOnly));
        assert!(!format_is_available(&metadata.formats, "audio-1", DownloadMode::VideoOnly));
    }

    #[test]
    fn metadata_parser_reports_manual_and_automatic_subtitle_languages() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"id":"captions","title":"Caption sample","subtitles":{"fr":[{"ext":"vtt"}],"en":[{"ext":"srt"}]},"automatic_captions":{"es":[{"ext":"vtt"}],"en":[{"ext":"vtt"}]}}"#,
        )
        .unwrap();

        let metadata = MediaMetadata::from_yt_dlp_json_for_playlist_item(
            &value,
            "https://example.test/captions",
            None,
        );
        assert_eq!(metadata.manual_subtitle_languages, ["en", "fr"]);
        assert_eq!(metadata.automatic_subtitle_languages, ["en", "es"]);

        let without_captions: serde_json::Value =
            serde_json::from_str(r#"{"id":"no-captions","title":"No captions"}"#).unwrap();
        let metadata = MediaMetadata::from_yt_dlp_json_for_playlist_item(
            &without_captions,
            "https://example.test/no-captions",
            None,
        );
        assert!(metadata.manual_subtitle_languages.is_empty());
        assert!(metadata.automatic_subtitle_languages.is_empty());
    }

    #[test]
    fn subtitle_choices_have_stable_database_values() {
        assert_eq!(
            DownloadMode::from_database_value("subtitles"),
            Some(DownloadMode::SubtitlesOnly)
        );
        assert_eq!(SubtitleFormat::from_database_value("srt"), Some(SubtitleFormat::Srt));
        assert_eq!(SubtitleFormat::from_database_value("vtt"), Some(SubtitleFormat::Vtt));
        assert_eq!(SubtitleFormat::from_database_value("ass"), None);
    }

    #[test]
    fn subtitle_language_selection_accepts_codes_and_rejects_ambiguous_input() {
        assert_eq!(normalize_subtitle_languages("en, fr-CA"), Some("en,fr-CA".to_owned()));
        assert_eq!(normalize_subtitle_languages("all"), Some("all".to_owned()));
        assert!(normalize_subtitle_languages("all,en").is_none());
        assert!(normalize_subtitle_languages("en,en").is_none());
        assert!(normalize_subtitle_languages("en;--exec").is_none());
        assert!(subtitle_languages_available("EN,fr", &["en".to_owned(), "fr".to_owned()]));
        assert!(!subtitle_languages_available("EN,fr", &["en".to_owned()]));
        assert!(!subtitle_languages_available("fr", &["en".to_owned()]));
        assert!(subtitle_languages_available("all", &["ja".to_owned()]));
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
    fn playlist_subscription_entries_keep_stable_ids_and_only_addressable_urls() {
        let playlist: serde_json::Value = serde_json::from_str(
            r#"{"title":"Playlist","extractor_key":"Example","entries":[{"id":"one","title":"First","playlist_index":1,"webpage_url":"https://example.test/watch/one"},{"id":"two","title":"Unavailable","playlist_index":2,"url":"two","availability":"unavailable"},{"id":"three","title":"Private","playlist_index":3,"webpage_url":"https://example.test/watch/three","availability":"private"}]}"#,
        )
        .unwrap();

        let metadata = MediaMetadata::from_yt_dlp_json_for_playlist_item(
            &playlist,
            "https://example.test/list",
            None,
        );

        assert_eq!(metadata.subscription_entries.len(), 3);
        assert_eq!(metadata.subscription_entries[0].media_id.as_deref(), Some("one"));
        assert_eq!(metadata.subscription_entries[0].media_url, "https://example.test/watch/one");
        assert!(metadata.subscription_entries[0].available);
        assert!(!metadata.subscription_entries[1].available);
        assert!(metadata.subscription_entries[1].media_url.is_empty());
        assert!(!metadata.subscription_entries[2].available);
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
        for (mode, value) in [
            (DownloadMode::Video, "video"),
            (DownloadMode::VideoOnly, "video_only"),
            (DownloadMode::AudioOnly, "audio"),
            (DownloadMode::SubtitlesOnly, "subtitles"),
        ] {
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

    #[test]
    fn video_quality_has_stable_persistence_and_format_constraints() {
        for (quality, stored, format) in [
            (VideoQuality::Best, "best", "bv*+ba/b"),
            (VideoQuality::P2160, "2160p", "bestvideo[height<=2160]+bestaudio/best[height<=2160]"),
            (VideoQuality::P1440, "1440p", "bestvideo[height<=1440]+bestaudio/best[height<=1440]"),
            (VideoQuality::P1080, "1080p", "bestvideo[height<=1080]+bestaudio/best[height<=1080]"),
            (VideoQuality::P720, "720p", "bestvideo[height<=720]+bestaudio/best[height<=720]"),
            (VideoQuality::P480, "480p", "bestvideo[height<=480]+bestaudio/best[height<=480]"),
            (VideoQuality::P360, "360p", "bestvideo[height<=360]+bestaudio/best[height<=360]"),
            (VideoQuality::Lowest, "lowest", "worstvideo+worstaudio/worst"),
        ] {
            assert_eq!(VideoQuality::from_database_value(stored), Some(quality));
            assert_eq!(VideoQuality::from_ui_label(quality.label()), Some(quality));
            assert_eq!(quality.as_database_value(), stored);
            assert_eq!(quality.yt_dlp_format(), format);
        }
        for (quality, format) in [
            (VideoQuality::Best, "bestvideo"),
            (VideoQuality::P2160, "bestvideo[height<=2160]"),
            (VideoQuality::P1440, "bestvideo[height<=1440]"),
            (VideoQuality::P1080, "bestvideo[height<=1080]"),
            (VideoQuality::P720, "bestvideo[height<=720]"),
            (VideoQuality::P480, "bestvideo[height<=480]"),
            (VideoQuality::P360, "bestvideo[height<=360]"),
            (VideoQuality::Lowest, "worstvideo"),
        ] {
            assert_eq!(quality.yt_dlp_video_only_format(), format);
            assert!(!format.contains("audio"));
            assert!(!format.contains("/"));
        }
        assert_eq!(VideoQuality::from_database_value("8k"), None);
    }

    #[test]
    fn new_download_presets_parse_valid_values_and_fall_back_per_setting() {
        let saved = DownloadPresets::from_database_values(
            Some("video_only"),
            Some("flac"),
            Some("192k"),
            Some("1440p"),
        );
        assert_eq!(saved.mode, DownloadMode::VideoOnly);
        assert_eq!(saved.audio_format, AudioFormat::Flac);
        assert_eq!(saved.audio_quality, AudioQuality::K192);
        assert_eq!(saved.video_quality, VideoQuality::P1440);

        let recovered = DownloadPresets::from_database_values(
            Some("invalid-mode"),
            Some("mp3"),
            Some("invalid-bitrate"),
            Some("720p"),
        );
        assert_eq!(recovered.mode, DownloadMode::Video);
        assert_eq!(recovered.audio_format, AudioFormat::Mp3);
        assert_eq!(recovered.audio_quality, AudioQuality::Best);
        assert_eq!(recovered.video_quality, VideoQuality::P720);
        assert_eq!(DownloadPresets::default(), recovered_default_presets());
    }

    #[test]
    fn new_download_presets_accept_ui_video_labels_and_stable_audio_values() {
        let presets = DownloadPresets::from_ui_values(false, false, "opus", "320k", "2160p / 4K");
        assert_eq!(presets.mode, DownloadMode::Video);
        assert_eq!(presets.audio_format, AudioFormat::Opus);
        assert_eq!(presets.audio_quality, AudioQuality::K320);
        assert_eq!(presets.video_quality, VideoQuality::P2160);
        assert_eq!(
            presets.database_values(),
            [
                ("new_download_mode", "video"),
                ("new_download_audio_format", "opus"),
                ("new_download_audio_quality", "320k"),
                ("new_download_video_quality", "2160p"),
            ]
        );
    }

    #[test]
    fn video_only_ui_preset_is_distinct_from_video_with_audio() {
        let presets = DownloadPresets::from_ui_values(false, true, "mp3", "320k", "720p");
        assert_eq!(presets.mode, DownloadMode::VideoOnly);
        assert_eq!(presets.database_values()[0], ("new_download_mode", "video_only"));
        assert_eq!(
            DownloadPresets::from_ui_values(false, false, "original", "best", "Best").mode,
            DownloadMode::Video
        );
        assert_eq!(
            DownloadPresets::from_ui_values(true, true, "original", "best", "Best").mode,
            DownloadMode::AudioOnly
        );
    }

    fn recovered_default_presets() -> DownloadPresets {
        DownloadPresets::from_database_values(None, None, None, None)
    }
}
