use std::path::Path;

use slint::SharedString;
use tokio::sync::mpsc;

use crate::{
    LibraryRowData,
    model::{DownloadMode, DownloadRecord, format_bytes},
};

use super::UiEvent;

pub(super) type LibraryCacheSignature =
    Vec<(String, String, String, String, Option<String>, String, Option<u64>, i64)>;

#[derive(Clone)]
pub(super) struct LibraryCacheEntry {
    row: LibraryRowData,
    searchable_text: String,
    title_sort_key: String,
    downloaded_at: i64,
    size_for_sort: Option<u64>,
}

pub(super) fn library_cache_signature_for(records: &[DownloadRecord]) -> LibraryCacheSignature {
    records
        .iter()
        .filter(|record| record.status == "Completed")
        .map(|record| {
            (
                record.id.clone(),
                record.url.clone(),
                record.title.clone(),
                record.source.clone(),
                record.output_path.clone(),
                record.download_mode.as_database_value().to_owned(),
                record.total_bytes,
                record.updated_at,
            )
        })
        .collect()
}

pub(super) fn request_library_scan(
    events: mpsc::UnboundedSender<UiEvent>,
    generation: &mut u64,
    pending_signature: &mut Option<LibraryCacheSignature>,
    records: Vec<DownloadRecord>,
    signature: LibraryCacheSignature,
) {
    *generation = generation.wrapping_add(1);
    let request_generation = *generation;
    *pending_signature = Some(signature.clone());
    tokio::spawn(async move {
        let result = match tokio::task::spawn_blocking(move || scan_library_entries(&records)).await
        {
            Ok(entries) => Ok(entries),
            Err(error) => Err(error.to_string()),
        };
        let _ = events.send(UiEvent::LibraryScanComplete {
            generation: request_generation,
            signature,
            result,
        });
    });
}

pub(super) fn scan_library_entries(records: &[DownloadRecord]) -> Vec<LibraryCacheEntry> {
    records
        .iter()
        .filter(|record| record.status == "Completed")
        .map(|record| {
            let path = record.output_path.as_deref().unwrap_or_default();
            let metadata = if path.is_empty() {
                None
            } else {
                std::fs::metadata(path).ok().filter(|metadata| metadata.is_file())
            };
            let path_available = metadata.is_some();
            let kind = match record.download_mode {
                DownloadMode::Video | DownloadMode::VideoOnly => "Video",
                DownloadMode::AudioOnly => "Audio",
                DownloadMode::SubtitlesOnly => "Subtitles",
                DownloadMode::MetadataOnly => "Metadata",
                DownloadMode::ThumbnailOnly => "Thumbnail",
            };
            let file_format = Path::new(path)
                .extension()
                .and_then(|extension| extension.to_str())
                .filter(|extension| !extension.is_empty())
                .map(|extension| extension.to_ascii_uppercase())
                .unwrap_or_else(|| "UNKNOWN".to_owned());
            let source = if record.source.trim().is_empty() {
                url::Url::parse(&record.url)
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_owned))
                    .unwrap_or_else(|| "Web media".to_owned())
            } else {
                record.source.clone()
            };
            let actual_size = metadata.as_ref().map(std::fs::Metadata::len);
            let size_for_sort = actual_size.or(record.total_bytes);
            let size_label = match actual_size {
                Some(bytes) => format!("{} on disk", format_bytes(Some(bytes))),
                None => record.total_bytes.map_or_else(
                    || "Size unknown".to_owned(),
                    |bytes| format!("Last known {}", format_bytes(Some(bytes))),
                ),
            };
            let downloaded_at = chrono::DateTime::from_timestamp(record.updated_at, 0)
                .map(|timestamp| timestamp.format("%b %d, %Y").to_string())
                .unwrap_or_else(|| "Unknown date".to_owned());
            let file_status = if path_available {
                "Available"
            } else {
                "Unavailable · locate moved file"
            };
            let searchable_text = [
                record.title.as_str(),
                source.as_str(),
                kind,
                file_format.as_str(),
                path,
                record.url.as_str(),
            ]
            .iter()
            .map(|value| value.to_lowercase())
            .collect::<Vec<_>>()
            .join(" ");
            LibraryCacheEntry {
                row: LibraryRowData {
                    id: SharedString::from(record.id.as_str()),
                    title: SharedString::from(record.title.as_str()),
                    source: SharedString::from(source),
                    kind: SharedString::from(kind),
                    file_format: SharedString::from(file_format),
                    file_path: SharedString::from(path),
                    file_status: SharedString::from(file_status),
                    size_label: SharedString::from(size_label),
                    downloaded_at: SharedString::from(downloaded_at),
                    path_available,
                },
                searchable_text,
                title_sort_key: record.title.to_lowercase(),
                downloaded_at: record.updated_at,
                size_for_sort,
            }
        })
        .collect()
}

pub(super) fn library_row_views_from_cache(
    cache: &[LibraryCacheEntry],
    query: &str,
    filter: &str,
    sort: &str,
) -> Vec<LibraryRowData> {
    let query = query.trim().to_lowercase();
    let mut rows = cache
        .iter()
        .filter(|entry| match filter {
            "Video" => entry.row.kind.as_str() == "Video",
            "Audio" => entry.row.kind.as_str() == "Audio",
            "Subtitles" => entry.row.kind.as_str() == "Subtitles",
            "Metadata" => entry.row.kind.as_str() == "Metadata",
            "Thumbnail" => entry.row.kind.as_str() == "Thumbnail",
            "Missing files" => !entry.row.path_available,
            _ => true,
        })
        .filter(|entry| query.is_empty() || entry.searchable_text.contains(&query))
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| match sort {
        "Oldest first" => left
            .downloaded_at
            .cmp(&right.downloaded_at)
            .then_with(|| left.row.id.as_str().cmp(right.row.id.as_str())),
        "Title A–Z" => left
            .title_sort_key
            .cmp(&right.title_sort_key)
            .then_with(|| left.row.id.as_str().cmp(right.row.id.as_str())),
        "Largest first" => right
            .size_for_sort
            .cmp(&left.size_for_sort)
            .then_with(|| right.downloaded_at.cmp(&left.downloaded_at))
            .then_with(|| left.row.id.as_str().cmp(right.row.id.as_str())),
        _ => right
            .downloaded_at
            .cmp(&left.downloaded_at)
            .then_with(|| left.row.id.as_str().cmp(right.row.id.as_str())),
    });
    rows.into_iter()
        .map(|entry| LibraryRowData {
            id: entry.row.id.clone(),
            title: entry.row.title.clone(),
            source: entry.row.source.clone(),
            kind: entry.row.kind.clone(),
            file_format: entry.row.file_format.clone(),
            file_path: entry.row.file_path.clone(),
            file_status: entry.row.file_status.clone(),
            size_label: entry.row.size_label.clone(),
            downloaded_at: entry.row.downloaded_at.clone(),
            path_available: entry.row.path_available,
        })
        .collect()
}
