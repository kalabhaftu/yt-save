use super::*;

fn library_row_views(
    records: &[DownloadRecord],
    query: &str,
    filter: &str,
    sort: &str,
) -> Vec<LibraryRowData> {
    let cache = scan_library_entries(records);
    library_row_views_from_cache(&cache, query, filter, sort)
}

mod library_media;
mod playlists;
mod queue;
mod url_download_parsing;
