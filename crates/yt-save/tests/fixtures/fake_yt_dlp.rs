use std::{env, fs, path::PathBuf};

fn main() {
    let arguments = env::args().collect::<Vec<_>>();
    if arguments.iter().any(|argument| argument == "--dump-single-json") {
        println!(
            "{}",
            r#"{"id":"fixture-123","title":"Fixture media","webpage_url":"https://example.test/media","extractor_key":"Fixture","duration":1,"formats":[]}"#
        );
        return;
    }

    let Some(output_directory) = arguments
        .iter()
        .find_map(|argument| argument.strip_prefix("home:"))
        .map(PathBuf::from)
    else {
        eprintln!("missing output directory");
        std::process::exit(2);
    };
    if let Err(error) = fs::create_dir_all(&output_directory) {
        eprintln!("could not create output directory: {error}");
        std::process::exit(2);
    }
    let subtitles_only = arguments.iter().any(|argument| argument == "--skip-download");
    let output_path = if subtitles_only {
        output_directory.join("Fixture media [fixture-123].en.srt")
    } else {
        output_directory.join("Fixture media [fixture-123].mp4")
    };
    let contents = if subtitles_only {
        b"1\n00:00:00,000 --> 00:00:01,000\nfixture subtitle\n".as_slice()
    } else {
        b"tiny deterministic media fixture".as_slice()
    };
    if let Err(error) = fs::write(&output_path, contents) {
        eprintln!("could not write fixture media: {error}");
        std::process::exit(2);
    }

    println!("__YT_SAVE_PROGRESS__downloading|32|32|32|NA|0");
    println!("__YT_SAVE_PROGRESS__finished|32|32|32|NA|0");
    if subtitles_only {
        let escaped = output_path.to_string_lossy().replace('\\', "\\\\").replace('"', "\\\"");
        println!("__YT_SAVE_SUBTITLE_OUTPUT__{{\"en\":{{\"ext\":\"srt\",\"filepath\":\"{escaped}\"}}}}");
    } else {
        println!("__YT_SAVE_OUTPUT__{}", output_path.display());
    }
}
