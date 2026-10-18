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
    let output_path = output_directory.join("Fixture media [fixture-123].mp4");
    if let Err(error) = fs::write(&output_path, b"tiny deterministic media fixture") {
        eprintln!("could not write fixture media: {error}");
        std::process::exit(2);
    }

    println!("__YT_SAVE_PROGRESS__downloading|32|32|32|NA|0");
    println!("__YT_SAVE_PROGRESS__finished|32|32|32|NA|0");
    println!("__YT_SAVE_OUTPUT__{}", output_path.display());
}
