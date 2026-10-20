use std::{env, fs, path::PathBuf};

fn main() {
    let branding_path = PathBuf::from("../../branding.json");
    println!("cargo:rerun-if-changed={}", branding_path.display());
    println!("cargo:rerun-if-changed=../../assets/yt-save.ico");
    println!("cargo:rerun-if-changed=../../assets/yt-save.png");

    let branding: serde_json::Value =
        serde_json::from_slice(&fs::read(&branding_path).expect("branding.json is required"))
            .expect("branding.json must be valid JSON");
    let product_name = branding["product_name"].as_str().expect("product_name must be a string");
    let package_name = branding["package_name"].as_str().expect("package_name must be a string");
    let publisher = branding["publisher"].as_str().expect("publisher must be a string");
    let bundle_identifier =
        branding["bundle_identifier"].as_str().expect("bundle_identifier must be a string");
    let github_owner = branding["github_owner"].as_str().expect("github_owner must be a string");
    let github_repository =
        branding["github_repository"].as_str().expect("github_repository must be a string");
    assert!(
        !github_owner.is_empty()
            && github_owner.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "github_owner must be a GitHub owner slug"
    );
    assert!(
        !github_repository.is_empty()
            && github_repository
                .bytes()
                .all(|byte| { byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') })
            && github_repository != "."
            && github_repository != "..",
        "github_repository must be a GitHub repository slug"
    );
    assert!(
        !package_name.is_empty()
            && package_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            && package_name != "."
            && package_name != "..",
        "package_name must be a safe package slug"
    );
    let configured_version = branding["version"].as_str().expect("version must be a string");
    let cargo_version = env::var("CARGO_PKG_VERSION").expect("Cargo supplies package version");
    assert_eq!(configured_version, cargo_version, "keep branding.json and Cargo version in sync");

    println!("cargo:rustc-env=YT_SAVE_PRODUCT_NAME={product_name}");
    println!("cargo:rustc-env=YT_SAVE_PACKAGE_NAME={package_name}");
    println!("cargo:rustc-env=YT_SAVE_PRODUCT_VERSION={configured_version}");
    println!("cargo:rustc-env=YT_SAVE_PUBLISHER={publisher}");
    println!("cargo:rustc-env=YT_SAVE_BUNDLE_IDENTIFIER={bundle_identifier}");
    println!("cargo:rustc-env=YT_SAVE_GITHUB_OWNER={github_owner}");
    println!("cargo:rustc-env=YT_SAVE_GITHUB_REPOSITORY={github_repository}");

    slint_build::compile("ui/main.slint").expect("Slint UI compilation failed");

    #[cfg(windows)]
    {
        let mut resource = winresource::WindowsResource::new();
        resource.set_icon("../../assets/yt-save.ico");
        resource.compile().expect("failed to embed the Yt Save Windows icon");
    }
}
