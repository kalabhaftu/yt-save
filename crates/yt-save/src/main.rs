mod app;
mod app_update;
mod clipboard_monitor;
mod dependencies;
mod engine;
mod history;
mod model;
mod persistence;
#[cfg(windows)]
mod single_instance;
#[cfg(windows)]
mod window_activation;
mod yt_dlp_update;

slint::include_modules!();

fn main() -> anyhow::Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if arguments.first().map(String::as_str) == Some("--yt-save-update-helper") {
        let exit_code = app_update::run_update_helper(&arguments)?;
        std::process::exit(exit_code);
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    #[cfg(windows)]
    slint::BackendSelector::new()
        .backend_name("winit".into())
        .with_winit_window_attributes_hook(|attributes| {
            if window_activation::consume_non_activating_window_marker() {
                use slint::winit_030::winit::platform::windows::WindowAttributesExtWindows;

                attributes.with_active(false).with_skip_taskbar(true)
            } else {
                attributes
            }
        })
        .select()?;

    app::run()
}
