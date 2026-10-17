mod app;
mod clipboard_monitor;
mod dependencies;
mod engine;
mod history;
mod model;
mod persistence;
#[cfg(windows)]
mod window_activation;

slint::include_modules!();

fn main() -> anyhow::Result<()> {
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
