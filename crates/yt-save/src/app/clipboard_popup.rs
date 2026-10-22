use super::*;

#[derive(Clone)]
pub(super) struct ClipboardPopupContext {
    pub(super) main_window: slint::Weak<MainWindow>,
    pub(super) popup_window: slint::Weak<ClipboardPopup>,
    pub(super) popup_generation: Arc<AtomicU64>,
    pub(super) shutdown_started: Arc<AtomicBool>,
}

pub(super) fn configure_clipboard_popup(popup: &ClipboardPopup, context: ClipboardPopupContext) {
    let analyze_context = context.clone();
    popup.on_analyze_requested(move |url| {
        dismiss_clipboard_popup(&analyze_context);
        if let Some(window) = analyze_context.main_window.upgrade() {
            let url = url.to_string();
            window.set_url_input(url.clone().into());
            window.set_status_message("Reading media details…".into());
            if let Err(error) = window.show() {
                error!(%error, "show window to analyze copied link");
                return;
            }
            window.invoke_analyze_requested(url.into());
        }
    });

    let video_context = context.clone();
    popup.on_add_video_requested(move |url| {
        queue_clipboard_candidate(url.to_string(), false, video_context.clone());
    });

    let audio_context = context.clone();
    popup.on_add_audio_requested(move |url| {
        queue_clipboard_candidate(url.to_string(), true, audio_context.clone());
    });

    let options_context = context.clone();
    popup.on_more_options_requested(move |url| {
        dismiss_clipboard_popup(&options_context);
        if let Some(window) = options_context.main_window.upgrade() {
            window.set_url_input(url);
            if let Err(error) = window.show() {
                error!(%error, "show window for copied-link options");
            }
        }
    });

    popup.on_dismiss_requested(move || dismiss_clipboard_popup(&context));
}

fn dismiss_clipboard_popup(context: &ClipboardPopupContext) {
    context.popup_generation.fetch_add(1, Ordering::Relaxed);
    if let Some(window) = context.main_window.upgrade() {
        window.set_clipboard_candidate(SharedString::default());
    }
    if let Some(popup) = context.popup_window.upgrade()
        && let Err(error) = popup.hide()
    {
        error!(%error, "hide clipboard review popup");
    }
}

fn queue_clipboard_candidate(url: String, audio_only: bool, context: ClipboardPopupContext) {
    dismiss_clipboard_popup(&context);
    if context.shutdown_started.load(Ordering::Acquire) {
        if let Some(window) = context.main_window.upgrade() {
            window.set_status_message("The application is shutting down.".into());
        }
        return;
    }
    let url = match validate_url(&url) {
        Ok(url) => url,
        Err(message) => {
            if let Some(window) = context.main_window.upgrade() {
                window.set_status_message(message.into());
            }
            return;
        }
    };

    if let Some(window) = context.main_window.upgrade() {
        window.set_url_input(url.clone().into());
        window.set_audio_only(audio_only);
        window.set_video_only(false);
        window.set_subtitle_only(false);
        window.set_format_choices(ModelRc::new(VecModel::from(vec![SharedString::from(
            AUTOMATIC_FORMAT_CHOICE,
        )])));
        window.set_selected_format(SharedString::from(AUTOMATIC_FORMAT_CHOICE));
        window.set_format_choice_mode(SharedString::default());
        window.set_format_choice_source_url(SharedString::default());
        window.set_format_choice_display_url(SharedString::default());
        window.set_format_choice_list_truncated(false);
        window.set_status_message("Checking the copied link before adding it to the queue…".into());
        if let Err(error) = window.show() {
            error!(%error, "show window to add copied link");
            return;
        }
        window.invoke_enqueue_requested(url.into());
    }
}

pub(super) fn clipboard_source_host(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_else(|| "Web media".to_owned())
}

pub(super) fn initial_popup_position(
    area: WorkArea,
    scale: f32,
    logical_width: i32,
    logical_height: i32,
) -> (i32, i32) {
    let scale = scale.max(1.0);
    let width = (logical_width as f32 * scale).round() as i32;
    let height = (logical_height as f32 * scale).round() as i32;
    (
        area.right.saturating_sub(width).saturating_sub(16).max(area.left.saturating_add(16)),
        area.bottom.saturating_sub(height).saturating_sub(16).max(area.top.saturating_add(16)),
    )
}

#[cfg(windows)]
pub(super) fn configure_clipboard_popup_window(
    popup: &ClipboardPopup,
    work_area: Arc<Mutex<Option<WorkArea>>>,
) {
    use slint::winit_030::{EventResult, WinitWindowAccessor, winit};

    let native_style_ready = Arc::new(AtomicBool::new(false));
    popup.window().on_winit_window_event(move |slint_window, event| {
        if matches!(
            event,
            winit::event::WindowEvent::RedrawRequested
                | winit::event::WindowEvent::Resized(_)
                | winit::event::WindowEvent::ScaleFactorChanged { .. }
        ) {
            let work_area = work_area.lock().ok().and_then(|area| *area);
            let native_style_ready = native_style_ready.clone();
            slint_window.with_winit_window(|window| {
                if !native_style_ready.load(Ordering::Acquire) && set_window_no_activate(window) {
                    native_style_ready.store(true, Ordering::Release);
                }

                if let Some(area) = work_area {
                    let size = window.inner_size();
                    let width = i32::try_from(size.width).unwrap_or(i32::MAX);
                    let height = i32::try_from(size.height).unwrap_or(i32::MAX);
                    let x = area
                        .right
                        .saturating_sub(width)
                        .saturating_sub(16)
                        .max(area.left.saturating_add(16));
                    let y = area
                        .bottom
                        .saturating_sub(height)
                        .saturating_sub(16)
                        .max(area.top.saturating_add(16));
                    window.set_outer_position(winit::dpi::PhysicalPosition::new(x, y));
                }
            });
        }

        EventResult::Propagate
    });
}

#[cfg(windows)]
pub(super) fn main_window_has_focus(window: &MainWindow) -> bool {
    use slint::winit_030::WinitWindowAccessor;
    window.window().with_winit_window(|window| window.has_focus()).unwrap_or(false)
}

#[cfg(windows)]
fn set_window_no_activate(window: &slint::winit_030::winit::window::Window) -> bool {
    use slint::winit_030::winit::raw_window_handle::{HasWindowHandle, RawWindowHandle};
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        GWL_EXSTYLE, GetWindowLongPtrW, SWP_FRAMECHANGED, SWP_NOACTIVATE, SWP_NOMOVE, SWP_NOSIZE,
        SWP_NOZORDER, SetWindowLongPtrW, SetWindowPos, WS_EX_NOACTIVATE, WS_EX_TOOLWINDOW,
    };

    let Ok(handle) = window.window_handle() else {
        return false;
    };
    let RawWindowHandle::Win32(handle) = handle.as_raw() else {
        return false;
    };
    let hwnd = handle.hwnd.get() as *mut std::ffi::c_void;
    unsafe {
        let style = GetWindowLongPtrW(hwnd, GWL_EXSTYLE);
        let required_style = (WS_EX_NOACTIVATE | WS_EX_TOOLWINDOW) as isize;
        SetWindowLongPtrW(hwnd, GWL_EXSTYLE, style | required_style);
        let updated_style = GetWindowLongPtrW(hwnd, GWL_EXSTYLE);
        if updated_style & required_style != required_style {
            return false;
        }
        SetWindowPos(
            hwnd,
            std::ptr::null_mut(),
            0,
            0,
            0,
            0,
            SWP_FRAMECHANGED | SWP_NOACTIVATE | SWP_NOMOVE | SWP_NOSIZE | SWP_NOZORDER,
        ) != 0
    }
}

pub(super) fn start_clipboard_monitor(
    slot: &Arc<Mutex<Option<ClipboardMonitor>>>,
    generation: &AtomicU64,
    events: mpsc::UnboundedSender<UiEvent>,
) -> Result<()> {
    let mut monitor_slot =
        slot.lock().map_err(|_| anyhow::anyhow!("clipboard monitor state is unavailable"))?;
    if monitor_slot.is_some() {
        return Ok(());
    }

    let (monitor, candidates) = ClipboardMonitor::start()?;
    let generation = generation.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    let dispatcher = std::thread::Builder::new()
        .name("yt-save-clipboard-events".to_owned())
        .spawn(move || {
            while let Ok(candidate) = candidates.recv() {
                if events.send(UiEvent::ClipboardCandidate { generation, candidate }).is_err() {
                    break;
                }
            }
        })
        .context("start clipboard candidate dispatcher")?;
    drop(dispatcher);
    *monitor_slot = Some(monitor);
    Ok(())
}

pub(super) fn stop_clipboard_monitor(
    slot: &Arc<Mutex<Option<ClipboardMonitor>>>,
    generation: &AtomicU64,
) -> Result<()> {
    let mut monitor_slot =
        slot.lock().map_err(|_| anyhow::anyhow!("clipboard monitor state is unavailable"))?;
    generation.fetch_add(1, Ordering::Relaxed);
    let monitor = monitor_slot.take();
    drop(monitor_slot);
    drop(monitor);
    Ok(())
}
