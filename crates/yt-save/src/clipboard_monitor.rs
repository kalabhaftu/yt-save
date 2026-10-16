//! Event-driven Windows clipboard URL monitoring.
//!
//! Clipboard contents are examined only when Windows reports a clipboard update. Only a
//! single, locally validated HTTP(S) URL is sent to the consumer; clipboard text is never
//! persisted or sent over the network by this module.

#[cfg(windows)]
mod windows_impl {

    use std::cell::RefCell;
    use std::ffi::c_void;
    use std::ptr::{null, null_mut};
    use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
    use std::thread::{self, JoinHandle};

    use anyhow::{Context, Result, anyhow};
    use url::Url;
    use windows_sys::Win32::Foundation::{GetLastError, HWND, LPARAM, WPARAM};
    use windows_sys::Win32::System::DataExchange::{
        AddClipboardFormatListener, CloseClipboard, GetClipboardData, GetClipboardSequenceNumber,
        OpenClipboard, RemoveClipboardFormatListener,
    };
    use windows_sys::Win32::System::LibraryLoader::GetModuleHandleW;
    use windows_sys::Win32::System::Memory::{GlobalLock, GlobalSize, GlobalUnlock};
    use windows_sys::Win32::System::Threading::GetCurrentThreadId;
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        CreateWindowExW, DefWindowProcW, DestroyWindow, DispatchMessageW, GetMessageW,
        HWND_MESSAGE, MSG, PM_NOREMOVE, PeekMessageW, PostThreadMessageW, RegisterClassW,
        TranslateMessage, UnregisterClassW, WM_CLIPBOARDUPDATE, WM_QUIT, WNDCLASSW,
    };

    const CF_UNICODETEXT: u32 = 13;
    const MAX_CLIPBOARD_BYTES: usize = 64 * 1024;
    const MAX_URL_BYTES: usize = 8 * 1024;

    thread_local! {
        /// Window callbacks run on the monitor's own message-loop thread, so all mutable Win32
        /// listener state stays local to that thread.
        static THREAD_STATE: RefCell<Option<ThreadState>> = const { RefCell::new(None) };
    }

    struct ThreadState {
        events: Sender<String>,
        sequences: SequenceDeduper,
    }

    /// Owns the background Win32 message loop that watches clipboard updates.
    pub struct ClipboardMonitor {
        thread_id: u32,
        worker: Option<JoinHandle<()>>,
    }

    impl ClipboardMonitor {
        /// Starts the hidden message-only window and returns a channel of validated URL candidates.
        ///
        /// Startup completes only after the window has registered as a clipboard listener, so any
        /// Win32 setup failure is returned to the caller instead of being reported asynchronously.
        pub fn start() -> Result<(Self, Receiver<String>)> {
            let (events_tx, events_rx) = mpsc::channel();
            let (startup_tx, startup_rx) = mpsc::sync_channel(1);
            let worker = thread::Builder::new()
                .name("yt-save-clipboard".to_owned())
                .spawn(move || monitor_thread(events_tx, startup_tx))
                .context("starting clipboard monitor thread")?;

            match startup_rx.recv() {
                Ok(Ok(thread_id)) => Ok((Self { thread_id, worker: Some(worker) }, events_rx)),
                Ok(Err(message)) => {
                    let _ = worker.join();
                    Err(anyhow!(message))
                }
                Err(error) => {
                    let _ = worker.join();
                    Err(anyhow!("clipboard monitor exited during startup: {error}"))
                }
            }
        }
    }

    impl Drop for ClipboardMonitor {
        fn drop(&mut self) {
            // monitor_thread creates its message queue before announcing successful startup.
            let _ = unsafe { PostThreadMessageW(self.thread_id, WM_QUIT, 0, 0) };
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }

    fn monitor_thread(
        events: Sender<String>,
        startup: SyncSender<std::result::Result<u32, String>>,
    ) {
        let thread_id = unsafe { GetCurrentThreadId() };
        let module = unsafe { GetModuleHandleW(null()) };
        if module.is_null() {
            let _ = startup.send(Err(last_error("getting the application module handle")));
            return;
        }

        THREAD_STATE.with(|state| {
            *state.borrow_mut() =
                Some(ThreadState { events, sequences: SequenceDeduper::default() });
        });

        let class_name: Vec<u16> =
            format!("YtSaveClipboardMonitor_{thread_id}").encode_utf16().chain(Some(0)).collect();
        let class = WNDCLASSW {
            style: 0,
            lpfnWndProc: Some(monitor_window_proc),
            cbClsExtra: 0,
            cbWndExtra: 0,
            hInstance: module,
            hIcon: null_mut(),
            hCursor: null_mut(),
            hbrBackground: null_mut(),
            lpszMenuName: null(),
            lpszClassName: class_name.as_ptr(),
        };

        if unsafe { RegisterClassW(&class) } == 0 {
            THREAD_STATE.with(|state| *state.borrow_mut() = None);
            let _ = startup.send(Err(last_error("registering the clipboard monitor window class")));
            return;
        }

        let window = unsafe {
            CreateWindowExW(
                0,
                class_name.as_ptr(),
                class_name.as_ptr(),
                0,
                0,
                0,
                0,
                0,
                HWND_MESSAGE,
                null_mut(),
                module,
                null::<c_void>(),
            )
        };
        if window.is_null() {
            let message = last_error("creating the clipboard monitor window");
            unsafe { UnregisterClassW(class_name.as_ptr(), module) };
            THREAD_STATE.with(|state| *state.borrow_mut() = None);
            let _ = startup.send(Err(message));
            return;
        }

        if unsafe { AddClipboardFormatListener(window) } == 0 {
            let message = last_error("registering for clipboard updates");
            unsafe {
                DestroyWindow(window);
                UnregisterClassW(class_name.as_ptr(), module);
            }
            THREAD_STATE.with(|state| *state.borrow_mut() = None);
            let _ = startup.send(Err(message));
            return;
        }

        // Ensure PostThreadMessageW can target this thread immediately after start() returns.
        let mut message: MSG = unsafe { std::mem::zeroed() };
        unsafe { PeekMessageW(&mut message, null_mut(), 0, 0, PM_NOREMOVE) };

        if startup.send(Ok(thread_id)).is_err() {
            unsafe {
                RemoveClipboardFormatListener(window);
                DestroyWindow(window);
                UnregisterClassW(class_name.as_ptr(), module);
            }
            THREAD_STATE.with(|state| *state.borrow_mut() = None);
            return;
        }

        loop {
            let result = unsafe { GetMessageW(&mut message, null_mut(), 0, 0) };
            if result <= 0 {
                break;
            }
            unsafe {
                TranslateMessage(&message);
                DispatchMessageW(&message);
            }
        }

        unsafe {
            RemoveClipboardFormatListener(window);
            DestroyWindow(window);
            UnregisterClassW(class_name.as_ptr(), module);
        }
        THREAD_STATE.with(|state| *state.borrow_mut() = None);
    }

    unsafe extern "system" fn monitor_window_proc(
        window: HWND,
        message: u32,
        wparam: WPARAM,
        lparam: LPARAM,
    ) -> isize {
        if message == WM_CLIPBOARDUPDATE {
            let sequence = unsafe { GetClipboardSequenceNumber() };
            let events = THREAD_STATE.with(|state| {
                let mut state = state.borrow_mut();
                let state = state.as_mut()?;
                state.sequences.should_process(sequence).then(|| state.events.clone())
            });

            if let Some(events) = events
                && let Some(candidate) = unsafe { read_clipboard_text(window) }
                    .as_deref()
                    .and_then(parse_single_http_url)
            {
                let _ = events.send(candidate);
            }
            return 0;
        }

        unsafe { DefWindowProcW(window, message, wparam, lparam) }
    }

    unsafe fn read_clipboard_text(owner: HWND) -> Option<String> {
        if unsafe { OpenClipboard(owner) } == 0 {
            return None;
        }
        let _clipboard = ClipboardGuard;
        let handle = unsafe { GetClipboardData(CF_UNICODETEXT) };
        if handle.is_null() {
            return None;
        }

        let size = unsafe { GlobalSize(handle) };
        if !(2..=MAX_CLIPBOARD_BYTES).contains(&size) || size % 2 != 0 {
            return None;
        }
        let locked = unsafe { GlobalLock(handle) } as *const u16;
        if locked.is_null() {
            return None;
        }
        let _global_lock = GlobalLockGuard(handle);

        let units = unsafe { std::slice::from_raw_parts(locked, size / 2) };
        let terminator = units.iter().position(|unit| *unit == 0)?;
        String::from_utf16(&units[..terminator]).ok()
    }

    struct ClipboardGuard;

    impl Drop for ClipboardGuard {
        fn drop(&mut self) {
            unsafe { CloseClipboard() };
        }
    }

    struct GlobalLockGuard(*mut c_void);

    impl Drop for GlobalLockGuard {
        fn drop(&mut self) {
            unsafe { GlobalUnlock(self.0) };
        }
    }

    fn last_error(operation: &str) -> String {
        format!("{operation} (Win32 error {})", unsafe { GetLastError() })
    }

    fn parse_single_http_url(text: &str) -> Option<String> {
        // Newlines are rejected even at the ends: multiline clipboard contents are not a candidate.
        if text.contains('\r') || text.contains('\n') {
            return None;
        }
        let candidate = text.trim();
        if candidate.is_empty()
            || candidate.len() > MAX_URL_BYTES
            || candidate
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
        {
            return None;
        }

        let lower = candidate.to_ascii_lowercase();
        if !(lower.starts_with("http://") || lower.starts_with("https://"))
            || lower.matches("http://").count() + lower.matches("https://").count() != 1
        {
            return None;
        }

        let parsed = Url::parse(candidate).ok()?;
        let authority = candidate.split_once("://")?.1.split(['/', '?', '#']).next()?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || authority.contains('@')
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return None;
        }
        Some(candidate.to_owned())
    }

    #[derive(Default)]
    struct SequenceDeduper(Option<u32>);

    impl SequenceDeduper {
        fn should_process(&mut self, sequence: u32) -> bool {
            if self.0 == Some(sequence) {
                false
            } else {
                self.0 = Some(sequence);
                true
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{SequenceDeduper, parse_single_http_url};

        #[test]
        fn trims_a_single_http_or_https_url() {
            assert_eq!(
                parse_single_http_url("  https://example.com/watch?v=1  "),
                Some("https://example.com/watch?v=1".to_owned())
            );
            assert_eq!(
                parse_single_http_url("http://example.com"),
                Some("http://example.com".to_owned())
            );
        }

        #[test]
        fn rejects_non_urls_and_non_http_schemes() {
            assert_eq!(parse_single_http_url("just some text"), None);
            assert_eq!(parse_single_http_url("file:///C:/video.mp4"), None);
            assert_eq!(parse_single_http_url("https://"), None);
        }

        #[test]
        fn rejects_urls_with_userinfo() {
            assert_eq!(parse_single_http_url("https://user:secret@example.com/video"), None);
            assert_eq!(parse_single_http_url("https://@example.com/video"), None);
        }

        #[test]
        fn rejects_multiline_or_multiple_candidates() {
            assert_eq!(parse_single_http_url("https://example.com\n"), None);
            assert_eq!(parse_single_http_url("https://example.com\nhttps://other.example"), None);
            assert_eq!(
                parse_single_http_url("https://example.com and https://other.example"),
                None
            );
            assert_eq!(parse_single_http_url("https://example.com,https://other.example"), None);
            assert_eq!(parse_single_http_url("https://example.com extra"), None);
        }

        #[test]
        fn sequence_deduper_suppresses_only_repeated_consecutive_numbers() {
            let mut deduper = SequenceDeduper::default();
            assert!(deduper.should_process(4));
            assert!(!deduper.should_process(4));
            assert!(deduper.should_process(5));
            assert!(deduper.should_process(4));
        }
    }
}

#[cfg(windows)]
pub use windows_impl::ClipboardMonitor;

#[cfg(not(windows))]
pub struct ClipboardMonitor;

#[cfg(not(windows))]
impl ClipboardMonitor {
    /// Returns an explicit platform error where native clipboard monitoring is unavailable.
    pub fn start() -> anyhow::Result<(Self, std::sync::mpsc::Receiver<String>)> {
        Err(anyhow::anyhow!("clipboard URL monitoring is supported only on Windows"))
    }
}
