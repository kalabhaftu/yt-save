use std::cell::Cell;

thread_local! {
    static NEXT_WINDOW_IS_NON_ACTIVATING: Cell<bool> = const { Cell::new(false) };
}

/// Creates one Slint window with the backend's initial activation disabled.
///
/// The Winit attributes hook consumes this marker while the component creates its adapter.
pub fn create_non_activating_window<T>(create: impl FnOnce() -> T) -> T {
    NEXT_WINDOW_IS_NON_ACTIVATING.with(|marker| marker.set(true));
    struct ResetMarker;
    impl Drop for ResetMarker {
        fn drop(&mut self) {
            NEXT_WINDOW_IS_NON_ACTIVATING.with(|marker| marker.set(false));
        }
    }

    let _reset = ResetMarker;
    create()
}

pub fn consume_non_activating_window_marker() -> bool {
    NEXT_WINDOW_IS_NON_ACTIVATING.with(|marker| marker.replace(false))
}
