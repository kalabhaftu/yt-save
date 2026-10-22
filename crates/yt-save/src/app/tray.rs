use super::*;

pub(super) struct TrayContext {
    pub(super) store: Store,
    pub(super) handle: tokio::runtime::Handle,
    pub(super) events: mpsc::UnboundedSender<UiEvent>,
    pub(super) active_jobs: ActiveJobs,
    pub(super) subscription_check_active: Arc<AtomicBool>,
    pub(super) dispatch_gate: Arc<AsyncMutex<()>>,
    pub(super) queue_wake: Arc<Notify>,
    pub(super) preferences: Arc<Mutex<TrayPreferences>>,
    pub(super) shutdown_started: Arc<AtomicBool>,
    pub(super) shutdown_committed: Arc<AtomicBool>,
}

pub(super) struct ShutdownState {
    pub(super) started: Arc<AtomicBool>,
    pub(super) committed: Arc<AtomicBool>,
}
pub(super) fn configure_tray(window: &MainWindow, tray: &AppTray, context: TrayContext) {
    let TrayContext {
        store,
        handle,
        events,
        active_jobs,
        subscription_check_active: playlist_subscription_check_active,
        dispatch_gate: update_dispatch_gate,
        queue_wake,
        preferences,
        shutdown_started,
        shutdown_committed,
    } = context;
    let open_window = window.as_weak();
    tray.on_open_requested(move || {
        if let Some(window) = open_window.upgrade() {
            window.window().set_minimized(false);
            if let Err(error) = window.show() {
                error!(%error, "show window from system tray");
            }
        }
    });

    let hide_window = window.as_weak();
    tray.on_hide_requested(move || {
        if let Some(window) = hide_window.upgrade()
            && let Err(error) = window.hide()
        {
            error!(%error, "hide window to system tray");
        }
    });

    let open_folder_window = window.as_weak();
    tray.on_open_folder_requested(move || {
        if let Some(window) = open_folder_window.upgrade() {
            window.invoke_open_download_folder();
        }
    });

    let pause_store = store.clone();
    let pause_active_jobs = active_jobs.clone();
    let pause_events = events.clone();
    let pause_handle = handle.clone();
    let pause_shutdown_guard = shutdown_started.clone();
    tray.on_pause_all_requested(move || {
        if pause_shutdown_guard.load(Ordering::Acquire) {
            return;
        }
        let store = pause_store.clone();
        let active_jobs = pause_active_jobs.clone();
        let events = pause_events.clone();
        pause_handle.spawn(async move {
            match pause_all_downloads(&store, &active_jobs, true).await {
                Ok((pending_count, active_count)) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ = events.send(UiEvent::Status(format!(
                        "Paused {pending_count} pending item(s); requested a safe pause for {active_count} active item(s)."
                    )));
                }
                Err(error) => {
                    error!(%error, "pause all downloads from system tray");
                    let _ = events.send(UiEvent::Status(format!("Could not pause all downloads: {error}")));
                }
            }
        });
    });

    let resume_store = store.clone();
    let resume_events = events.clone();
    let resume_wake = queue_wake.clone();
    let resume_handle = handle.clone();
    let resume_shutdown_guard = shutdown_started.clone();
    tray.on_resume_all_requested(move || {
        if resume_shutdown_guard.load(Ordering::Acquire) {
            return;
        }
        let store = resume_store.clone();
        let events = resume_events.clone();
        let queue_wake = resume_wake.clone();
        resume_handle.spawn(async move {
            match resume_all_downloads(store).await {
                Ok(count) => {
                    let _ = events.send(UiEvent::Refresh);
                    let _ =
                        events.send(UiEvent::Status(format!("Resumed {count} paused item(s).")));
                    if count > 0 {
                        queue_wake.notify_one();
                    }
                }
                Err(error) => {
                    error!(%error, "resume all downloads from system tray");
                    let _ = events.send(UiEvent::Status(format!(
                        "Could not resume paused downloads: {error}"
                    )));
                }
            }
        });
    });

    let quit_store = store.clone();
    let quit_handle = handle.clone();
    let quit_events = events.clone();
    let quit_active_jobs = active_jobs.clone();
    let quit_subscription_checks = playlist_subscription_check_active.clone();
    let quit_queue_wake = queue_wake.clone();
    let quit_window = window.as_weak();
    let quit_dispatch_gate = update_dispatch_gate.clone();
    let quit_guard = shutdown_started.clone();
    let quit_committed = shutdown_committed.clone();
    tray.on_quit_requested(move || {
        begin_orderly_quit(
            quit_handle.clone(),
            quit_store.clone(),
            quit_active_jobs.clone(),
            quit_subscription_checks.clone(),
            quit_queue_wake.clone(),
            quit_events.clone(),
            quit_window.clone(),
            quit_dispatch_gate.clone(),
            ShutdownState { started: quit_guard.clone(), committed: quit_committed.clone() },
        );
    });

    let preferences_state = preferences.clone();
    let preference_write_lock = Arc::new(AsyncMutex::new(()));
    let close_preference_write_lock = preference_write_lock.clone();
    let preference_store = store.clone();
    let preference_handle = handle.clone();
    let preference_events = events.clone();
    let preference_window = window.as_weak();
    window.on_close_to_tray_requested(move |enabled| {
        if let Some(window) = preference_window.upgrade() {
            window.set_close_to_tray_enabled(enabled);
        }
        save_tray_preference_update(
            &preferences_state,
            Some(enabled),
            None,
            close_preference_write_lock.clone(),
            preference_store.clone(),
            preference_handle.clone(),
            preference_events.clone(),
        );
    });

    let preferences_state = preferences.clone();
    let minimize_preference_write_lock = preference_write_lock.clone();
    let preference_store = store.clone();
    let preference_handle = handle.clone();
    let preference_events = events.clone();
    let preference_window = window.as_weak();
    window.on_minimize_to_tray_requested(move |enabled| {
        if let Some(window) = preference_window.upgrade() {
            window.set_minimize_to_tray_enabled(enabled);
        }
        save_tray_preference_update(
            &preferences_state,
            None,
            Some(enabled),
            minimize_preference_write_lock.clone(),
            preference_store.clone(),
            preference_handle.clone(),
            preference_events.clone(),
        );
    });

    let close_preferences = preferences.clone();
    let close_store = store;
    let close_handle = handle;
    let close_events = events;
    let close_active_jobs = active_jobs;
    let close_subscription_checks = playlist_subscription_check_active;
    let close_queue_wake = queue_wake;
    let close_window = window.as_weak();
    let close_dispatch_gate = update_dispatch_gate;
    let close_guard = shutdown_started;
    let close_committed = shutdown_committed;
    window.window().on_close_requested(move || {
        let close_to_tray =
            close_preferences.lock().map(|preferences| preferences.close_to_tray).unwrap_or(false);
        if close_to_tray {
            CloseRequestResponse::HideWindow
        } else {
            begin_orderly_quit(
                close_handle.clone(),
                close_store.clone(),
                close_active_jobs.clone(),
                close_subscription_checks.clone(),
                close_queue_wake.clone(),
                close_events.clone(),
                close_window.clone(),
                close_dispatch_gate.clone(),
                ShutdownState { started: close_guard.clone(), committed: close_committed.clone() },
            );
            CloseRequestResponse::KeepWindowShown
        }
    });
}

fn save_tray_preference_update(
    state: &Arc<Mutex<TrayPreferences>>,
    close_to_tray: Option<bool>,
    minimize_to_tray: Option<bool>,
    write_lock: Arc<AsyncMutex<()>>,
    store: Store,
    handle: tokio::runtime::Handle,
    events: mpsc::UnboundedSender<UiEvent>,
) {
    match state.lock() {
        Ok(mut preferences) => {
            if let Some(enabled) = close_to_tray {
                preferences.close_to_tray = enabled;
            }
            if let Some(enabled) = minimize_to_tray {
                preferences.minimize_to_tray = enabled;
            }
        }
        Err(_) => {
            let _ = events.send(UiEvent::Status(
                "Could not update tray behavior because settings state is unavailable.".to_owned(),
            ));
            return;
        }
    };

    let state = state.clone();
    handle.spawn(async move {
        let _write_guard = write_lock.lock().await;
        let preferences = match state.lock() {
            Ok(preferences) => *preferences,
            Err(_) => {
                let _ = events.send(UiEvent::Status(
                    "Tray behavior changed for this session but could not be saved because settings state is unavailable.".to_owned(),
                ));
                return;
            }
        };
        if let Err(error) = store.set_tray_preferences(preferences, now()).await {
            error!(%error, "save tray behavior preferences");
            let _ = events.send(UiEvent::Status(format!(
                "Tray behavior changed for this session but could not be saved: {error}"
            )));
        }
    });
}

pub(super) fn cancel_staged_update_helper(
    helper_state: &Arc<Mutex<Option<crate::app_update::UpdateHelper>>>,
) -> Result<()> {
    let mut helper_state =
        helper_state.lock().map_err(|_| anyhow::anyhow!("updater helper state is unavailable"))?;
    if let Some(helper) = helper_state.as_mut() {
        helper.cancel_and_wait()?;
    }
    helper_state.take();
    Ok(())
}

fn begin_orderly_quit(
    handle: tokio::runtime::Handle,
    store: Store,
    active_jobs: ActiveJobs,
    subscription_check_active: Arc<AtomicBool>,
    queue_wake: Arc<Notify>,
    events: mpsc::UnboundedSender<UiEvent>,
    window: slint::Weak<MainWindow>,
    dispatch_gate: Arc<AsyncMutex<()>>,
    shutdown: ShutdownState,
) {
    let shutdown_started = shutdown.started;
    let shutdown_committed = shutdown.committed;
    if shutdown_started.swap(true, Ordering::AcqRel) {
        return;
    }
    queue_wake.notify_one();
    if let Some(window) = window.upgrade() {
        window.set_status_message(
            "Pausing queue work and waiting for playlist checks before quitting. Partial files are being preserved."
                .into(),
        );
    }

    handle.spawn(async move {
        let shutdown_result = async {
            let dispatch_guard = dispatch_gate.lock().await;
            pause_all_downloads(&store, &active_jobs, false).await?;
            drop(dispatch_guard);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                let active_count = active_jobs
                    .lock()
                    .map_err(|_| anyhow::anyhow!("active download state is unavailable"))?
                    .len();
                if active_count == 0
                    && !subscription_check_active.load(Ordering::Acquire)
                {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    anyhow::bail!(
                        "active downloads or playlist checks did not stop within 30 seconds; the app is still open"
                    );
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        match shutdown_result {
            Ok(()) => {
                let _ = events.send(UiEvent::Status(
                    "Queue work is safely paused. The application is shutting down.".to_owned(),
                ));
                let quit_queue_wake = queue_wake.clone();
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(window) = window.upgrade()
                        && let Err(error) = window.hide()
                    {
                        error!(%error, "hide window during orderly shutdown");
                    }
                    if let Err(error) = slint::quit_event_loop() {
                        shutdown_started.store(false, Ordering::Release);
                        shutdown_committed.store(false, Ordering::Release);
                        quit_queue_wake.notify_one();
                        error!(%error, "quit Slint event loop during orderly shutdown");
                        if let Some(window) = window.upgrade() {
                            window.set_status_message(
                                format!("Could not finish shutting down: {error}").into(),
                            );
                            let _ = window.show();
                        }
                        return;
                    }
                    shutdown_committed.store(true, Ordering::Release);
                    quit_queue_wake.notify_one();
                });
            }
            Err(error) => {
                shutdown_started.store(false, Ordering::Release);
                shutdown_committed.store(false, Ordering::Release);
                queue_wake.notify_one();
                error!(%error, "orderly application shutdown");
                let message = format!(
                    "Could not safely quit: {error}. The window and tray remain available."
                );
                let _ = events.send(UiEvent::Status(message.clone()));
                if let Err(invoke_error) = slint::invoke_from_event_loop(move || {
                    if let Some(window) = window.upgrade() {
                        window.window().set_minimized(false);
                        window.set_status_message(message.into());
                        if let Err(show_error) = window.show() {
                            error!(%show_error, "show window after orderly shutdown failure");
                        }
                    }
                }) {
                    error!(%invoke_error, "restore window after orderly shutdown failure");
                }
            }
        }
    });
}
