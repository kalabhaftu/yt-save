use std::{cell::RefCell, process::Command, rc::Rc, sync::atomic::Ordering};

use anyhow::{Context, Result};
use slint::{CloseRequestResponse, ComponentHandle};
use tracing::error;

use crate::cookie_auth::CookieRecord;

use super::{CookieAuthContext, UiEvent};
#[cfg(windows)]
pub(super) fn configure_cookie_auth(context: CookieAuthContext<'_>) {
    let CookieAuthContext {
        main_window,
        login_window,
        cookie_store,
        handle,
        events,
        generation,
        login_session,
        shutdown_started,
    } = context;
    main_window.on_cookie_runtime_download_requested(|| {
        if let Err(error) = open_webview2_runtime_page() {
            error!(%error, "open Microsoft WebView2 Runtime download page");
        }
    });

    let webview_slot = Rc::new(RefCell::new(None::<wry::WebView>));
    let login_url_slot = Rc::new(RefCell::new(None::<String>));

    let state_store = cookie_store.clone();
    let state_handle = handle.clone();
    let state_events = events.clone();
    let state_generation = generation.clone();
    main_window.on_cookie_vault_state_requested(move |requested_url| {
        let url = requested_url.to_string();
        let request_generation = state_generation.fetch_add(1, Ordering::AcqRel) + 1;
        if validate_cookie_sign_in_url(&url).is_err() {
            let _ = state_events.send(UiEvent::CookieAuthState {
                generation: request_generation,
                url,
                has_saved: Some(false),
                message: None,
                close_login: false,
                login_session: None,
                refresh_current_url: false,
            });
            return;
        }

        let store = state_store.clone();
        let events = state_events.clone();
        state_handle.spawn_blocking(move || {
            let (has_saved, message) = match store.has_saved_for_url(&url) {
                Ok(saved) => (saved, None),
                Err(_) => (
                    true,
                    Some(
                        "Saved sign-in data could not be read. Clear it and sign in again."
                            .to_owned(),
                    ),
                ),
            };
            let _ = events.send(UiEvent::CookieAuthState {
                generation: request_generation,
                url,
                has_saved: Some(has_saved),
                message,
                close_login: false,
                login_session: None,
                refresh_current_url: false,
            });
        });
    });

    let clear_store = cookie_store.clone();
    let clear_handle = handle.clone();
    let clear_events = events.clone();
    let clear_generation = generation.clone();
    main_window.on_cookie_clear_requested(move |requested_url| {
        let url = requested_url.to_string();
        let request_generation = clear_generation.fetch_add(1, Ordering::AcqRel) + 1;
        if validate_cookie_sign_in_url(&url).is_err() {
            let _ = clear_events.send(UiEvent::CookieAuthState {
                generation: request_generation,
                url,
                has_saved: Some(false),
                message: Some("Enter a secure HTTPS media URL first.".to_owned()),
                close_login: false,
                login_session: None,
                refresh_current_url: false,
            });
            return;
        }

        let store = clear_store.clone();
        let events = clear_events.clone();
        let reported_url = url.clone();
        let operation = clear_handle.spawn_blocking(move || store.clear_for_url(&url));
        clear_handle.spawn(async move {
            let outcome = operation.await;
            let (has_saved, message) = match outcome {
                Ok(Ok(_)) => (Some(false), Some("Saved sign-in removed.".to_owned())),
                _ => {
                    (Some(true), Some("Saved sign-in could not be removed. Try again.".to_owned()))
                }
            };
            let _ = events.send(UiEvent::CookieAuthState {
                generation: request_generation,
                url: reported_url,
                has_saved,
                message,
                close_login: false,
                login_session: None,
                refresh_current_url: false,
            });
        });
    });

    let sign_in_window = login_window.as_weak();
    let sign_in_main = main_window.as_weak();
    let sign_in_webview = webview_slot.clone();
    let sign_in_url = login_url_slot.clone();
    let sign_in_session = login_session.clone();
    let sign_in_shutdown = shutdown_started.clone();
    main_window.on_cookie_sign_in_requested(move |submitted_url| {
        if sign_in_shutdown.load(Ordering::Acquire) {
            if let Some(window) = sign_in_main.upgrade() {
                window.set_status_message("The application is shutting down.".into());
            }
            return;
        }
        let target_url = submitted_url.to_string();
        let (normalized_url, host) = match validate_cookie_sign_in_url(&target_url) {
            Ok(target) => target,
            Err(_) => {
                if let Some(window) = sign_in_main.upgrade() {
                    window.set_status_message(
                        "Enter a secure HTTPS media URL before signing in.".into(),
                    );
                }
                return;
            }
        };

        if sign_in_window.upgrade().is_some_and(|login| login.get_saving()) {
            if let Some(window) = sign_in_main.upgrade() {
                window.set_status_message("Sign-in cookies are still being protected. Please wait.".into());
            }
            return;
        }
        sign_in_session.fetch_add(1, Ordering::AcqRel);

        discard_cookie_webview(&sign_in_webview);
        *sign_in_url.borrow_mut() = Some(normalized_url.clone());
        let Some(login) = sign_in_window.upgrade() else {
            return;
        };
        login.set_site_label(host.into());
        login.set_login_status(
            "Sign in on the page below, then choose Save cookies. Nothing is saved if you cancel."
                .into(),
        );
        login.set_saving(false);
        login.window().set_size(slint::LogicalSize::new(920.0, 720.0));
        if let Err(error) = login.show() {
            error!(%error, "show embedded sign-in window");
            if let Some(window) = sign_in_main.upgrade() {
                window.set_status_message("The sign-in window could not be opened.".into());
            }
            return;
        }

        use slint::winit_030::WinitWindowAccessor;
        let scale = login.window().scale_factor();
        let bounds = wry::Rect {
            position: wry::dpi::Position::Physical(wry::dpi::PhysicalPosition::new(
                0,
                (108.0 * scale).round() as i32,
            )),
            size: wry::dpi::Size::Physical(wry::dpi::PhysicalSize::new(
                (920.0 * scale).round() as u32,
                (500.0 * scale).round() as u32,
            )),
        };
        let built = login.window().with_winit_window(|native_window| {
            wry::WebViewBuilder::new()
                .with_url(normalized_url)
                .with_bounds(bounds)
                .with_incognito(true)
                .with_devtools(false)
                .with_autoplay(false)
                .with_navigation_handler(is_secure_web_navigation)
                .with_new_window_req_handler(|_, _| wry::NewWindowResponse::Deny)
                .with_permission_handler(|_| wry::PermissionResponse::Deny)
                .with_download_started_handler(|_, _| false)
                .build_as_child(native_window)
        });
        match built {
            Some(Ok(webview)) => {
                *sign_in_webview.borrow_mut() = Some(webview);
                if let Some(webview) = sign_in_webview.borrow().as_ref()
                    && let Err(error) = webview.focus()
                {
                    error!(%error, "focus embedded sign-in view");
                }
            }
            Some(Err(_)) | None => {
                *sign_in_url.borrow_mut() = None;
                let _ = login.hide();
                if let Some(window) = sign_in_main.upgrade() {
                    window.set_status_message(
                        "The embedded sign-in view could not start. Check that Microsoft Edge WebView2 Runtime is installed.".into(),
                    );
                }
            }
        }
    });

    let save_login = login_window.as_weak();
    let save_webview = webview_slot.clone();
    let save_url = login_url_slot.clone();
    let save_store = cookie_store.clone();
    let save_handle = handle.clone();
    let save_events = events.clone();
    let save_generation = generation.clone();
    let save_session = login_session.clone();
    login_window.on_save_requested(move || {
        let Some(target_url) = save_url.borrow().clone() else {
            if let Some(login) = save_login.upgrade() {
                login.set_login_status("Open a site before saving sign-in cookies.".into());
            }
            return;
        };
        let cookie_records = {
            let borrowed = save_webview.borrow();
            let Some(webview) = borrowed.as_ref() else {
                if let Some(login) = save_login.upgrade() {
                    login.set_login_status("The sign-in view is no longer active. Close it and try again.".into());
                }
                return;
            };
            let cookies = match webview.cookies_for_url(&target_url) {
                Ok(cookies) => cookies,
                Err(_) => {
                    if let Some(login) = save_login.upgrade() {
                        login.set_login_status("Yt Save could not read cookies for this URL. Finish sign-in and try again.".into());
                    }
                    return;
                }
            };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64)
                .unwrap_or_default();
            cookies
                .into_iter()
                .filter_map(|cookie| {
                    let domain = cookie.domain_raw()?.to_owned();
                    let expires = cookie.expires_datetime().map(|date| date.unix_timestamp()).unwrap_or(0);
                    if expires != 0 && expires <= now {
                        return None;
                    }
                    Some(CookieRecord {
                        domain,
                        path: cookie.path().unwrap_or("/").to_owned(),
                        name: cookie.name().to_owned(),
                        value: cookie.value().to_owned(),
                        secure: cookie.secure().unwrap_or(false),
                        http_only: cookie.http_only().unwrap_or(false),
                        expires,
                    })
                })
                .collect::<Vec<_>>()
        };
        if cookie_records.is_empty() {
            if let Some(login) = save_login.upgrade() {
                login.set_login_status(
                    "No unexpired cookies were found for this URL. Complete sign-in, then try again."
                        .into(),
                );
            }
            return;
        }

        *save_url.borrow_mut() = None;
        discard_cookie_webview(&save_webview);
        if let Some(login) = save_login.upgrade() {
            login.set_saving(true);
            login.set_login_status("Protecting your sign-in cookies with Windows DPAPI…".into());
        }
        let request_generation = save_generation.load(Ordering::Acquire);
        let request_login_session = save_session.load(Ordering::Acquire);
        let store = save_store.clone();
        let target_for_save = target_url.clone();
        let operation = save_handle.spawn_blocking(move || {
            store.save_for_url(&target_for_save, &cookie_records)
        });
        let events = save_events.clone();
        save_handle.spawn(async move {
            let succeeded = matches!(operation.await, Ok(Ok(())));
            let _ = events.send(UiEvent::CookieAuthState {
                generation: request_generation,
                url: target_url,
                has_saved: Some(succeeded),
                message: Some(if succeeded {
                    "Saved sign-in cookies for this site. They will be used for matching HTTPS URLs.".to_owned()
                } else {
                    "Sign-in cookies could not be saved. Sign in again and retry.".to_owned()
                }),
                close_login: true,
                login_session: Some(request_login_session),
                refresh_current_url: true,
            });
        });
    });

    let cancel_webview = webview_slot.clone();
    let cancel_url = login_url_slot.clone();
    let cancel_login = login_window.as_weak();
    let cancel_session = login_session.clone();
    login_window.on_cancel_requested(move || {
        cancel_session.fetch_add(1, Ordering::AcqRel);
        discard_cookie_webview(&cancel_webview);
        *cancel_url.borrow_mut() = None;
        if let Some(login) = cancel_login.upgrade()
            && let Err(error) = login.hide()
        {
            error!(%error, "cancel embedded sign-in");
        }
    });

    let back_webview = webview_slot.clone();
    login_window.on_back_requested(move || {
        if let Some(webview) = back_webview.borrow().as_ref()
            && webview.can_go_back().unwrap_or(false)
            && let Err(error) = webview.go_back()
        {
            error!(%error, "navigate embedded sign-in view backward");
        }
    });

    let forward_webview = webview_slot.clone();
    login_window.on_forward_requested(move || {
        if let Some(webview) = forward_webview.borrow().as_ref()
            && webview.can_go_forward().unwrap_or(false)
            && let Err(error) = webview.go_forward()
        {
            error!(%error, "navigate embedded sign-in view forward");
        }
    });

    let reload_webview = webview_slot.clone();
    login_window.on_reload_requested(move || {
        if let Some(webview) = reload_webview.borrow().as_ref()
            && let Err(error) = webview.reload()
        {
            error!(%error, "reload embedded sign-in view");
        }
    });

    let close_webview = webview_slot.clone();
    let close_url = login_url_slot.clone();
    let close_login = login_window.as_weak();
    let close_session = login_session.clone();
    login_window.window().on_close_requested(move || {
        if close_login.upgrade().is_some_and(|login| login.get_saving()) {
            return CloseRequestResponse::KeepWindowShown;
        }
        close_session.fetch_add(1, Ordering::AcqRel);
        discard_cookie_webview(&close_webview);
        *close_url.borrow_mut() = None;
        CloseRequestResponse::HideWindow
    });
}

#[cfg(windows)]
pub(super) fn cookie_webview_runtime_status() -> (bool, String) {
    match wry::webview_version() {
        Ok(version) if supports_private_webview_runtime(&version) => (true, String::new()),
        Ok(_) => (
            false,
            "Update Microsoft Edge WebView2 Runtime to enable secure site sign-in.".to_owned(),
        ),
        Err(_) => (
            false,
            "Install Microsoft Edge WebView2 Runtime to enable secure site sign-in.".to_owned(),
        ),
    }
}

#[cfg(windows)]
fn supports_private_webview_runtime(version: &str) -> bool {
    let Some(actual) = version
        .split('.')
        .take(4)
        .map(str::parse::<u64>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok()
    else {
        return false;
    };
    actual.len() == 4 && actual.as_slice() >= &[101, 0, 1210, 39]
}

#[cfg(windows)]
fn open_webview2_runtime_page() -> Result<()> {
    let mut command = Command::new("cmd.exe");
    command.args([
        "/C",
        "start",
        "",
        "https://developer.microsoft.com/en-us/microsoft-edge/webview2/",
    ]);
    command.spawn().context("launch the official WebView2 Runtime page")?;
    Ok(())
}

#[cfg(windows)]
fn validate_cookie_sign_in_url(input: &str) -> Result<(String, String)> {
    let url = url::Url::parse(input).context("Enter a complete secure HTTPS URL")?;
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("Sign-in cookies require a secure HTTPS URL without embedded credentials");
    }
    let host = url
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| anyhow::anyhow!("The URL must include a website host"))?
        .to_owned();
    Ok((url.to_string(), host))
}

#[cfg(windows)]
fn is_secure_web_navigation(input: String) -> bool {
    url::Url::parse(&input).is_ok_and(|url| {
        url.scheme() == "https" && url.username().is_empty() && url.password().is_none()
    })
}

#[cfg(windows)]
fn discard_cookie_webview(slot: &Rc<RefCell<Option<wry::WebView>>>) {
    if let Some(webview) = slot.borrow_mut().take() {
        let _ = webview.clear_all_browsing_data();
    }
}
#[cfg(test)]
mod tests {
    use super::supports_private_webview_runtime;

    #[test]
    fn private_webview_gate_requires_a_runtime_with_incognito_support() {
        assert!(supports_private_webview_runtime("101.0.1210.39"));
        assert!(supports_private_webview_runtime("146.0.3856.59"));
        assert!(!supports_private_webview_runtime("101.0.1210.38"));
        assert!(!supports_private_webview_runtime("not-a-version"));
    }
}
