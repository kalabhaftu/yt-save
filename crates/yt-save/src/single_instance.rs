use std::ptr;

use anyhow::{Context, Result};
use windows_sys::Win32::{
    Foundation::{CloseHandle, ERROR_ALREADY_EXISTS, GetLastError, HANDLE, SetLastError},
    System::Threading::CreateMutexW,
    UI::WindowsAndMessaging::{MB_ICONINFORMATION, MB_OK, MessageBoxW},
};

pub struct SingleInstanceGuard(HANDLE);

impl SingleInstanceGuard {
    pub fn acquire() -> Result<Option<Self>> {
        let name = format!(
            r"Local\{}-{}-instance",
            env!("YT_SAVE_PUBLISHER"),
            env!("YT_SAVE_PRODUCT_NAME")
        );
        Self::acquire_named(&name)
    }

    fn acquire_named(name: &str) -> Result<Option<Self>> {
        let name: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        unsafe { SetLastError(0) };
        let handle = unsafe { CreateMutexW(ptr::null(), 0, name.as_ptr()) };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error())
                .context("create the Yt Save instance guard");
        }
        if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
            unsafe {
                CloseHandle(handle);
            }
            return Ok(None);
        }
        Ok(Some(Self(handle)))
    }
}

impl Drop for SingleInstanceGuard {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.0);
        }
    }
}

pub fn show_already_running_message() {
    let message: Vec<u16> = "Yt Save is already running.".encode_utf16().chain([0]).collect();
    let title: Vec<u16> = env!("YT_SAVE_PRODUCT_NAME").encode_utf16().chain([0]).collect();
    unsafe {
        MessageBoxW(ptr::null_mut(), message.as_ptr(), title.as_ptr(), MB_OK | MB_ICONINFORMATION);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_instance_guard_rejects_a_second_process_and_releases_on_drop() {
        let name = format!(r"Local\YtSave-test-{}", uuid::Uuid::new_v4());
        let first = SingleInstanceGuard::acquire_named(&name).unwrap().unwrap();
        assert!(SingleInstanceGuard::acquire_named(&name).unwrap().is_none());
        drop(first);
        assert!(SingleInstanceGuard::acquire_named(&name).unwrap().is_some());
    }
}
