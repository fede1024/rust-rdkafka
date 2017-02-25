#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "macos")]
mod macos;  // Temporarily use linux binding for mac as well

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
panic!("Your platform is not yet supported.");


#[cfg(target_os = "linux")]
pub use self::linux::*;

#[cfg(target_os = "macos")]
pub use self::macos::*;
