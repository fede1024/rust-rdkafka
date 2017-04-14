#[cfg(target_os = "linux")]
#[allow(non_camel_case_types)]
#[allow(non_upper_case_globals)]
mod linux;

#[cfg(target_os = "macos")]
#[allow(non_camel_case_types)]
#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
mod macos;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
panic!("Your platform is not yet supported.");

#[cfg(target_os = "linux")]
pub use self::linux::*;

#[cfg(target_os = "macos")]
pub use self::macos::*;
