#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
#[allow(non_camel_case_types)]
#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
mod linux_64;

#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
pub use self::linux_64::*;

#[cfg(all(target_os = "macos", target_pointer_width = "64"))]
#[allow(non_camel_case_types)]
#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
mod macos_64;

#[cfg(all(target_os = "macos", target_pointer_width = "64"))]
pub use self::macos_64::*;

#[cfg(all(target_os = "freebsd", target_pointer_width = "64"))]
#[allow(non_camel_case_types)]
#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
mod freebsd_64;

#[cfg(all(target_os = "freebsd", target_pointer_width = "64"))]
pub use self::freebsd_64::*;

#[cfg(not(any(all(target_os = "linux", target_pointer_width = "64"),
              all(target_os = "macos", target_pointer_width = "64"),
              all(target_os = "freebsd", target_pointer_width = "64"))))]
compile_error!("Your platform is not yet supported. Build your rdkafka-sys bindings manually");
