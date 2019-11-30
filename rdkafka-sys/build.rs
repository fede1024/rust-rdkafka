use std::borrow::Borrow;
use std::env;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{self, Command};

fn run_command_or_fail<P, S>(dir: &str, cmd: P, args: &[S])
where
    P: AsRef<Path>,
    S: Borrow<str> + AsRef<OsStr>,
{
    let cmd = cmd.as_ref();
    let cmd = if cmd.components().count() > 1 && cmd.is_relative() {
        // If `cmd` is a relative path (and not a bare command that should be
        // looked up in PATH), absolutize it relative to `dir`, as otherwise the
        // behavior of std::process::Command is undefined.
        // https://github.com/rust-lang/rust/issues/37868
        PathBuf::from(dir)
            .join(cmd)
            .canonicalize()
            .expect("canonicalization failed")
    } else {
        PathBuf::from(cmd)
    };
    eprintln!(
        "Running command: \"{} {}\" in dir: {}",
        cmd.display(),
        args.join(" "),
        dir
    );
    let ret = Command::new(cmd).current_dir(dir).args(args).status();
    match ret.map(|status| (status.success(), status.code())) {
        Ok((true, _)) => return,
        Ok((false, Some(c))) => panic!("Command failed with error code {}", c),
        Ok((false, None)) => panic!("Command got killed"),
        Err(e) => panic!("Command failed with error: {}", e),
    }
}

fn main() {
    let librdkafka_version = env!("CARGO_PKG_VERSION")
        .split('-')
        .next()
        .expect("Crate version is not valid");

    if env::var("DEP_OPENSSL_VENDORED").is_ok() {
        let openssl_root = env::var("DEP_OPENSSL_ROOT").expect("DEP_OPENSSL_ROOT is not set");
        env::set_var("CFLAGS", format!("-I{}/include", openssl_root));
        env::set_var("LDFLAGS", format!("-L{}/lib", openssl_root));
    }

    if env::var("CARGO_FEATURE_DYNAMIC_LINKING").is_ok() {
        eprintln!("librdkafka will be linked dynamically");
        let pkg_probe = pkg_config::Config::new()
            .cargo_metadata(true)
            .atleast_version(librdkafka_version)
            .probe("rdkafka");

        match pkg_probe {
            Ok(library) => {
                eprintln!("librdkafka found on the system:");
                eprintln!("  Name: {:?}", library.libs);
                eprintln!("  Path: {:?}", library.link_paths);
                eprintln!("  Version: {}", library.version);
            }
            Err(_) => {
                eprintln!(
                    "librdkafka {} cannot be found on the system",
                    librdkafka_version
                );
                eprintln!("Dynamic linking failed. Exiting.");
                process::exit(1);
            }
        }
    } else {
        if !Path::new("librdkafka/LICENSE").exists() {
            eprintln!("Setting up submodules");
            run_command_or_fail("../", "git", &["submodule", "update", "--init"]);
        }
        eprintln!("Building and linking librdkafka statically");
        build_librdkafka();
    }
}

#[cfg(not(feature = "cmake_build"))]
fn build_librdkafka() {
    let mut configure_flags: Vec<String> = Vec::new();
    let mut cflags = Vec::new();
    let mut ldflags = Vec::new();

    if env::var("CARGO_FEATURE_SSL").is_ok() {
        configure_flags.push("--enable-ssl".into());
    } else {
        configure_flags.push("--disable-ssl".into());
    }
    if env::var("DEP_OPENSSL_VENDORED").is_ok() {
        let openssl_root = env::var("DEP_OPENSSL_ROOT").expect("DEP_OPENSSL_ROOT is not set");
        cflags.push(format!("-I{}/include", openssl_root));
        ldflags.push(format!("-L{}/lib", openssl_root));
    }

    if env::var("CARGO_FEATURE_SASL").is_ok() {
        configure_flags.push("--enable-sasl".into());
        println!("cargo:rustc-link-lib=sasl2");
    } else {
        configure_flags.push("--disable-sasl".into());
    }

    if env::var("CARGO_FEATURE_LIBZ").is_ok() {
        // There is no --enable-zlib option, but it is enabled by default.
    } else {
        configure_flags.push("--disable-zlib".into());
    }

    if env::var("CARGO_FEATURE_ZSTD").is_ok() {
        configure_flags.push("--enable-zstd".into());
    } else {
        configure_flags.push("--disable-zstd".into());
    }

    if env::var("CARGO_FEATURE_EXTERNAL_LZ4").is_ok() {
        configure_flags.push("--enable-lz4".into());
    } else {
        configure_flags.push("--disable-lz4".into());
    }

    if !cflags.is_empty() {
        configure_flags.push(format!("--CFLAGS={}", cflags.join(" ")));
    }
    if !ldflags.is_empty() {
        configure_flags.push(format!("--LDFLAGS={}", ldflags.join(" ")));
    }

    println!("Configuring librdkafka");
    run_command_or_fail("librdkafka", "./configure", configure_flags.as_slice());

    println!("Compiling librdkafka");
    env::set_var("MAKEFLAGS", env::var_os("CARGO_MAKEFLAGS").expect("CARGO_MAKEFLAGS env var missing"));
    run_command_or_fail(
        "librdkafka",
        if cfg!(target_os = "freebsd") { "gmake" } else { "make" },
        &["libs"],
    );

    println!(
        "cargo:rustc-link-search=native={}/librdkafka/src",
        env::current_dir()
            .expect("Can't find current dir")
            .display()
    );
    println!("cargo:rustc-link-lib=static=rdkafka");
}

#[cfg(feature = "cmake_build")]
fn build_librdkafka() {
    let mut config = cmake::Config::new("librdkafka");

    config
        .define("RDKAFKA_BUILD_STATIC", "1")
        .build_target("rdkafka");

    if env::var("CARGO_FEATURE_LIBZ").is_ok() {
        config.define("WITH_ZLIB", "1");
        config.register_dep("libz");
    } else {
        config.define("WITH_ZLIB", "0");
    }

    if env::var("CARGO_FEATURE_SSL").is_ok() {
        config.define("WITH_SSL", "1");
    } else {
        config.define("WITH_SSL", "0");
    }
    if env::var("DEP_OPENSSL_VENDORED").is_ok() {
        config.register_dep("openssl");
    }

    if env::var("CARGO_FEATURE_SASL").is_ok() {
        config.define("WITH_SASL", "1");
        println!("cargo:rustc-link-lib=sasl2");
    } else {
        config.define("WITH_SASL", "0");
    }

    if env::var("CARGO_FEATURE_ZSTD").is_ok() {
        config.define("WITH_ZSTD", "1");
        config.register_dep("zstd");
    } else {
        config.define("WITH_ZSTD", "0");
    }

    if env::var("CARGO_FEATURE_EXTERNAL_LZ4").is_ok() {
        config.define("ENABLE_LZ4_EXT", "1");
    } else {
        config.define("ENABLE_LZ4_EXT", "0");
    }

    if let Ok(system_name) = env::var("CMAKE_SYSTEM_NAME") {
        config.define("CMAKE_SYSTEM_NAME", system_name);
    }

    // The CMake build will incorrectly use config.h from the non-CMake build,
    // if it exists, so remove it if it does.
    match std::fs::remove_file("librdkafka/config.h") {
        Ok(()) => (),
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => (),
            _ => panic!("Unable to remove config.h from non-CMake build: {}", err),
        }
    }

    println!("Configuring and compiling librdkafka");
    let dst = config.build();

    if cfg!(target_env = "msvc") {
        let profile = match &env::var("PROFILE").expect("Cannot determine build profile")[..] {
            "release" | "bench" => "Release",
            _ => "Debug"
        };
        println!("cargo:rustc-link-search=native={}/build/src/{}", dst.display(), profile);
    } else {
        println!("cargo:rustc-link-search=native={}/build/src", dst.display());
    }
    println!("cargo:rustc-link-lib=static=rdkafka");
}
