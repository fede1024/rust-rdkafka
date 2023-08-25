use std::borrow::Borrow;
use std::env;
use std::ffi::OsStr;
#[cfg(feature = "cmake-build")]
use std::fs;
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
        Ok((true, _)) => (),
        Ok((false, Some(c))) => panic!("Command failed with error code {}", c),
        Ok((false, None)) => panic!("Command got killed"),
        Err(e) => panic!("Command failed with error: {}", e),
    }
}

fn main() {
    if env::var("CARGO_FEATURE_DYNAMIC_LINKING").is_ok() {
        eprintln!("librdkafka will be linked dynamically");

        let librdkafka_version = match env!("CARGO_PKG_VERSION")
            .split('+')
            .collect::<Vec<_>>()
            .as_slice()
        {
            [_rdsys_version, librdkafka_version] => *librdkafka_version,
            _ => panic!("Version format is not valid"),
        };

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
            Err(err) => {
                eprintln!(
                    "librdkafka {} cannot be found on the system: {}",
                    librdkafka_version, err
                );
                eprintln!("Dynamic linking failed. Exiting.");
                process::exit(1);
            }
        }
    } else {
        // Ensure that we are in the right directory
        let rdkafkasys_root = Path::new("rdkafka-sys");
        if rdkafkasys_root.exists() {
            assert!(env::set_current_dir(&rdkafkasys_root).is_ok());
        }
        if !Path::new("librdkafka/LICENSE").exists() {
            eprintln!("Setting up submodules");
            run_command_or_fail("../", "git", &["submodule", "update", "--init"]);
        }
        eprintln!("Building and linking librdkafka statically");
        build_librdkafka();
    }
}

#[cfg(not(feature = "cmake-build"))]
fn build_librdkafka() {
    let mut configure_flags: Vec<String> = Vec::new();

    let mut cflags = Vec::new();
    if let Ok(var) = env::var("CFLAGS") {
        cflags.push(var);
    }

    let mut ldflags = Vec::new();
    if let Ok(var) = env::var("LDFLAGS") {
        ldflags.push(var);
    }

    if env::var("CARGO_FEATURE_SSL").is_ok() {
        configure_flags.push("--enable-ssl".into());
        if let Ok(openssl_root) = env::var("DEP_OPENSSL_ROOT") {
            cflags.push(format!("-I{}/include", openssl_root));
            ldflags.push(format!("-L{}/lib", openssl_root));
        }
    } else {
        configure_flags.push("--disable-ssl".into());
    }

    if env::var("CARGO_FEATURE_GSSAPI").is_ok() {
        configure_flags.push("--enable-gssapi".into());
        if let Ok(sasl2_root) = env::var("DEP_SASL2_ROOT") {
            cflags.push(format!("-I{}/include", sasl2_root));
            ldflags.push(format!("-L{}/build", sasl2_root));
        }
    } else {
        configure_flags.push("--disable-gssapi".into());
    }

    if env::var("CARGO_FEATURE_LIBZ").is_ok() {
        // There is no --enable-zlib option, but it is enabled by default.
        if let Ok(z_root) = env::var("DEP_Z_ROOT") {
            cflags.push(format!("-I{}/include", z_root));
            ldflags.push(format!("-L{}/build", z_root));
        }
    } else {
        configure_flags.push("--disable-zlib".into());
    }

    if env::var("CARGO_FEATURE_CURL").is_ok() {
        // There is no --enable-curl option, but it is enabled by default.
        if let Ok(curl_root) = env::var("DEP_CURL_ROOT") {
            cflags.push("-DCURLSTATIC_LIB".to_string());
            cflags.push(format!("-I{}/include", curl_root));
        }
    } else {
        configure_flags.push("--disable-curl".into());
    }

    if env::var("CARGO_FEATURE_ZSTD").is_ok() {
        configure_flags.push("--enable-zstd".into());
        if let Ok(zstd_root) = env::var("DEP_ZSTD_ROOT") {
            cflags.push(format!("-I{}/include", zstd_root));
            ldflags.push(format!("-L{}", zstd_root));
        }
    } else {
        configure_flags.push("--disable-zstd".into());
    }

    if env::var("CARGO_FEATURE_EXTERNAL_LZ4").is_ok() {
        configure_flags.push("--enable-lz4-ext".into());
        if let Ok(lz4_root) = env::var("DEP_LZ4_ROOT") {
            cflags.push(format!("-I{}/include", lz4_root));
            ldflags.push(format!("-L{}", lz4_root));
        }
    } else {
        configure_flags.push("--disable-lz4-ext".into());
    }

    env::set_var("CFLAGS", cflags.join(" "));
    env::set_var("LDFLAGS", ldflags.join(" "));

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR missing");

    if !Path::new(&out_dir).join("LICENSE").exists() {
        // We're not allowed to build in-tree directly, as ~/.cargo/registry is
        // globally shared. mklove doesn't support out-of-tree builds [0], so we
        // work around the issue by creating a clone of librdkafka inside of
        // OUT_DIR, and build inside of *that* tree.
        //
        // https://github.com/edenhill/mklove/issues/17
        println!("Cloning librdkafka");
        run_command_or_fail(".", "cp", &["-a", "librdkafka/.", &out_dir]);
    }

    println!("Configuring librdkafka");
    run_command_or_fail(&out_dir, "./configure", configure_flags.as_slice());

    println!("Compiling librdkafka");
    if let Some(makeflags) = env::var_os("CARGO_MAKEFLAGS") {
        env::set_var("MAKEFLAGS", makeflags);
    }
    run_command_or_fail(
        &out_dir,
        if cfg!(target_os = "freebsd") {
            "gmake"
        } else {
            "make"
        },
        &["libs"],
    );

    println!("cargo:rustc-link-search=native={}/src", out_dir);
    println!("cargo:rustc-link-lib=static=rdkafka");
    println!("cargo:root={}", out_dir);
}

#[cfg(feature = "cmake-build")]
fn build_librdkafka() {
    let mut config = cmake::Config::new("librdkafka");
    let mut cmake_library_paths = vec![];

    config
        .define("RDKAFKA_BUILD_STATIC", "1")
        .define("RDKAFKA_BUILD_TESTS", "0")
        .define("RDKAFKA_BUILD_EXAMPLES", "0")
        // CMAKE_INSTALL_LIBDIR is inferred as "lib64" on some platforms, but we
        // want a stable location that we can add to the linker search path.
        // Since we're not actually installing to /usr or /usr/local, there's no
        // harm to always using "lib" here.
        .define("CMAKE_INSTALL_LIBDIR", "lib");

    if env::var("CARGO_FEATURE_LIBZ").is_ok() {
        config.define("WITH_ZLIB", "1");
        config.register_dep("z");
        if let Ok(z_root) = env::var("DEP_Z_ROOT") {
            cmake_library_paths.push(format!("{}/build", z_root));
        }
    } else {
        config.define("WITH_ZLIB", "0");
    }

    if env::var("CARGO_FEATURE_CURL").is_ok() {
        config.define("WITH_CURL", "1");
        config.register_dep("curl");
        if let Ok(curl_root) = env::var("DEP_CURL_ROOT") {
            config.define("CURL_STATICLIB", "1");
            cmake_library_paths.push(format!("{}/lib", curl_root));

            config.cflag("-DCURL_STATICLIB");
            config.cxxflag("-DCURL_STATICLIB");
            config.cflag(format!("-I{}/include", curl_root));
            config.cxxflag(format!("-I{}/include", curl_root));
            config.cflag(format!("-L{}/lib", curl_root));
            config.cxxflag(format!("-L{}/lib", curl_root));
            //FIXME: Upstream should be copying this in their build.rs
            fs::copy(
                format!("{}/build/libcurl.a", curl_root),
                format!("{}/lib/libcurl.a", curl_root),
            )
            .unwrap();
        }
    } else {
        config.define("WITH_CURL", "0");
    }

    if env::var("CARGO_FEATURE_SSL").is_ok() {
        config.define("WITH_SSL", "1");
        config.define("WITH_SASL_SCRAM", "1");
        config.define("WITH_SASL_OAUTHBEARER", "1");
        config.register_dep("openssl");
    } else {
        config.define("WITH_SSL", "0");
    }

    if env::var("CARGO_FEATURE_GSSAPI").is_ok() {
        config.define("WITH_SASL", "1");
        config.register_dep("sasl2");
        if let Ok(sasl2_root) = env::var("DEP_SASL2_ROOT") {
            config.cflag(format!("-I{}/include", sasl2_root));
            config.cxxflag(format!("-I{}/include", sasl2_root));
        }
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
        config.register_dep("lz4");
    } else {
        config.define("ENABLE_LZ4_EXT", "0");
    }

    if let Ok(system_name) = env::var("CMAKE_SYSTEM_NAME") {
        config.define("CMAKE_SYSTEM_NAME", system_name);
    }

    if !cmake_library_paths.is_empty() {
        env::set_var("CMAKE_LIBRARY_PATH", cmake_library_paths.join(";"));
    }

    println!("Configuring and compiling librdkafka");
    let dst = config.build();

    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=static=rdkafka");
}
