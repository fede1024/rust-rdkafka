extern crate num_cpus;
extern crate pkg_config;

use std::path::Path;
use std::process::{Command, self};
use std::io::Write;
use std::env;

macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

fn run_command_or_fail(dir: &str, cmd: &str, args: &[&str]) {
    println_stderr!("Running command: \"{} {}\" in dir: {}", cmd, args.join(" "), dir);
    let ret = Command::new(cmd).current_dir(dir).args(args).status();
    match ret.map(|status| (status.success(), status.code())) {
        Ok((true, _)) => { return },
        Ok((false, Some(c))) => { panic!("Command failed with error code {}", c) },
        Ok((false, None)) => { panic!("Command got killed") },
        Err(e) => { panic!("Command failed with error: {}", e) },
    }
}

fn main() {
    let librdkafka_version = env!("CARGO_PKG_VERSION")
        .split('-')
        .next()
        .expect("Crate version is not valid");

    if env::var("CARGO_FEATURE_DYNAMIC_LINKING").is_ok() {
        println_stderr!("Librdkafka will be linked dynamically");
        let pkg_probe = pkg_config::Config::new()
            .cargo_metadata(true)
            .atleast_version(librdkafka_version)
            .probe("rdkafka");

        match pkg_probe {
            Ok(library) => {
                println_stderr!("librdkafka found on the system:");
                println_stderr!("  Name: {:?}", library.libs);
                println_stderr!("  Path: {:?}", library.link_paths);
                println_stderr!("  Version: {}", library.version);
            }
            Err(_) => {
                println_stderr!("librdkafka {} cannot be found on the system", librdkafka_version);
                println_stderr!("Dynamic linking failed. Exiting.");
                process::exit(1);
            }
        }
    } else {
        println_stderr!("Building and linking librdkafka statically");
        build_librdkafka();
    }
}

fn build_librdkafka() {
    let mut configure_flags = Vec::new();

    if env::var("CARGO_FEATURE_SASL").is_ok() {
        configure_flags.push("--enable-sasl");
    } else {
        configure_flags.push("--disable-sasl");
    }

    if env::var("CARGO_FEATURE_SSL").is_ok() {
        configure_flags.push("--enable-ssl");
    } else {
        configure_flags.push("--disable-ssl");
    }

    configure_flags.push("--enable-static");

    if !Path::new("librdkafka/LICENSE").exists() {
        println_stderr!("Setting up submodules");
        run_command_or_fail("../", "git", &["submodule", "update", "--init"]);
    }

    println!("Configuring librdkafka");
    run_command_or_fail("librdkafka", "./configure", configure_flags.as_slice());

    println!("Compiling librdkafka");
    run_command_or_fail("librdkafka", "make", &["-j", &num_cpus::get().to_string()]);

    println!("cargo:rustc-link-search=native={}/librdkafka/src",
             env::current_dir().expect("Can't find current dir").display());
    println!("cargo:rustc-link-lib=static=rdkafka");
}
