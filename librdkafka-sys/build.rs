extern crate num_cpus;

// use std::path::Path;
use std::process::Command;
use std::io::Write;
use std::env;

macro_rules! t {
    ($e:expr) => (match $e{
        Ok(e) => e,
        Err(e) => panic!("{} failed with {}", stringify!($e), e),
    })
}

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
    // if !Path::new("librdkafka/.git").exists() {
    //     println!("Setting up submodules");
    //     run_command_or_fail(".", "git", &["submodule", "update", "--init"]);
    // }
    println!("Configuring librdkafka");
    run_command_or_fail("librdkafka", "./configure", &["--disable-sasl", "--disable-ssl"]);
    println!("Compiling librdkafka");
    run_command_or_fail("librdkafka", "make", &["-j", &num_cpus::get().to_string()]);
    println!("cargo:rustc-link-search=native={}/librdkafka/src", env::current_dir().expect("Can't find current dir").display());
    println!("cargo:rustc-link-search=/usr/local/opt/openssl/lib");
    println!("cargo:rustc-link-lib=static=rdkafka");
    println!("cargo:rustc-link-lib=dylib=crypto");
    println!("cargo:rustc-link-lib=dylib=ssl");
    println!("cargo:rustc-link-lib=dylib=z");
    println!("cargo:libdir=/usr/local/opt/openssl/lib");
    println!("cargo:include=/usr/local/opt/openssl/include");
}
