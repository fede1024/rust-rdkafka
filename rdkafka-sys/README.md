# rdkafka-sys

Low level bindings to [librdkafka](https://github.com/edenhill/librdkafka).

## Bindings

To regenerate the bindings:

``` bash
git submodule update --init
bindgen --builtins --convert-macros librdkafka/src/rdkafka.h > src/bindings/{platform}.rs
```

## Version

The rdkafka-sys version number is in the format `X.Y.Z-P`, where `X.Y.Z`
corresponds to the librdkafka version, and `P` indicates the version of the
rust bindings.

## Build

This crate will first check if there is an installed version of librdkafka on
the system using `pkg-config`. If the library is found and the version is the
one targeted by rdkafka-sys, rdkafka-sys will build using a dynamic link to the
installed library.

If those conditions are not met, a submodule with the librdkafka sourced pinned
to a specific commit will be used to compile and statically link the library.

The build process is defined in [`build.rs`].

[`build.rs`]: https://github.com/fede1024/rust-rdkafka/blob/master/rdkafka-sys/build.rs
