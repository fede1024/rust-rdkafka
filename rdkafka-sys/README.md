# rdkafka-sys

Low level bindings to [librdkafka](https://github.com/edenhill/librdkafka).

## Bindings

To regenerate the bindings:

``` bash
git submodule update --init
cargo install bindgen
./update-bindings.sh
```

## Version

The rdkafka-sys version number is in the format `X.Y.Z-P`, where `X.Y.Z`
corresponds to the librdkafka version, and `P` indicates the version of the
rust bindings.

## Build

By default a submodule with the librdkafka sources pinned to a specific commit
will be used to compile and statically link the library.

The **`dynamic-linking`** feature can be used to link rdkafka to a locally
installed version of librdkafka: if the feature is enabled, the build script
will use `pkg-config` to check the version of the library installed in the
system, and it will configure the compiler to dynamically link against it.

The **`cmake-build`** feature builds librdkafka with its [CMake] build system,
rather than its default build system, which uses a bespoke tool called [mklove].
The CMake build system is more reliable and should be preferred, as it supports
out-of-tree builds and cross-compilation, but it requires that CMake be
installed on the system.

The following features directly correspond to librdkafka features (i.e., flags
you would pass to `configure` if you were compiling manually).

  * The **`ssl`** feature enables SSL support. By default, the system's OpenSSL
    library is dynamically linked, but static linking of the version bundled
    with the openssl-sys crate can be requested with the `ssl-vendored` feature.
  * The **`gssapi`** feature enables SASL GSSAPI support with Cyrus libsasl2.
    This feature requires that libsasl2 is installed on the system, as there is
    not yet a libsasl2-sys crate that can build and link against a bundled
    copy of the library.
  * The **`libz`** feature enables support for zlib compression. This
    feature is enabled by default. By default, the system's libz is dynamically
    linked, but static linking of the version bundled with the libz-sys crate
    can be requested with the `libz-static` feature.
  * The **`zstd`** feature enables support for ZSTD compression. By default,
    this builds and statically links the version bundled with the zstd-sys
    crate, but dynamic linking of the system's version can be requested with the
    `zstd-pkg-config` feature.
  * The **`external-lz4`** feature statically links against the copy of liblz4
    bundled with the lz4-sys crate. By default, librdkafka statically links
    against its own bundled version of liblz4. Due to limitations with lz4-sys,
    it is not yet possible to dynamically link against the system's version of
    liblz4.

All features are disabled by default unless noted otherwise above. The build
process is defined in [`build.rs`](build.rs).

[CMake]: https://cmake.org
[mklove]: https://github.com/edenhill/mklove

## Updating

To upgrade change the git submodule in `librdkafka`, check if new errors
need to be added to `helpers::primive_to_rd_kafka_resp_err_t` and update
the version in `Cargo.toml`.
