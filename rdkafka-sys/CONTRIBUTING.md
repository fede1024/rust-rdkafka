# Maintainer and contributor instructions

## Upgrading librdkafka

To update to a new version of librdkafka:

``` bash
git submodule update --init
cd rdkafka-sys/librdkafka
git checkout $DESIRED_VERSION
cargo install bindgen
./update-bindings.sh
```

Then:

  * Add a changelog entry to rdkafka-sys/changelog.md.
  * Update src/lib.rs with the new version.

## Releasing

* Ensure the changelog is up to date.
* Ensure Cargo.toml is up to date.
* Run `cargo publish`.
