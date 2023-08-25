#!/usr/bin/env bash

# rd_kafka_conf_set_open_cb/rd_kafka_conf_set_resolve_cb are blocklisted
# because it is not compiled on Windows due to its usage of the Unix-only
# `mode_t` type. With a bit of elbow grease we could include it if not
# targeting Windows, but it doesn't seem worthwhile at the moment.

bindgen \
    --no-doc-comments \
    --no-layout-tests \
    --rustified-enum ".*" \
    --allowlist-function "rd_kafka.*" \
    --allowlist-type "rd_kafka.*" \
    --allowlist-var "rd_kafka.*|RD_KAFKA_.*" \
    --no-recursive-allowlist \
    --blocklist-function "rd_kafka_conf_set_open_cb" \
    --blocklist-function "rd_kafka_conf_set_resolve_cb" \
    --raw-line "use libc::{FILE, sockaddr, c_int, c_void, c_char};" \
    --raw-line "use num_enum::TryFromPrimitive;" \
    --default-macro-constant-type "signed" \
    "bindings.h" -o "src/bindings.rs"

# Derive TryFromPrimitive for rd_kafka_resp_err_t.
perl -i -p0e 's/#\[derive\((.*)\)\]\npub enum rd_kafka_resp_err_t/#\[derive($1, TryFromPrimitive)\]\npub enum rd_kafka_resp_err_t/s' src/bindings.rs

# Clean up the bindings a bit.

sed \
    -e 's/::std::option::Option/Option/' \
    -e 's/::std::os::raw::c_int/c_int/' \
    -e 's/::std::os::raw::c_void/c_void/' \
    -e 's/::std::os::raw::c_char/c_char/' \
    src/bindings.rs > src/bindings.rs.new

mv src/bindings.rs{.new,}

rustfmt src/bindings.rs
