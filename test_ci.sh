#!/usr/bin/env bash

set -e

if [[ "$RDKAFKA_RUN_TESTS" ]]; then
    ./test_suite.sh
else
    cargo build --all-targets --verbose --features "$FEATURES"
fi
(cd rdkafka-sys && cargo test --features "$FEATURES")
