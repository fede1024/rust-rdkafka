#!/usr/bin/env bash

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

set -euo pipefail

echo_good() {
    tput setaf 2
    echo "$@"
    tput sgr0
}

echo_bad() {
    tput setaf 1
    echo "$@"
    tput sgr0
}

run_with_valgrind() {
    if ! valgrind --error-exitcode=100 --suppressions=rdkafka.suppressions --gen-suppressions=all --leak-check=full "$1" --nocapture --test-threads=1
    then
        echo_bad "*** Failure in $1 ***"
        exit 1
    fi
}

# Initialize.

git submodule update --init
cargo test --no-run
docker-compose up -d

# Run unit tests.

echo_good "*** Run unit tests ***"
for test_file in target/debug/deps/rdkafka-*
do
    if [[ -x "$test_file" ]]
    then
        echo_good "Executing "$test_file""
        run_with_valgrind "$test_file"
    fi
done
echo_good "*** Unit tests succeeded ***"

# Run integration tests.

echo_good "*** Run unit tests ***"
for test_file in target/debug/deps/test_*
do
    if [[ -x "$test_file" ]]
    then
        echo_good "Executing "$test_file""
        run_with_valgrind "$test_file"
    fi
done
echo_good "*** Integration tests succeeded ***"

# Run smol runtime example.

echo_good "*** Run runtime_smol example ***"
cargo run --example runtime_smol --no-default-features --features cmake-build -- --topic smol
echo_good "*** runtime_smol example succeeded ***"

# Run async-std runtime example.

echo_good "*** Run runtime_async_std example ***"
cargo run --example runtime_async_std --no-default-features --features cmake-build -- --topic async-std
echo_good "*** runtime_async_std example succeeded ***"
