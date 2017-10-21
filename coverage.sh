#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

EXCLUDE="/.cargo,/usr/lib,/usr/include,rdkafka-sys/librdkafka,rdkafka-sys/src/bindings,tests"
TARGET="target/cov"

RDKAFKA_UNIT_TESTS="target/debug/rdkafka-"
RDKAFKASYS_UNIT_TESTS="rdkafka-sys/target/debug/rdkafka_sys-"
INTEGRATION_TESTS="target/debug/test_"

export RUSTFLAGS="-C link-dead-code"

echo -e "${GREEN}*** Clean previous coverage results and executables ***${NC}"
rm -rf "$TARGET"
rm -f "$RDKAFKA_UNIT_TESTS"*
rm -f "$RDKAFKASYS_UNIT_TESTS"*
rm -f "$INTEGRATION_TESTS"*

echo -e "${GREEN}*** Rebuilding tests ***${NC}"
cargo test --no-run
pushd rdkafka-sys && cargo test --no-run && popd

echo -e "${GREEN}*** Run coverage on unit tests ***${NC}"
kcov --exclude-pattern="$EXCLUDE" --verify "$TARGET" "$RDKAFKA_UNIT_TESTS"*
if [ "$?" != "0" ]; then
    echo -e "${RED}*** Failure during unit test converage ***${NC}"
    exit 1
fi

kcov --exclude-pattern="$EXCLUDE" --verify "$TARGET" "$RDKAFKASYS_UNIT_TESTS"*
if [ "$?" != "0" ]; then
    echo -e "${RED}*** Failure during rdkafka-sys unit test converage ***${NC}"
    exit 1
fi

echo -e "${GREEN}*** Run coverage on integration tests ***${NC}"

for test_file in `ls "$INTEGRATION_TESTS"*`
do
    echo -e "${GREEN}Executing "$test_file"${NC}"
    kcov --exclude-pattern="$EXCLUDE" --verify "$TARGET" "$test_file"
    if [ "$?" != "0" ]; then
        echo -e "${RED}*** Failure during integration converage ***${NC}"
        exit 1
    fi
done

echo -e "${GREEN}*** Coverage completed successfully ***${NC}"
