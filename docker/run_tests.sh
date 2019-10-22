#!/bin/bash
set -x

# This script is supposed to run inside the docker container.

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

UNIT_TESTS="target/debug/rdkafka-"
INTEGRATION_TESTS="target/debug/test_"

echo -e "${GREEN}*** Clean previous build ***${NC}"
cp -r /mount/* /rdkafka
cd /rdkafka/rdkafka-sys/librdkafka
make clean > /dev/null 2>&1
cd /rdkafka/
rm -f "$UNIT_TESTS"*
rm -f "$INTEGRATION_TESTS"*

echo -e "${GREEN}*** Inject system allocator ***${NC}"
sed -i "/\/\/>alloc_system/use std::alloc::System;\n\#[global_allocator]\nstatic A: System = System;\n" src/lib.rs

echo -e "${GREEN}*** Build tests ***${NC}"
echo "Rust version: $(rustc --version)"
ls -l
cargo test --no-run
if [ "$?" != "0" ]; then
    echo -e "${RED}*** Failure during compilation ***${NC}"
    exit 1
fi

# UNIT TESTS

V_SUPP="rdkafka.suppressions"

echo -e "${GREEN}*** Run unit tests ***${NC}"
for test_file in `ls "$UNIT_TESTS"*`
do
    if [[ ! -x "$test_file" ]]; then
        continue
    fi
    echo -e "${GREEN}Executing "$test_file"${NC}"
    valgrind --error-exitcode=100 --suppressions="$V_SUPP" --leak-check=full "$test_file" --nocapture
    if [ "$?" != "0" ]; then
        echo -e "${RED}*** Failure in unit tests ***${NC}"
        exit 1
    fi
done
echo -e "${GREEN}*** Unit tests succeeded ***${NC}"

# INTEGRATION TESTS

for test_file in `ls "$INTEGRATION_TESTS"*`
do
    if [[ ! -x "$test_file" ]]; then
        continue
    fi
    echo -e "${GREEN}Executing "$test_file"${NC}"
    valgrind --error-exitcode=100 --suppressions="$V_SUPP" --leak-check=full "$test_file" --nocapture
    if [ "$?" != "0" ]; then
        echo -e "${RED}*** Failure in integration tests ***${NC}"
        exit 1
    fi
done

echo -e "${GREEN}*** Integration tests succeeded ***${NC}"
