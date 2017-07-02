#!/bin/bash

# This script is supposed to run inside the docker container.

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}*** Clean previous build ***${NC}"
cp -r /mount/* /rdkafka
cd /rdkafka/rdkafka-sys/librdkafka
make clean > /dev/null 2>&1
cd /rdkafka/
rm target/debug/rdkafka-*
rm target/debug/produce_consume_base_test-*

echo -e "${GREEN}*** Inject system allocator ***${NC}"
sed -i "/\/\/>alloc_system/ c\#![feature(alloc_system)]\nextern crate alloc_system;" src/lib.rs

echo -e "${GREEN}*** Build tests ***${NC}"
cargo test --no-run
if [ "$?" != "0" ]; then
    echo -e "${RED}*** Failure during compilation ***${NC}"
    exit 1
fi

echo -e "${GREEN}*** Run unit tests ***${NC}"
valgrind --error-exitcode=100 --leak-check=full target/debug/rdkafka-* --nocapture

if [ "$?" != "0" ]; then
    echo -e "${RED}*** Failure in unit tests ***${NC}"
    exit 1
else
    echo -e "${GREEN}*** Unit tests succeeded ***${NC}"
fi

valgrind --error-exitcode=100 --leak-check=full target/debug/produce_consume_base_test-* --nocapture
if [ "$?" != "0" ]; then
    echo -e "${RED}*** Failure in integration tests ***${NC}"
    exit 1
else
    echo -e "${GREEN}*** Integration tests succeeded ***${NC}"
fi
