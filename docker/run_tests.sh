#!/bin/bash

# This script is supposed to run inside the docker container.

cp -r /mount/* /rdkafka

cd /rdkafka/rdkafka-sys/librdkafka
make clean > /dev/null 2>&1

cd /rdkafka/
rm target/debug/produce_consume_base_test-*
cargo test --no-run
# valgrind --leak-check=full target/debug/produce_consume_base_test-*
valgrind --error-exitcode=100 target/debug/produce_consume_base_test-*
