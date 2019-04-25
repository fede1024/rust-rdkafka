FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y curl
RUN apt-get install -y openssl libssl-dev
RUN apt-get install -y pkg-config
RUN apt-get install -y valgrind
RUN apt-get install -y zlib1g-dev
RUN apt-get install -y python
RUN apt-get install -y llvm-3.9-dev libclang-3.9-dev clang-3.9

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain nightly
ENV PATH /root/.cargo/bin/:$PATH

# # Create dummy project for rdkafka
# COPY Cargo.toml /rdkafka/
# RUN mkdir -p /rdkafka/src && echo "fn main() {}" > /rdkafka/src/main.rs
#
# # Create dummy project for rdkafka
# RUN mkdir /rdkafka/rdkafka-sys
# COPY rdkafka-sys/Cargo.toml /rdkafka/rdkafka-sys
# RUN mkdir -p /rdkafka/rdkafka-sys/src && touch /rdkafka/rdkafka-sys/src/lib.rs
# RUN echo "fn main() {}" > /rdkafka/rdkafka-sys/build.rs
#
# RUN cd /rdkafka && test --no-run

COPY docker/run_tests.sh /rdkafka/

ENV KAFKA_HOST kafka:9092

WORKDIR /rdkafka
