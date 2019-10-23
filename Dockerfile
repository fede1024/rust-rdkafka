FROM ubuntu:16.04

RUN apt-get update && apt-get install -y build-essential \
    curl \
    openssl libssl-dev \
    pkg-config \
    python \
    valgrind \
    zlib1g-dev

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly-2019-10-17
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
