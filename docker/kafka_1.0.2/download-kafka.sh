#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# We can't use a standard Apache mirror, since old versions of Kafka are
# aggressively garbage collected. They live forever on archive.apache.org,
# fortunately.
url="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
wget -q "${url}" -O "/tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

if [ "$?" != "0" ]; then
    echo -e "${RED}*** Failure while downloading Kafka version \"${KAFKA_VERSION}\" ***${NC}"
    exit 1
fi
echo -e "${GREEN}*** Kafka \"${KAFKA_VERSION}\" downloaded successfully ***${NC}"
