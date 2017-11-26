#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

mirror=$(curl --stderr /dev/null https://www.apache.org/dyn/closer.cgi\?as_json\=1 | jq -r '.preferred')
url="${mirror}kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
wget -q "${url}" -O "/tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

if [ "$?" != "0" ]; then
    echo -e "${RED}*** Failure while downloading Kafka version \"${KAFKA_VERSION}\" ***${NC}"
    exit 1
fi
echo -e "${GREEN}*** Kafka \"${KAFKA_VERSION}\" downloaded successfully ***${NC}"
