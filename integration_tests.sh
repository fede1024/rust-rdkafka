#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

set -e

git submodule update --init
docker-compose stop
docker-compose build
docker-compose up --abort-on-container-exit

failures=`docker inspect -f '{{ .State.ExitCode }}' rustrdkafka_itest_1`

if [ "$failures" != "0" ]; then
    echo -e "${RED}One or more terminated with errors${NC}"
    echo -e "${RED}Integration tests failed${NC}"
    exit 1
else
    echo -e "${GREEN}Ingtegration tests succeeded${NC}"
fi
