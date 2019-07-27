#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

set -e

git submodule update --init
docker-compose stop
docker-compose build
docker-compose up --abort-on-container-exit

failures=`docker inspect -f '{{ .State.ExitCode }}' itest`

if [ "$failures" != "0" ]; then
    echo -e "${RED}One or more container terminated with errors${NC}"
    echo -e "${RED}Test suite failed${NC}"
    docker-compose rm -f && exit 1
else
    echo -e "${GREEN}Test suite succeeded${NC}"
    docker-compose rm -f && exit 0
fi
