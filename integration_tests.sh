#!/bin/bash

set -e

git submodule update --init
docker-compose stop
docker-compose build
docker-compose up --abort-on-container-exit

failures=`docker inspect -f '{{ .State.ExitCode }}' rustrdkafka_itest_1`

if [ "$failures" != "0" ]; then
    echo "One or more container failed"
    exit 1
fi
