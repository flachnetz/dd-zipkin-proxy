#!/bin/sh

set -e

CGO_ENABLED=0 ./build.sh

docker build -t flachnetz/dd-zipkin-proxy .
