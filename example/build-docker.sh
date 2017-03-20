#!/bin/sh

set -e

CGO_ENABLED=0 GOOS=linux ./build.sh

docker build --pull -t flachnetz/dd-zipkin-proxy .
