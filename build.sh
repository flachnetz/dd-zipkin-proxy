#!/bin/sh

set -e

# format everything
go fmt . ./datadog ./example ./zipkin

# install dependencies and build file
glide install
go build -o dd-zipkin-proxy ./example
