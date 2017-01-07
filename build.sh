#!/bin/sh

set -e

if ! which glide 2> /dev/null ; then
    echo "Install glide as dependency manager"
    go get -u github.com/Masterminds/glide/
fi

# format everything
go fmt . ./datadog ./example ./zipkin

# install dependencies and build file
glide install
go build -o dd-zipkin-proxy ./example
