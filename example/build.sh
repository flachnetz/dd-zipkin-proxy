#!/bin/sh

set -e

if ! which glide 2> /dev/null ; then
    echo "Install glide as dependency manager"
    go get -u github.com/Masterminds/glide/
fi

# format everything
go fmt *.go

# install dependencies and build file
cd ..
glide install
go build -a -o example/dd-zipkin-proxy ./example
