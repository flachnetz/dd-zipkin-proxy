#!/bin/sh

# forward parameters to dd-agent
/dd-zipkin-proxy "$@" &

# run the trace-agent
exec /opt/datadog-agent/bin/trace-agent
