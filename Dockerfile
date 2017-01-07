FROM datadog/docker-dd-agent

RUN echo "deb http://apt-trace.datad0g.com.s3.amazonaws.com/ stable main" > /etc/apt/sources.list.d/datadog-trace.list \
 && apt-get update \
 && apt-get install -y ca-certificates supervisor \
 && apt-get install --no-install-recommends -y dd-trace-agent \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY entrypoint.sh /entrypoint.sh
COPY dd-zipkin-proxy /dd-zipkin-proxy

EXPOSE 7777/tcp 9411/tcp

ENTRYPOINT ["/entrypoint.sh"]
