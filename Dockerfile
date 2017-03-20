FROM datadog/docker-dd-agent

COPY supervisor.conf.extra /tmp/
RUN cat /tmp/supervisor.conf.extra >> /etc/dd-agent/supervisor.conf \
    && sed -i s/programs=/programs=dd-zipkin-proxy,/g /etc/dd-agent/supervisor.conf \
    && echo "apm_enabled: true" >> /etc/dd-agent/datadog.conf

COPY dd-zipkin-proxy /dd-zipkin-proxy
EXPOSE 9411/tcp