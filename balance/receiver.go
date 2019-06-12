package balance

import (
	"bytes"
	"github.com/Shopify/sarama"
	"github.com/flachnetz/dd-zipkin-proxy/balance/avro"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	. "github.com/flachnetz/startup/startup_logrus"
	"github.com/pkg/errors"
	"io"
	"time"
)

type SpanCallback func(proxy.Span)

func Consume(consumer sarama.Consumer, topic string, partition int32, callback SpanCallback) (io.Closer, error) {
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return nil, errors.WithMessage(err, "create partition consumer")
	}

	go func() {
		for msg := range partitionConsumer.Messages() {
			span, err := avro.DeserializeSpan(bytes.NewBuffer(msg.Value))
			if err != nil {
				GetLogger(nil, Consume).Warnf("Cannot deserialize message: %s", err)
				continue
			}

			proxySpan := proxy.Span{
				Id:     proxy.Id(span.Id),
				Parent: proxy.Id(span.Parent),
				Trace:  proxy.Id(span.Trace),

				Name:    cache.String(span.Name),
				Service: cache.String(span.Service),

				Timestamp: proxy.Timestamp(span.TimestampInNanos),
				Duration:  time.Duration(span.DurationInNanos),

				Timings: proxy.Timings{
					CR: proxy.Timestamp(span.CrInNanos),
					CS: proxy.Timestamp(span.CsInNanos),
					SR: proxy.Timestamp(span.SrInNanos),
					SS: proxy.Timestamp(span.SsInNanos),
				},
			}

			// internalize tags
			proxySpan.Tags = make(map[string]string, len(span.Tags))
			for key, value := range span.Tags {
				proxySpan.Tags[cache.String(key)] = cache.String(value)
			}

			callback(proxySpan)
		}
	}()

	return partitionConsumer, nil
}
