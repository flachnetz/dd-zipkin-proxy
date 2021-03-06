package balance

import (
	"github.com/Shopify/sarama"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
)

var metricKafkaSpanSize = metrics.NewRegisteredHistogram("kafka.span.size", nil, metrics.NewUniformSample(1024))

type Sender struct {
	producer sarama.AsyncProducer
	topic    string
}

func NewSender(client sarama.Client, topic string) (*Sender, error) {
	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, errors.WithMessage(err, "initialize kafka producer")
	}

	return &Sender{producer, topic}, nil
}

func (s *Sender) Send(spans <-chan proxy.Span) {
	for span := range spans {
		s.sendSpan(span)
	}
}

func (s *Sender) sendSpan(span proxy.Span) {
	s.producer.Input() <- makeProducerMessage(span, s.topic)
}
