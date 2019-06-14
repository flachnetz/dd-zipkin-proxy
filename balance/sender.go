package balance

import (
	"bytes"
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/flachnetz/dd-zipkin-proxy/codec"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/pkg/errors"
)

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
	var buf bytes.Buffer
	_ = codec.BinaryEncode(span, &buf)

	s.producer.Input() <- &sarama.ProducerMessage{
		Key:   keyOf(span.Trace),
		Value: sarama.ByteEncoder(buf.Bytes()),
		Topic: s.topic,
	}
}

func keyOf(traceId proxy.Id) sarama.Encoder {
	var buffer [8]byte
	binary.LittleEndian.PutUint64(buffer[:], traceId.Uint64())
	return sarama.ByteEncoder(buffer[:])
}
