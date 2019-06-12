package balance

import (
	"bytes"
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/flachnetz/dd-zipkin-proxy/balance/avro"
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
	avroSpan := avro.Span{
		Id:     int64(span.Id),
		Trace:  int64(span.Trace),
		Parent: int64(span.Parent),

		Name:    span.Name,
		Service: span.Service,

		TimestampInNanos: span.Timestamp.ToNanos(),
		DurationInNanos:  span.Duration.Nanoseconds(),

		CrInNanos: span.Timings.CR.ToNanos(),
		CsInNanos: span.Timings.CS.ToNanos(),
		SrInNanos: span.Timings.CR.ToNanos(),
		SsInNanos: span.Timings.SS.ToNanos(),

		Tags: span.Tags,
	}
	var buf bytes.Buffer
	_ = avroSpan.Serialize(&buf)
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
