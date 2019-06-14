package balance

import (
	"bytes"
	"context"
	"github.com/Shopify/sarama"
	"github.com/flachnetz/dd-zipkin-proxy/codec"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var log = logrus.WithField("prefix", "balance")

type SpanCallback func(proxy.Span)

func Consume(consumerGroup sarama.ConsumerGroup, topic string, callback SpanCallback) func() {
	handler := &consumerGroupHandler{
		callback: callback,
	}

	ctx, cancel := context.WithCancel(context.Background())

	finishedCh := make(chan bool)

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(finishedCh)
				return

			default:
				if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
					log.Warnf("Error during consumer group session: %s", err)
				}
			}
		}
	}()

	return func() {
		cancel()
		<-finishedCh
	}
}

func decodeKafkaMessage(message *sarama.ConsumerMessage) (proxy.Span, error) {
	proxySpan, err := codec.BinaryDecode(bytes.NewReader(message.Value))
	return proxySpan, errors.WithMessage(err, "deserialize avro message")
}

// Represents a Sarama consumer group consumer
type consumerGroupHandler struct {
	callback SpanCallback
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	log.Debugf("Member of consumer group with generation id %d", session.GenerationID())
	for topic, partitions := range session.Claims() {
		for _, partition := range partitions {
			log.Debugf("  * claimed %s:%d", topic, partition)
		}
	}

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		// log.Debugf("Got message on topic %s:%d (%d bytes)", message.Topic, message.Partition, len(message.Value))

		proxySpan, err := decodeKafkaMessage(message)
		if err != nil {
			log.Warnf("Cannot deserialize kafka message: %s", err)
			continue
		}

		consumer.callback(proxySpan)

		session.MarkMessage(message, "")
	}

	return nil
}
