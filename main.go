package zipkinproxy

import (
	"github.com/Shopify/sarama"
	"github.com/flachnetz/go-admin"
	"github.com/flachnetz/startup/v2"
	"github.com/flachnetz/startup/v2/lib/kafka"
	. "github.com/flachnetz/startup/v2/startup_base"
	"github.com/flachnetz/startup/v2/startup_http"
	"github.com/flachnetz/startup/v2/startup_kafka"
	"github.com/flachnetz/startup/v2/startup_metrics"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/profile"
	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/flachnetz/dd-zipkin-proxy/balance"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/datadog"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"

	_ "github.com/apache/thrift/lib/go/thrift"
)

var log = logrus.WithField("prefix", "main")

type SpanConverter func(span proxy.Span) (proxy.Span, error)
type Routing func(*httprouter.Router) http.Handler

func Main(spanConverter SpanConverter) {
	routing := func(router *httprouter.Router) http.Handler {
		return router
	}

	MainWithRouting(routing, spanConverter)
}

func MainWithRouting(routing Routing, spanConverter SpanConverter) {
	var opts struct {
		Base    BaseOptions                    `group:"Base options"`
		HTTP    startup_http.HTTPOptions       `group:"HTTP server options"`
		Metrics startup_metrics.MetricsOptions `group:"Metrics configuration"`

		Kafka struct {
			startup_kafka.KafkaOptions

			Topic           string `long:"kafka-topic" default:"zipkin-spans" description:"Kafka topic to put spans to. Will be created if it does not exist"`
			ConsumerGroupId string `long:"kafka-consumer-group" default:"zipkin-proxy" description:"Name of the consumer group to use to load balance zipkin spans."`
		} `group:"Load balancing configuration"`

		ProfileCPU bool `long:"profile" description:"Enable CPU profiling"`

		TraceAgent struct {
			Host string `long:"trace-host" default:"localhost" description:"Hostname of the trace agent."`
			Port int    `long:"trace-port" default:"8126" description:"Port of the trace agent."`
		}
	}

	opts.Metrics.Inputs.MetricsPrefix = "zipkin.proxy"
	opts.Kafka.Inputs.KafkaConfig = sarama.NewConfig()
	opts.Kafka.Inputs.KafkaConfig.Version = sarama.V1_1_1_0
	opts.Kafka.Inputs.KafkaConfig.Consumer.Fetch.Min = 128 * 1024
	opts.Kafka.Inputs.KafkaConfig.Consumer.Fetch.Max = 1024 * 1024
	opts.Kafka.Inputs.KafkaConfig.Consumer.MaxWaitTime = 1 * time.Second
	opts.Kafka.Inputs.KafkaConfig.Producer.RequiredAcks = sarama.NoResponse
	opts.Kafka.Inputs.KafkaConfig.Producer.Flush.Frequency = 1 * time.Second
	opts.Kafka.Inputs.KafkaConfig.Producer.MaxMessageBytes = 768 * 1024
	opts.Kafka.Inputs.KafkaConfig.ChannelBufferSize = 64

	startup.MustParseCommandLine(&opts)

	cache.RegisterCacheMetrics(metrics.DefaultRegistry)

	if opts.ProfileCPU {
		defer profile.Start().Stop()
	}

	var channels []chan<- proxy.Trace

	if true {
		log.Info("Enable forwarding of spans to datadog trace-agent")
		transport := datadog.DefaultTransport(opts.TraceAgent.Host, strconv.Itoa(opts.TraceAgent.Port))

		// accept zipkin traces
		traces := make(chan proxy.Trace, 256)
		channels = append(channels, traces)

		go datadog.Sink(transport, traces)
	}

	// a channel to store the last spans that were received
	var buffer *SpansBuffer
	{
		spans := make(chan proxy.Trace, 256)
		channels = append(channels, spans)

		// just keep references to previous spans.
		buffer = NewSpansBuffer(2048)
		go buffer.ReadFrom(spans)
	}

	// multiplex input channel to all the target channels
	processedSpans := make(chan proxy.Trace, 64)
	go forwardSpansToChannels(processedSpans, channels, spanConverter)

	// http handler will put spans into this channel
	httpInputSpans := make(chan proxy.Span, 256)

	if len(opts.Kafka.Addresses) > 0 {
		log.Infof(
			"Kafka load balancing activated, processing spans from topic %s in consumer group %s",
			opts.Kafka.Topic, opts.Kafka.ConsumerGroupId)

		log.Debugf("Connect to kafka brokers at %s", strings.Join(opts.Kafka.Addresses, ", "))
		client := opts.Kafka.KafkaClient("dd-zipkin-proxy")

		log.Debugf("Ensure that topic %s exists", opts.Kafka.Topic)
		topic := kafka.Topic{
			Name: opts.Kafka.Topic, NumPartitions: 12, ReplicationFactor: 1,
			Config: map[string]*string{
				"retention.ms":    toStringPtr(strconv.Itoa(int(10 * time.Minute / time.Millisecond))),
				"retention.bytes": toStringPtr(strconv.Itoa(64 * 1024 * 1024)),
				"segment.bytes":   toStringPtr(strconv.Itoa(4 * 1024 * 1024)),
			}}

		err := kafka.EnsureTopics(client, kafka.Topics{topic})
		FatalOnError(err, "Ensure that the topic exists failed")

		log.Debugf("Create kafka span sender")
		kafkaSender, err := balance.NewSender(client, opts.Kafka.Topic)
		FatalOnError(err, "Create kafka sender for spans failed")

		// send spans to kafka
		go kafkaSender.Send(httpInputSpans)

		kafkaInputSpans := make(chan proxy.Span, 256)

		log.Debugf("Create consumer group with name %s", opts.Kafka.ConsumerGroupId)
		consumerGroup, err := sarama.NewConsumerGroupFromClient(opts.Kafka.ConsumerGroupId, client)
		FatalOnError(err, "Cannot create consumer for group %s", opts.Kafka.ConsumerGroupId)

		log.Debugf("Start consuming topic %s", opts.Kafka.Topic)
		callback := func(span proxy.Span) { kafkaInputSpans <- span }
		closeConsumerGroup := balance.Consume(consumerGroup, opts.Kafka.Topic, callback)

		//noinspection ALL
		defer closeConsumerGroup()

		// send spans received from kafka to processing
		go ErrorCorrectSpans(kafkaInputSpans, processedSpans)

	} else {
		log.Infof("No kafka load balancing activated, processing spans from http handler only")

		// directly process all input spans
		go ErrorCorrectSpans(httpInputSpans, processedSpans)
	}

	log.Info("Setup completed, starting http listener now")

	opts.HTTP.Serve(startup_http.Config{
		Name: "dd-zipkin-proxy",

		AdminHandlers: []admin.RouteConfig{
			admin.Describe("A buffer of the previous traces (in openzipkin-format) in the order they were received.",
				admin.WithGenericValue("/spans", buffer.ToSlice)),
		},

		Routing: func(router *httprouter.Router) http.Handler {
			handleSpans(router, httpInputSpans)
			return handleGzipRequestBody(routing(router))
		},
	})
}

func toStringPtr(stringValue string) *string {
	return &stringValue
}

func forwardSpansToChannels(source <-chan proxy.Trace, targets []chan<- proxy.Trace, converter SpanConverter) {
	processTrace := func(trace proxy.Trace) {
		// we re-use the same slice for the target and just overwrite
		// the spans in there.
		var result = trace[:0]

		for idx := range trace {
			converted, err := converter(trace[idx])
			if err != nil || converted.Id == 0 || converted.Trace == 0 {
				continue
			}

			result = append(result, converted)
		}

		for _, target := range targets {
			target <- result
		}
	}

	for trace := range source {
		processTrace(trace)
	}
}
