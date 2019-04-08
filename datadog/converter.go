package datadog

import (
	"github.com/openzipkin-contrib/zipkin-go-opentracing/thrift/gen-go/zipkincore"
	"github.com/pkg/errors"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"reflect"
	"time"
	"unsafe"
)

func Initialize(addr string) {
	// sets the global instance
	tracer.Start(tracer.WithAgentAddr(addr))
}

func sinkSpan(span *zipkincore.Span) tracer.Span {
	startTime := time.Unix(0, span.GetTimestamp()*int64(time.Microsecond))
	finishTime := startTime.Add(time.Duration(span.GetDuration()) * time.Microsecond)

	tags := map[string]interface{}{}
	for _, an := range span.BinaryAnnotations {
		value := string(an.Value)

		switch an.Key {
		case "dd.name":
			tags[ext.SpanName] = value

		case "dd.service":
			tags[ext.ServiceName] = value

		case "dd.resource":
			tags[ext.ResourceName] = value

		default:
			tags[an.Key] = string(an.Value)
		}
	}

	name := span.Name
	if name == "" {
		name = "unknown"
	}

	var ddSpan ddtrace.Span = tracer.StartSpan(name, func(cfg *ddtrace.StartSpanConfig) {
		cfg.StartTime = startTime
		cfg.SpanID = uint64(span.ID)
		cfg.Tags = tags
	})

	var context ddtrace.SpanContext = ddSpan.Context()

	refSpan := reflect.ValueOf(ddSpan)

	// validate type
	refSpanType := refSpan.Type()
	if refSpanType.Kind() != reflect.Ptr || refSpanType.String() != "*tracer.span" {
		panic(errors.Errorf("Expected Span of type *tracer.span, got '%s'", refSpanType.String()))
	}

	// dereference the pointer value
	refSpan = refSpan.Elem()

	refSpan.FieldByName("TraceID").SetUint(uint64(span.TraceID))

	if span.ParentID != nil {
		refSpan.FieldByName("ParentID").SetUint(uint64(*span.ParentID))
	}

	refContext := reflect.ValueOf(context).Elem()
	setUnexportetFieldValue(refContext.FieldByName("traceID"), uint64(span.TraceID))
	setUnexportetFieldValue(refContext.FieldByName("spanID"), uint64(span.ID))

	ddSpan.Finish(tracer.FinishTime(finishTime))

	return ddSpan
}

func setUnexportetFieldValue(target reflect.Value, value uint64) {
	if target.Kind() != reflect.Uint64 {
		panic(errors.Errorf("Expect type uint64, got %s", target.Kind()))
	}

	ptr := unsafe.Pointer(target.UnsafeAddr())
	*(*uint64)(ptr) = value
}

// Reads all zipkin spans from the given channel, converts them to datadog spans using the given converter
// and write them into another channel.
func Sink(zipkinSpans <-chan *zipkincore.Span) {
	for span := range zipkinSpans {
		sinkSpan(span)
	}
}
