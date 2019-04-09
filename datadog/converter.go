package datadog

import (
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/pkg/errors"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"reflect"
	"strings"
	"unsafe"
)

func Initialize(addr string) {
	// sets the global instance
	tracer.Start(tracer.WithAgentAddr(addr))
}

func sinkSpan(span proxy.Span) tracer.Span {
	tags := map[string]interface{}{}

	tags[ext.ServiceName] = span.Service
	tags[ext.SpanName] = span.Service
	tags[ext.ResourceName] = span.Name

	for key, value := range span.Tags {
		switch key {
		case "dd.name":
			tags[ext.SpanName] = value

		case "dd.service":
			tags[ext.ServiceName] = value

		case "dd.resource":
			tags[ext.ResourceName] = value

		default:
			tags[key] = string(value)
		}
	}

	name := span.Name
	if name == "" {
		name = "unknown"
	}

	var timingTags []string
	for timingTag := range span.Timings {
		timingTags = append(timingTags, timingTag)
	}

	if len(timingTags) > 0 {
		tags["timingTags"] = strings.Join(timingTags, ",")
	}

	var ddSpan ddtrace.Span = tracer.StartSpan(name, func(cfg *ddtrace.StartSpanConfig) {
		cfg.StartTime = span.Timestamp.ToTime()
		cfg.SpanID = uint64(span.Id)
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

	refSpan.FieldByName("TraceID").SetUint(uint64(span.Trace))

	if span.HasParent() {
		refSpan.FieldByName("ParentID").SetUint(uint64(span.Parent))
	}

	refContext := reflect.ValueOf(context).Elem()
	setUnexportetFieldValue(refContext.FieldByName("traceID"), uint64(span.Trace))
	setUnexportetFieldValue(refContext.FieldByName("spanID"), uint64(span.Id))

	ddSpan.Finish(tracer.FinishTime(span.Timestamp.ToTime().Add(span.Duration)))

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
func Sink(zipkinSpans <-chan proxy.Span) {
	for span := range zipkinSpans {
		sinkSpan(span)
	}
}
