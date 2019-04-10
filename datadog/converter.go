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

	// name of the service, will be displayed in datadog on the overview page
	tags[ext.ServiceName] = span.Service

	// the resource of the tag will be shown "by tag"
	tags[ext.ResourceName] = span.Name

	// the name of the span as shown in flame graphs
	tags[ext.SpanName] = span.Name

	for key, value := range span.Tags {
		tags[key] = value

		switch key {
		case "dd.name":
			tags[ext.SpanName] = value

		case "dd.service":
			tags[ext.ServiceName] = value

		case "dd.resource":
			tags[ext.ResourceName] = value
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
		cfg.SpanID = span.Id.Uint64()
		cfg.Tags = tags
	})

	refSpan := assertIsRealSpan(ddSpan)
	setValue(refSpan.FieldByName("TraceID"), span.Trace.Uint64())
	setValue(refSpan.FieldByName("ParentID"), span.Parent.Uint64())

	refContext := reflect.ValueOf(ddSpan.Context()).Elem()
	setValue(refContext.FieldByName("traceID"), span.Trace.Uint64())
	setValue(refContext.FieldByName("spanID"), span.Id.Uint64())

	ddSpan.Finish(tracer.FinishTime(span.Timestamp.ToTime().Add(span.Duration)))

	return ddSpan
}

func assertIsRealSpan(ddSpan ddtrace.Span) reflect.Value {
	refSpan := reflect.ValueOf(ddSpan)

	// validate type
	refSpanType := refSpan.Type()
	if refSpanType.Kind() != reflect.Ptr || refSpanType.String() != "*tracer.span" {
		panic(errors.Errorf("Expected Span of type *tracer.span, got '%s'", refSpanType.String()))
	}

	// dereference the pointer value
	return refSpan.Elem()
}

func setValue(target reflect.Value, value uint64) {
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
