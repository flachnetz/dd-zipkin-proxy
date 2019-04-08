package datadog

import (
	. "github.com/onsi/gomega"
	"github.com/openzipkin-contrib/zipkin-go-opentracing/thrift/gen-go/zipkincore"
	"reflect"
	"testing"
	"time"
)

func TestSinkSpan(t *testing.T) {
	g := NewGomegaWithT(t)

	Initialize("http://127.0.0.1")

	timestampMicros := int64(1554718206 * time.Millisecond / time.Microsecond)
	durationMicros := int64(1000 * time.Microsecond)

	span := &zipkincore.Span{
		ID:        1,
		ParentID:  toPointer(2),
		TraceID:   3,
		Timestamp: toPointer(timestampMicros),
		Duration:  toPointer(durationMicros),
	}

	ddSpan := sinkSpan(span)

	g.Expect(ddSpan.Context().SpanID()).To(BeEquivalentTo(1))
	g.Expect(ddSpan.Context().TraceID()).To(BeEquivalentTo(3))

	g.Expect(reflect.ValueOf(ddSpan).Elem().FieldByName("SpanID").Uint()).To(BeEquivalentTo(1))
	g.Expect(reflect.ValueOf(ddSpan).Elem().FieldByName("ParentID").Uint()).To(BeEquivalentTo(2))
	g.Expect(reflect.ValueOf(ddSpan).Elem().FieldByName("TraceID").Uint()).To(BeEquivalentTo(3))

}

func toPointer(value int64) *int64 {
	return &value
}
