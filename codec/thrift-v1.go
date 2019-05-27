package codec

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/openzipkin-contrib/zipkin-go-opentracing/thrift/gen-go/zipkincore"
	"github.com/pkg/errors"
	"io"
	"time"
)

func ParseThriftV1(body io.Reader) ([]proxy.Span, error) {
	pb := thrift.NewTBinaryProtocolTransport(thrift.NewStreamTransportR(body))
	protocol := cache.NewProtocol(pb)

	_, size, err := protocol.ReadListBegin()
	if err != nil {
		return nil, errors.WithMessage(err, "begin of list")
	}

	if size <= 0 || size > 32*1024 {
		return nil, errors.Errorf("too many spans, will not read %d spans", size)
	}

	spans := make([]proxy.Span, size)
	for idx := 0; idx < size; idx++ {
		var span zipkincore.Span
		if err := span.Read(protocol); err != nil {
			return nil, errors.WithMessage(err, "read thrift v1 encoded span")
		}

		spans = append(spans, convertThriftSpan(span))
	}

	return spans, errors.WithMessage(protocol.ReadListEnd(), "Could not read end of list")
}

func convertThriftSpan(span zipkincore.Span) proxy.Span {
	var parentId Id
	if span.ParentID != nil {
		parentId = Id(*span.ParentID)
	}

	proxySpan := proxy.NewSpan(span.Name, Id(span.TraceID), Id(span.ID), parentId)

	if span.Timestamp != nil {
		proxySpan.Timestamp = proxy.Microseconds(*span.Timestamp)
	}

	if span.Duration != nil {
		proxySpan.Duration = time.Duration(*span.Duration) * time.Microsecond
	}

	for _, annotation := range span.Annotations {
		proxySpan.AddTiming(annotation.Value, proxy.Microseconds(annotation.Timestamp))

		if annotation.Host != nil && proxySpan.Service == "" {
			proxySpan.Service = annotation.Host.ServiceName
		}
	}

	for _, annotation := range span.BinaryAnnotations {
		proxySpan.AddTag(annotation.Key, toStringCached(annotation.Value))

		if annotation.Host != nil && proxySpan.Service == "" {
			proxySpan.Service = annotation.Host.ServiceName
		}
	}

	proxySpan.AddTag(tagProtocolVersion, tagThriftV1)

	fillInTimestamp(&proxySpan)

	return proxySpan
}
