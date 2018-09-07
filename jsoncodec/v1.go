package jsoncodec

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/openzipkin/zipkin-go-opentracing/thrift/gen-go/zipkincore"
	"net"
)

type SpanV1 struct {
	TraceID  Id  `json:"traceId"`
	ID       Id  `json:"id"`
	ParentID *Id `json:"parentId"`

	Annotations       []AnnotationV1       `json:"annotations"`
	BinaryAnnotations []BinaryAnnotationV1 `json:"binaryAnnotations"`

	Name string `json:"name"`

	Debug bool `json:"debug"`

	Timestamp *int64 `json:"timestamp"`
	Duration  *int64 `json:"duration"`
}

type AnnotationV1 struct {
	Timestamp int64     `json:"timestamp"`
	Value     string    `json:"value"`
	Endpoint  *Endpoint `json:"endpoint"`
}

type BinaryAnnotationV1 struct {
	Key      string      `json:"key"`
	Value    interface{} `json:"value"`
	Endpoint *Endpoint   `json:"endpoint"`
}

type Endpoint struct {
	ServiceName string `json:"serviceName"`
	Ipv4        net.IP `json:"ipv4,omitempty"`
	Ipv6        net.IP `json:"ipv6,omitempty"`
	Port        uint16 `json:"port"`
}

type ZipkincoreSpaner interface {
	ToZipkincoreSpan() *zipkincore.Span
}

type Id int64

var _ json.Marshaler = new(Id)
var _ json.Unmarshaler = new(Id)

func FromSpan(span *zipkincore.Span) SpanV1 {
	var annotations []AnnotationV1
	for _, an := range span.Annotations {
		annotations = append(annotations, AnnotationV1{
			Timestamp: an.Timestamp,
			Value:     an.Value,
			Endpoint:  endpointToJson(an.Host),
		})
	}

	var binaryAnnotations []BinaryAnnotationV1
	for _, an := range span.BinaryAnnotations {
		binaryAnnotations = append(binaryAnnotations, BinaryAnnotationV1{
			Key:      an.Key,
			Value:    string(an.Value),
			Endpoint: endpointToJson(an.Host),
		})
	}

	return SpanV1{
		TraceID:  Id(span.TraceID),
		ID:       Id(span.ID),
		ParentID: (*Id)(span.ParentID),

		Name: span.Name,

		Annotations:       annotations,
		BinaryAnnotations: binaryAnnotations,

		Timestamp: span.Timestamp,
		Duration:  span.Duration,
		Debug:     span.Debug,
	}
}

func (span *SpanV1) ToZipkincoreSpan() *zipkincore.Span {
	var annotations []*zipkincore.Annotation
	if len(span.Annotations) > 0 {
		annotations = make([]*zipkincore.Annotation, len(span.Annotations))

		for idx, annotation := range span.Annotations {
			annotations[idx] = &zipkincore.Annotation{
				Value:     cache.String(annotation.Value),
				Timestamp: annotation.Timestamp,
				Host:      endpointToZipkin(annotation.Endpoint),
			}
		}
	}

	var binaryAnnotations []*zipkincore.BinaryAnnotation
	if len(span.BinaryAnnotations) > 0 {
		binaryAnnotations = make([]*zipkincore.BinaryAnnotation, len(span.BinaryAnnotations))

		for idx, annotation := range span.BinaryAnnotations {
			binaryAnnotations[idx] = &zipkincore.BinaryAnnotation{
				Key:            cache.String(annotation.Key),
				Value:          toBytesCached(annotation.Value),
				Host:           endpointToZipkin(annotation.Endpoint),
				AnnotationType: zipkincore.AnnotationType_STRING,
			}
		}
	}

	// in root spans the traceId equals the span id.
	parentId := span.ParentID
	if span.TraceID == span.ID {
		parentId = nil
	}

	return &zipkincore.Span{
		TraceID: int64(span.TraceID),
		ID:      int64(span.ID),
		Name:    cache.String(span.Name),

		ParentID: (*int64)(parentId),

		Annotations:       annotations,
		BinaryAnnotations: binaryAnnotations,

		Debug: span.Debug,

		Timestamp: span.Timestamp,
		Duration:  span.Duration,
	}
}

func toBytesCached(i interface{}) []byte {
	if str, ok := i.(string); ok {
		return cache.ByteSlice([]byte(str))
	} else {
		return cache.ByteSlice([]byte(fmt.Sprintf("%v", i)))
	}
}

func endpointToJson(endpoint *zipkincore.Endpoint) *Endpoint {
	if endpoint == nil {
		return nil
	}

	result := &Endpoint{
		Port:        uint16(endpoint.Port),
		ServiceName: endpoint.ServiceName,
	}

	if endpoint.Ipv4 != 0 {
		var bytes [4]byte
		binary.BigEndian.PutUint32(bytes[:], uint32(endpoint.Ipv4))
		result.Ipv4 = net.IP(bytes[:])
	}

	if endpoint.Ipv6 != nil {
		result.Ipv6 = net.IP(endpoint.Ipv6)
	}

	return result
}

func endpointToZipkin(endpoint *Endpoint) *zipkincore.Endpoint {
	if endpoint == nil {
		return nil
	}

	result := zipkincore.Endpoint{
		Port:        int16(endpoint.Port),
		ServiceName: cache.String(endpoint.ServiceName),
	}

	if endpoint.Ipv4 != nil {
		bytes := endpoint.Ipv4.To4()
		result.Ipv4 = (int32(bytes[0]) << 24) | (int32(bytes[1]) << 16) | (int32(bytes[2]) << 8) | int32(bytes[3])
	}

	if endpoint.Ipv6 != nil {
		result.Ipv6 = cache.ByteSlice([]byte(endpoint.Ipv6.To16()))
	}

	return &result
}

func (id *Id) MarshalJSON() ([]byte, error) {
	value := int64(*id)

	bytes := [8]byte{
		byte((value >> 56) & 0xff),
		byte((value >> 48) & 0xff),
		byte((value >> 40) & 0xff),
		byte((value >> 32) & 0xff),
		byte((value >> 24) & 0xff),
		byte((value >> 16) & 0xff),
		byte((value >> 8) & 0xff),
		byte(value & 0xff),
	}

	var encoded [18]byte
	hex.Encode(encoded[1:], bytes[:])

	// the result is a json string
	encoded[0] = '"'
	encoded[17] = '"'

	return encoded[:], nil
}

func (id *Id) UnmarshalJSON(bytes []byte) error {
	if len(bytes) < 2 || bytes[0] != '"' || bytes[len(bytes)-1] != '"' {
		return errors.New("Expected hex encoded string.")
	}

	if len(bytes) > 34 {
		return errors.New("Hex value too large.")
	}

	var result int64
	for idx := 1; idx < len(bytes)-1; idx++ {
		c := bytes[idx]
		switch {
		case '0' <= c && c <= '9':
			result = (result << 4) | int64(c-'0')

		case 'a' <= c && c <= 'f':
			result = (result << 4) | int64(c-'a') + 10

		case 'A' <= c && c <= 'F':
			result = (result << 4) | int64(c-'A') + 10

		default:
			return fmt.Errorf("Hex value must only contain [0-9a-f], got '%c'.", c)
		}
	}

	*id = Id(result)

	return nil
}
