package jsoncodec

import (
	"encoding/hex"
	"errors"
	"net"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
	"encoding/json"
	"fmt"
)

type Span struct {
	TraceID           Id
	Name              string
	ID                Id
	ParentID          *Id

	Annotations       []Annotation
	BinaryAnnotations []BinaryAnnotation

	Debug             bool

	// In milliseconds
	Timestamp         *int64
	Duration          *int64
}

func (span *Span) ToZipkincoreSpan() *zipkincore.Span {
	var parentId int64

	if span.ParentID != nil {
		parentId = int64(*span.ParentID)
	}

	var annotations []*zipkincore.Annotation
	if len(span.Annotations) > 0 {
		annotations = make([]*zipkincore.Annotation, len(span.Annotations))

		for idx, annotation := range span.Annotations {
			annotations[idx] = &zipkincore.Annotation{
				Value: annotation.Value,
				Timestamp: annotation.Timestamp,
				Host: endpointToHost(annotation.Endpoint),
			}
		}
	}

	var binaryAnnotations []*zipkincore.BinaryAnnotation
	if len(span.BinaryAnnotations) > 0 {
		binaryAnnotations = make([]*zipkincore.BinaryAnnotation, len(span.Annotations))

		for idx, annotation := range span.BinaryAnnotations {
			annotationType, _ := zipkincore.AnnotationTypeFromString(string(annotation.Type))

			binaryAnnotations[idx] = &zipkincore.BinaryAnnotation{
				Key: annotation.Key,
				Value: annotation.Value,
				AnnotationType: annotationType,
				Host: endpointToHost(annotation.Endpoint),
			}
		}
	}

	return &zipkincore.Span{
		TraceID: int64(span.TraceID),
		Name: span.Name,
		ID: int64(span.ID),
		ParentID: &parentId,

		Annotations: annotations,
		BinaryAnnotations: binaryAnnotations,

		Debug: span.Debug,

		Timestamp: span.Timestamp,
		Duration: span.Duration,
	}
}

func endpointToHost(endpoint *Endpoint) *zipkincore.Endpoint {
	if endpoint == nil {
		return nil
	}

	result := zipkincore.Endpoint{
		Port: endpoint.Port,
		ServiceName: endpoint.ServiceName,
	}

	if endpoint.Ipv4 != nil {
		bytes := endpoint.Ipv4.To4()
		result.Ipv4 = (int32(bytes[0]) << 24) | (int32(bytes[1]) << 16) | (int32(bytes[2]) << 8) | int32(bytes[3])
	}

	if endpoint.Ipv6 != nil {
		result.Ipv6 = []byte(endpoint.Ipv6.To16())
	}

	return &result
}

type Annotation struct {
	Timestamp int64
	Value     string
	Endpoint  *Endpoint
}

type BinaryAnnotation struct {
	Key      string
	Endpoint *Endpoint
	Type     Type
	Value    json.RawMessage
}

type Endpoint struct {
	ServiceName string
	Ipv4        *net.IP
	Ipv6        *net.IP
	Port        int16
}

type Type string

//const typeBool = "BOOL"
//const typeBytes = "BYTES"
//const typei16 = "I16"
//const typei32 = "I32"
//const typei64 = "I64"
//const typeDouble = "DOUBLE"
//const typeString = "STRING"

type Id int64

func (id *Id) MarshalJSON() ([]byte, error) {
	value := *id

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
	if len(bytes) < 2 || bytes[0] != '"' || bytes[len(bytes) - 1] != '"' {
		return errors.New("Expected hex encoded string.")
	}

	if len(bytes) > 32 {
		return errors.New("Hex value too large.")
	}

	bytes = bytes[1:len(bytes) - 2]

	var result int64
	for idx := len(bytes) - 1; idx >= 0; idx-- {
		c := bytes[idx]
		switch {
		case '0' <= c && c <= '9':
			result = (result << 4) | int64(c - '0')

		case 'a' <= c && c <= 'f':
			result = (result << 4) | int64(c - 'a')

		case 'A' <= c && c <= 'F':
			result = (result << 4) | int64(c - 'A')

		default:
			return fmt.Errorf("Hex value must only contain [0-9a-f], got '%c'.", c)
		}
	}

	*id = Id(result)

	return nil
}
