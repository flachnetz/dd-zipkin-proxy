package codec

import (
	"github.com/flachnetz/dd-zipkin-proxy/codec/hyperjson"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/modern-go/reflect2"
	"github.com/pkg/errors"
	"io"
	"strings"
	"sync"
	"time"
	"unsafe"
)

type spanV2 struct {
	TraceID  Id `json:"traceId"`
	ID       Id `json:"id"`
	ParentID Id `json:"parentId"`

	Name string `json:"name"`

	Endpoint endpoint `json:"localEndpoint"`

	Tags map[string]string `json:"tags"`

	Kind string `json:"kind"`

	Timestamp uint64 `json:"timestamp"`
	Duration  uint64 `json:"duration"`
}

var buffers = sync.Pool{
	New: func() interface{} {
		return make([]byte, 8*1024)
	},
}

func ParseJsonV2(input io.Reader) ([]proxy.Span, error) {
	buf := buffers.Get()
	defer buffers.Put(buf)

	r := hyperjson.ForReader(input, buf.([]byte))

	var decoded []spanV2

	if err := decoder(reflect2.NoEscape(unsafe.Pointer(&decoded)), r); err != nil {
		return nil, errors.WithMessage(err, "parse spans for json v2")
	}

	parsedSpans := make([]proxy.Span, len(decoded))
	for idx, span := range decoded {
		parsedSpans[idx] = span.ToSpan()
	}

	return parsedSpans, nil
}

func (span *spanV2) ToSpan() proxy.Span {
	proxySpan := proxy.NewSpan(span.Name, span.TraceID, span.ID, span.ParentID)

	if span.Endpoint.ServiceName != "" {
		proxySpan.Service = span.Endpoint.ServiceName
	}

	proxySpan.Tags = span.Tags
	proxySpan.AddTag(tagProtocolVersion, tagJsonV2)

	proxySpan.Timestamp = proxy.Microseconds(int64(span.Timestamp))
	proxySpan.Duration = time.Duration(span.Duration) * time.Microsecond

	if proxySpan.Duration == 0 {
		proxySpan.Duration = 1 * time.Millisecond
	}

	if strings.EqualFold(span.Kind, "client") {
		proxySpan.Timings.CS = proxySpan.Timestamp
		proxySpan.Timings.CR = proxySpan.Timestamp.Add(proxySpan.Duration)
	}

	if strings.EqualFold(span.Kind, "server") {
		proxySpan.Timings.SR = proxySpan.Timestamp
		proxySpan.Timings.SS = proxySpan.Timestamp.Add(proxySpan.Duration)
	}

	return proxySpan
}

func idValueDecoder(target unsafe.Pointer, p *hyperjson.Parser) error {
	next, err := p.NextType()
	if err != nil {
		return err
	}

	if next == hyperjson.TypeNull {
		*(*Id)(target) = 0
		return p.Skip()
	}

	tok, err := p.ReadString()
	if err != nil {
		return errors.WithMessage(err, "decode id value")
	}

	if len(tok.Value) > 16 {
		return errors.New("hex value too large")
	}

	var result proxy.Id
	for _, c := range tok.Value {
		switch {
		case '0' <= c && c <= '9':
			result = (result << 4) | proxy.Id(c-'0')

		case 'a' <= c && c <= 'f':
			result = (result << 4) | proxy.Id(c-'a') + 10

		case 'A' <= c && c <= 'F':
			result = (result << 4) | proxy.Id(c-'A') + 10

		default:
			return errors.Errorf("hex value must only contain [0-9a-f], got '%c'", c)
		}
	}

	*(*Id)(target) = result

	return nil
}

// Returns a string that shares the data with the given byte slice.
func byteSliceToString(bytes []byte) string {
	if bytes == nil {
		return ""
	}

	return *(*string)(unsafe.Pointer(&bytes))
}

var decoder hyperjson.ValueDecoder = hyperjson.MakeSliceDecoder(
	reflect2.TypeOf([]spanV2{}).(reflect2.SliceType),
	hyperjson.MakeStructDecoder(map[string]hyperjson.Field{
		"id": {
			Offset:  hyperjson.OffsetOf(spanV2{}, "ID"),
			Decoder: idValueDecoder,
		},
		"traceId": {
			Offset:  hyperjson.OffsetOf(spanV2{}, "TraceID"),
			Decoder: idValueDecoder,
		},
		"parentId": {
			Offset:  hyperjson.OffsetOf(spanV2{}, "ParentID"),
			Decoder: idValueDecoder,
		},
		"timestamp": {
			Offset:  hyperjson.OffsetOf(spanV2{}, "Timestamp"),
			Decoder: hyperjson.Uint64ValueDecoder,
		},
		"duration": {
			Offset:  hyperjson.OffsetOf(spanV2{}, "Duration"),
			Decoder: hyperjson.Uint64ValueDecoder,
		},
		"name": {
			Offset:  hyperjson.OffsetOf(spanV2{}, "Name"),
			Decoder: hyperjson.StringValueDecoder,
		},
		"tags": {
			Offset:  hyperjson.OffsetOf(spanV2{}, "Tags"),
			Decoder: hyperjson.MakeMapDecoder(hyperjson.StringValueDecoder, hyperjson.StringValueDecoder),
		},
		"kind": {
			Offset:  hyperjson.OffsetOf(spanV2{}, "Kind"),
			Decoder: hyperjson.StringValueDecoder,
		},
		"localEndpoint": {
			Offset: hyperjson.OffsetOf(spanV2{}, "Endpoint"),
			Decoder: hyperjson.MakeStructDecoder(map[string]hyperjson.Field{
				"serviceName": {
					Decoder: hyperjson.StringValueDecoder,
					Offset:  hyperjson.OffsetOf(endpoint{}, "ServiceName"),
				},
			}),
		},
	}))
