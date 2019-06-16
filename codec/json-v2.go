package codec

import (
	"github.com/flachnetz/dd-zipkin-proxy/codec/hyperjson"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/modern-go/reflect2"
	"github.com/pkg/errors"
	"io"
	"strings"
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

func ParseJsonV2(input io.Reader) ([]proxy.Span, error) {
	// get a buffer to re-use
	buf := bufferPool.Get()
	defer bufferPool.Put(buf)

	// get a new reader and initialize it with the pooled buffer.
	r := hyperjson.NewWithReader(input, buf.([]byte))

	// we know, that the reader wont escape, sadly, go doesnt know that,
	// so we give it a little hint
	p := (*hyperjson.Parser)(reflect2.NoEscape(unsafe.Pointer(r)))

	// decode spans into the span slice.
	var decoded []spanV2
	if err := decoderSliceSpanV2(reflect2.NoEscape(unsafe.Pointer(&decoded)), p); err != nil {
		return nil, errors.WithMessage(err, "parse spans for json v2")
	}

	// now convert them to span objects
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

func spanV2ValueDecoder() hyperjson.ValueDecoder {
	decoder := hyperjson.MakeStructDecoder([]hyperjson.Field{
		{
			JsonName: "id",
			Offset:   hyperjson.OffsetOf(spanV2{}, "ID"),
			Decoder:  idValueDecoder,
		},
		{
			JsonName: "traceId",
			Offset:   hyperjson.OffsetOf(spanV2{}, "TraceID"),
			Decoder:  idValueDecoder,
		},
		{
			JsonName: "parentId",
			Offset:   hyperjson.OffsetOf(spanV2{}, "ParentID"),
			Decoder:  idValueDecoder,
		},
		{
			JsonName: "timestamp",
			Offset:   hyperjson.OffsetOf(spanV2{}, "Timestamp"),
			Decoder:  hyperjson.Uint64ValueDecoder,
		},
		{
			JsonName: "duration",
			Offset:   hyperjson.OffsetOf(spanV2{}, "Duration"),
			Decoder:  hyperjson.Uint64ValueDecoder,
		},
		{
			JsonName: "name",
			Offset:   hyperjson.OffsetOf(spanV2{}, "Name"),
			Decoder:  hyperjson.StringValueDecoder,
		},
		{
			JsonName: "tags",
			Offset:   hyperjson.OffsetOf(spanV2{}, "Tags"),
			Decoder:  hyperjson.MakeMapDecoder(hyperjson.StringValueDecoder, hyperjson.StringValueDecoder),
		},
		{
			JsonName: "kind",
			Offset:   hyperjson.OffsetOf(spanV2{}, "Kind"),
			Decoder:  hyperjson.StringValueDecoder,
		},
		{
			JsonName: "localEndpoint",
			Offset:   hyperjson.OffsetOf(spanV2{}, "Endpoint"),
			Decoder:  endpointValueDecoder,
		},
	})

	return func(target unsafe.Pointer, p *hyperjson.Parser) error {
		// we might be reusing a span, so clear it before decoding into it
		*(*spanV2)(target) = spanV2{}
		return decoder(target, p)
	}
}

var decoderSliceSpanV2 = hyperjson.MakeSliceDecoder(
	reflect2.TypeOf([]spanV2{}).(reflect2.SliceType),
	spanV2ValueDecoder())
