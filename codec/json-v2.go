package codec

import (
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/codec/hyperjson"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
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
	decoded, err := hyParseSpanSlice(r)

	if err != nil {
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

func hyParseSpanSlice(p *hyperjson.Parser) ([]spanV2, error) {
	var result []spanV2

	if err := p.ConsumeArrayBegin(); err != nil {
		return nil, err
	}

	for {
		next, err := p.NextType()
		if err != nil {
			return nil, err
		}

		// stop at the end of the array
		if next == hyperjson.TypeArrayEnd {
			return result, p.ConsumeArrayEnd()
		}

		// parse the span
		span, err := hyParseSpan(p)
		if err != nil {
			return nil, err
		}

		result = append(result, span)
	}
}

func hyParseSpan(p *hyperjson.Parser) (spanV2, error) {
	var result spanV2

	if err := p.ConsumeObjectBegin(); err != nil {
		return result, err
	}

	for {
		next, err := p.NextType()
		if err != nil {
			return result, err
		}

		if next == hyperjson.TypeObjectEnd {
			return result, p.ConsumeObjectEnd()
		}

		// read the key
		keyToken, err := p.ReadString()
		if err != nil {
			return result, err
		}

		var tok hyperjson.Token

		switch byteSliceToString(keyToken.Value) {
		case "id":
			result.ID, err = hyParseId(p)

		case "traceId":
			result.TraceID, err = hyParseId(p)

		case "parentId":
			result.ParentID, err = hyParseId(p)

		case "name":
			tok, err = p.ReadString()
			result.Name = cache.StringForByteSliceCopy(tok.Value)

		case "kind":
			tok, err = p.ReadString()
			result.Kind = cache.StringForByteSliceCopy(tok.Value)

		case "timestamp":
			result.Timestamp, err = hyParseUint64(p)

		case "duration":
			result.Duration, err = hyParseUint64(p)

		case "localEndpoint":
			result.Endpoint, err = hyParseEndpoint(p)

		case "tags":
			result.Tags, err = hyParseMap(p)

		default:
			err = p.Skip()
		}

		if err != nil {
			return result, err
		}
	}
}

func hyParseEndpoint(p *hyperjson.Parser) (endpoint, error) {
	var result endpoint

	next, err := p.NextType()
	if err != nil {
		return result, err
	}

	if next == hyperjson.TypeNull {
		return result, p.Skip()
	}

	if err := p.ConsumeObjectBegin(); err != nil {
		return result, err
	}

	for {
		next, err := p.NextType()
		if err != nil {
			return result, err
		}

		if next == hyperjson.TypeObjectEnd {
			return result, p.ConsumeObjectEnd()
		}

		// read the key
		keyToken, err := p.ReadString()
		if err != nil {
			return result, err
		}

		if byteSliceToString(keyToken.Value) == "serviceName" {
			// read the value
			valueToken, err := p.Read()
			if err != nil {
				return result, err
			}

			result.ServiceName = cache.StringForByteSliceCopy(valueToken.Value)
		} else {
			if err := p.Skip(); err != nil {
				return result, err
			}
		}
	}
}

func hyParseMap(p *hyperjson.Parser) (map[string]string, error) {
	if err := p.ConsumeObjectBegin(); err != nil {
		return nil, err
	}

	var result map[string]string
	for {
		next, err := p.NextType()
		if err != nil {
			return nil, err
		}

		if next == hyperjson.TypeObjectEnd {
			return result, p.ConsumeObjectEnd()
		}

		if result == nil {
			result = make(map[string]string)
		}

		keyToken, err := p.ReadString()
		if err != nil {
			return nil, err
		}

		key := cache.StringForByteSliceCopy(keyToken.Value)

		valueToken, err := p.Read()
		if err != nil {
			return nil, err
		}

		value := cache.StringForByteSliceCopy(valueToken.Value)
		result[key] = value
	}
}

func hyParseUint64(p *hyperjson.Parser) (uint64, error) {
	tok, err := p.ReadNumber()
	if err != nil {
		return 0, errors.WithMessage(err, "decode timestamp value")
	}

	var result uint64
	for _, ch := range tok.Value {
		if ch < '0' || ch > '9' {
			return 0, errors.Errorf("Unexpected character in number: '%c'", ch)
		}

		result = result*10 + uint64(ch-'0')
	}

	return result, nil
}

func hyParseId(p *hyperjson.Parser) (proxy.Id, error) {
	next, err := p.NextType()
	if err != nil {
		return 0, err
	}

	if next == hyperjson.TypeNull {
		return 0, p.Skip()
	}

	tok, err := p.ReadString()
	if err != nil {
		return 0, errors.WithMessage(err, "decode id value")
	}

	if len(tok.Value) > 16 {
		return 0, errors.New("hex value too large")
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
			return 0, errors.Errorf("hex value must only contain [0-9a-f], got '%c'", c)
		}
	}

	return result, nil
}

// Returns a string that shares the data with the given byte slice.
func byteSliceToString(bytes []byte) string {
	if bytes == nil {
		return ""
	}

	return *(*string)(unsafe.Pointer(&bytes))
}
