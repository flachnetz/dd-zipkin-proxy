package codec

import (
	"fmt"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/json-iterator/go"
	"strconv"
	"unsafe"
)

type Id = proxy.Id

func toStringCached(i interface{}) string {
	switch value := i.(type) {
	case string:
		return cache.String(value)

	case []byte:
		return cache.StringForByteSliceNoCopy(value)

	case fmt.Stringer:
		return cache.String(value.String())

	default:
		return cache.String(fmt.Sprintf("%v", i))
	}
}

var tagJsonV1 = "json v1"
var tagJsonV2 = "json v2"
var tagThriftV1 = "thrift v1"
var tagProtocolVersion = "protocolVersion"

var jsonConfig = jsoniter.Config{
	OnlyTaggedField:               true,
	TagKey:                        "json",
	EscapeHTML:                    false,
	ObjectFieldMustBeSimpleString: true,
}.Froze()

func decodeId(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	bytes := iter.ReadStringAsSlice()
	if len(bytes) > 16 {
		iter.ReportError("decode id", "hex value too large")
		return
	}

	var result Id
	for _, c := range bytes {
		switch {
		case '0' <= c && c <= '9':
			result = (result << 4) | Id(c-'0')

		case 'a' <= c && c <= 'f':
			result = (result << 4) | Id(c-'a') + 10

		case 'A' <= c && c <= 'F':
			result = (result << 4) | Id(c-'A') + 10

		default:
			iter.ReportError("decode id", fmt.Sprintf("hex value must only contain [0-9a-f], got '%c'", c))
			return
		}
	}

	*(*Id)(ptr) = result
}

func init() {
	jsoniter.RegisterTypeDecoderFunc("proxy.Id", decodeId)

	jsoniter.RegisterTypeDecoderFunc("string", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		*(*string)(ptr) = cache.StringForByteSliceCopy(iter.ReadStringAsSlice())
	})

	jsoniter.RegisterFieldDecoderFunc("codec.binaryAnnotationV1", "Value", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		switch iter.WhatIsNext() {
		case jsoniter.StringValue:
			token := iter.ReadStringAsSlice()
			*(*string)(ptr) = cache.StringForByteSliceCopy(token)

		case jsoniter.NumberValue:
			*((*string)(ptr)) = strconv.Itoa(iter.ReadInt())

		case jsoniter.BoolValue:
			if iter.ReadBool() {
				*((*string)(ptr)) = "true"
			} else {
				*((*string)(ptr)) = "false"
			}

		default:
			*((*string)(ptr)) = iter.ReadAny().ToString()
		}
	})
}
