package codec

import (
	"fmt"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
	"strconv"
	"unsafe"
)

var jsonConfig = jsoniter.Config{
	OnlyTaggedField:               true,
	TagKey:                        "json",
	EscapeHTML:                    false,
	ObjectFieldMustBeSimpleString: true,
}.Froze()

func init() {
	iterType := reflect2.TypeOf(jsoniter.Iterator{}).(reflect2.StructType)
	fBuf := iterType.FieldByName("buf")
	fHead := iterType.FieldByName("head")
	fTail := iterType.FieldByName("tail")

	readStringOrSlice := func(iter *jsoniter.Iterator) (string, []byte) {
		if iter.WhatIsNext() == jsoniter.StringValue {
			buf := *(*[]byte)(fBuf.UnsafeGet(unsafe.Pointer(iter)))
			head := (*int)(fHead.UnsafeGet(unsafe.Pointer(iter)))
			tail := (*int)(fTail.UnsafeGet(unsafe.Pointer(iter)))

			for i := *head + 1; i < *tail; i++ {
				// require ascii string and no escape
				// for: field name, base64, number
				if buf[i] == '"' {
					// fast path: reuse the underlying buffer
					return "", iter.ReadStringAsSlice()
				}

				if buf[i] == '\\' {
					break
				}
			}
		}

		return iter.ReadString(), nil
	}

	jsoniter.RegisterTypeDecoderFunc("string", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		strValue, sliceValue := readStringOrSlice(iter)
		if sliceValue != nil {
			*(*string)(ptr) = cache.StringForByteSliceCopy(sliceValue)
		} else {
			*(*string)(ptr) = strValue
		}
	})

	jsoniter.RegisterTypeDecoderFunc("proxy.Id", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
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
	})

	jsoniter.RegisterFieldDecoderFunc("codec.binaryAnnotationV1", "Value", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		switch iter.WhatIsNext() {
		case jsoniter.StringValue:
			*(*string)(ptr) = iter.ReadString()

		case jsoniter.NumberValue:
			*((*string)(ptr)) = strconv.Itoa(iter.ReadInt())

		case jsoniter.BoolValue:
			if iter.ReadBool() {
				*((*string)(ptr)) = "true"
			} else {
				*((*string)(ptr)) = "false"
			}

		default:
			// should not happen
			*((*string)(ptr)) = cache.StringForByteSliceNoCopy(iter.SkipAndReturnBytes())
		}
	})
}
