package codec

import (
	"fmt"
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
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

var tagCS = "cs"
var tagCR = "cr"
var tagSR = "sr"
var tagSS = "ss"
