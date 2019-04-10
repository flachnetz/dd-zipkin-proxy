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
		return cache.String(string(value))

	case fmt.Stringer:
		return cache.String(value.String())

	default:
		return cache.String(fmt.Sprintf("%v", i))
	}
}
