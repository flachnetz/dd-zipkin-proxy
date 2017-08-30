package main

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/DataDog/dd-trace-go/tracer"
	"github.com/Sirupsen/logrus"
	"github.com/flachnetz/dd-zipkin-proxy"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
)

func main() {
	converter := DefaultSpanConverter{}
	zipkinproxy.Main(converter.Convert)
}

type DefaultSpanConverter struct {
}

func (converter *DefaultSpanConverter) Convert(span *zipkincore.Span) *tracer.Span {
	// ignore long running consul update tasks.
	if span.Name == "watch-config-key-values" || span.Name == "catalog-services-watch" {
		return nil
	}

	name := SimplifyResourceName(span.Name)

	converted := &tracer.Span{
		SpanID:   uint64(span.ID),
		ParentID: uint64(span.GetParentID()),
		TraceID:  uint64(span.TraceID),
		Name:     name,
		Resource: name,
		Start:    1000 * span.GetTimestamp(),
		Duration: 1000 * span.GetDuration(),
		Sampled:  true,
	}

	// datadog traces use zero for a root span
	if converted.ParentID == converted.SpanID {
		converted.ParentID = 0
	}

	// convert binary annotations (like tags)
	if len(span.BinaryAnnotations) > 0 {
		converted.Meta = make(map[string]string, len(span.BinaryAnnotations))
		for _, an := range span.BinaryAnnotations {
			if an.AnnotationType == zipkincore.AnnotationType_STRING {
				key := an.Key

				// rename keys to better match the datadog one.
				switch key {
				case "http.status":
					key = "http.status_code"

				case "client.url":
					key = "http.url"
				}

				converted.Meta[key] = string(an.Value)
			}

			if an.Host != nil && an.Host.ServiceName != "" {
				converted.Service = an.Host.ServiceName
			}
		}

		if url := converted.Meta["http.url"]; url != "" {
			converted.Resource = SimplifyResourceName(url)
			converted.Meta["http.url"] = RemoveQueryString(url)
		}

		if status := converted.Meta["http.status_code"]; status != "" {
			if len(status) > 0 && '3' <= status[0] && status[0] <= '9' {
				if statusValue, err := strconv.Atoi(status); err == nil && statusValue >= 400 {
					converted.Error = int32(statusValue)
				}
			}
		}
	}

	updateInfoFromAnnotations(span, converted)

	if ddService := converted.Meta["dd.service"]; ddService != "" {
		converted.Service = ddService
		delete(converted.Meta, "dd.service")
	}

	if ddName := converted.Meta["dd.name"]; ddName != "" {
		converted.Name = ddName
		delete(converted.Meta, "dd.name")
	}

	if ddResource := converted.Meta["dd.resource"]; ddResource != "" {
		converted.Resource = SimplifyResourceName(ddResource)
		delete(converted.Meta, "dd.resource")
	}

	// if name and service differ than the overview page in datadog will only show the one with
	// most of the time spend. This is why we just rename it to the service here so that we can get a nice
	// overview of all resources belonging to the service. Can be removed in the future
	// when datadog is changing things
	converted.Name = converted.Service

	return converted
}

func updateInfoFromAnnotations(span *zipkincore.Span, converted *tracer.Span) {
	// try to get the service from the cs/cr or sr/ss annotations
	var minTimestamp, maxTimestamp int64
	for _, an := range span.Annotations {
		if an.Value == "sr" && an.Host != nil && an.Host.ServiceName != "" {
			converted.Service = an.Host.ServiceName
		}

		if an.Timestamp < minTimestamp || minTimestamp == 0 {
			minTimestamp = an.Timestamp
		}

		if an.Timestamp > maxTimestamp {
			maxTimestamp = an.Timestamp
		}
	}

	if converted.Start == 0 || converted.Duration == 0 {
		logrus.Warnf("Span had no start/duration, guessing from annotations: %s", identifySpan(span))
		converted.Start = 1000 * minTimestamp
		converted.Duration = 1000 * (maxTimestamp - minTimestamp)
	}
}

// Tries to get some identification for this span. The method tries
// to include the value of the local-component tag and the value of the tags name.
func identifySpan(span *zipkincore.Span) string {
	var name string
	for _, an := range span.BinaryAnnotations {
		if an.Key == "lc" {
			name = string(an.Value) + ":"
		}
	}

	return name + span.Name
}

var reHash = regexp.MustCompile(`\b(?:[a-f0-9]{32}|[a-f0-9]{24}|[a-f0-9-]{8}-[a-f0-9-]{4}-[a-f0-9-]{4}-[a-f0-9-]{4}-[a-f0-9-]{12})\b`)
var reNumber = regexp.MustCompile(`b[0-9]{2,}\b`)

func SimplifyResourceName(value string) string {
	// check if we need to apply the (costly) regexp by checking if a match is possible or not
	digitCount := 0
	hashCharCount := 0
	for idx, char := range value {
		isDigit := char >= '0' && char <= '9'
		if isDigit {
			digitCount++
		}

		if isDigit || char >= 'a' && char <= 'f' {
			hashCharCount++
		}
		if char == '?' {
			value = value[:idx]
			break
		}
	}

	// only search for hash, if we have enough chars for it
	if hashCharCount >= 24 {
		value = reHash.ReplaceAllString(value, "_HASH_")
	}

	// only replace numbers, if we have enough digits for a match
	if digitCount >= 2 {
		value = reNumber.ReplaceAllString(value, "_NUMBER_")
	}

	return value
}

func RemoveQueryString(value string) string {
	idx := strings.IndexByte(value, '?')
	if idx >= 0 {
		return value[:idx]
	}
	return value
}
