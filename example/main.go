package main

import (
	"github.com/DataDog/dd-trace-go/tracer"
	"github.com/Sirupsen/logrus"
	"github.com/flachnetz/dd-zipkin-proxy"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
	"regexp"
	"strconv"
	"strings"
)

func main() {
	converter := DefaultSpanConverter{}
	zipkinproxy.Main(converter.Convert)
}

var reHash = regexp.MustCompile("\\b(?:[a-f0-9]{32}|[a-f0-9-]{8}-[a-f0-9-]{4}-[a-f0-9-]{4}-[a-f0-9-]{4}-[a-f0-9-]{12})\\b")
var reNumber = regexp.MustCompile("\\b[0-9]{2,}\\b")
var reIwgHash = regexp.MustCompile("iwg\\.[A-Za-z0-9]{12}\\b")

func SimplifyResourceName(value string) string {
	// check if we need to apply the regexp by checking if a match is possible or not
	digitCount := 0
	hashCharCount := 0
	for _, char := range value {
		isDigit := char >= '0' && char <= '9'
		if isDigit {
			digitCount++
		}

		if isDigit || char >= 'a' && char <= 'f' {
			hashCharCount++
		}
	}

	// only search for hash, if we have enough chars for it
	if hashCharCount >= 32 {
		value = reHash.ReplaceAllString(value, "_HASH_")
	}

	// only replace numbers, if we have enough digits for a match
	if digitCount >= 2 {
		value = reNumber.ReplaceAllString(value, "_NUMBER_")
	}

	if strings.HasPrefix(value, "iwg.") {
		value = reIwgHash.ReplaceAllString(value, "iwg._HASH_")
	}

	return value
}

type DefaultSpanConverter struct {
	current  map[uint64]string
	previous map[uint64]string
}

func (converter *DefaultSpanConverter) Convert(span *zipkincore.Span) *tracer.Span {
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

	// datadog traces use a trace of 0
	if converted.ParentID == converted.SpanID {
		converted.ParentID = 0
	}

	// split "http:/some/url" or "get:/some/url"
	for _, prefix := range []string{"http:/", "get:/", "post:/"} {
		if strings.HasPrefix(converted.Name, prefix) {
			converted.Resource = converted.Name[len(prefix)-1:]
			converted.Name = prefix[:len(prefix)-2]
			break
		}
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
		}

		if status := converted.Meta["http.status_code"]; status != "" {
			if len(status) > 0 && '3' <= status[0] && status[0] <= '9' {
				if statusValue, err := strconv.Atoi(status); err == nil && statusValue >= 400 {
					converted.Error = int32(statusValue)
				}
			}
		}
	}

	// try to get the service from the cs/cr or sr/ss annotations
	var minTimestamp, maxTimestamp int64
	for _, an := range span.Annotations {
		if an.Host != nil && an.Host.ServiceName != "" {
			converted.Service = an.Host.ServiceName
		}

		if an.Timestamp < minTimestamp || minTimestamp == 0 {
			minTimestamp = an.Timestamp
		}

		if an.Timestamp > maxTimestamp {
			maxTimestamp = an.Timestamp
		}
	}

	if converted.Start == 0 {
		converted.Start = 1000 * minTimestamp
		converted.Duration = 1000 * (maxTimestamp - minTimestamp)
	}

	// simplify some names
	if strings.HasPrefix(converted.Name, "http:") {
		converted.Service = converted.Name[5:]
	}

	if converted.Name == "transaction" {
		converted.Service = "oracle"
	}

	if strings.HasPrefix(converted.Name, "redis:") {
		converted.Service = "redis"

		if key := converted.Meta["redis.key"]; key != "" {
			converted.Resource = SimplifyResourceName(key)

			// the hash is not really important later on. lets not spam datadog with it.
			delete(converted.Meta, "redis.key")
		}
	}

	if converted.Service == "core-services" {

		if strings.Contains(converted.Meta["http.url"], ":6080/") {
			converted.Service = "iwg-restrictor"
			converted.Name = getNameFromResource(converted.Resource, converted.Name)
		}

		if strings.Contains(converted.Meta["http.url"], ":2080/") {
			converted.Service = "iwg-game"
			converted.Name = "iwg"
		}

	}

	// If we could not get a service, we'll try to get it from the parent span.
	// Try first in the current map, then in the previous one.
	if converted.Service == "" {
		parentService := converter.current[converted.ParentID]
		if parentService != "" {
			converted.Service = parentService
		} else {
			parentService = converter.previous[converted.ParentID]
			if parentService != "" {
				converted.Service = parentService
			}
		}

		// if we did not get a service, use a fallback
		if converted.Service == "" {
			logrus.Warnf("Could not get a service for this span: %+v", span)
			converted.Service = "unknown"
		}
	}

	if converted.Name == "" && converted.Meta["lc"] != "" {
		converted.Name = converted.Meta["lc"]
	}

	// guess a type for the datadog ui.
	if converted.Name == "transaction" || converted.Service == "redis" {
		converted.Type = "db"
	} else {
		converted.Type = "http"
	}

	if converted.Name == "hystrix" {
		converted.Resource = converted.Meta["thread"]
	}

	if converted.Name == "" {
		logrus.Warnf("Could not get a name for this span: %+v converted: %+v", span, converted)
	}

	// initialize history maps for span -> parent assignment
	if len(converter.current) >= 40000 || converter.current == nil {
		converter.previous = converter.current
		converter.current = make(map[uint64]string, 40000)
	}

	// remember the service for a short while
	converter.current[converted.SpanID] = converted.Service

	return converted
}

func getNameFromResource(resource string, fallback string) string {
	splittedUrl := strings.Split(strings.Trim(resource, "/"), "/")
	if len(splittedUrl) == 1 && splittedUrl[0] != "" {
		return splittedUrl[0]
	} else if len(splittedUrl) > 1 {
		return splittedUrl[len(splittedUrl)-2] + "/" + splittedUrl[len(splittedUrl)-1]
	}
	return fallback
}
