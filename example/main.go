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
var reEmail = regexp.MustCompile("emailAddress=[^%]+%40[^&]+")

func SimplifyResourceName(value string) string {
	// check if we need to apply the regexp by checking if a match is possible or not
	digitCount := 0
	hashCharCount := 0
	mightHaveEmailSign := false
	for _, char := range value {
		isDigit := char >= '0' && char <= '9'
		if isDigit {
			digitCount++
		}

		if isDigit || char >= 'a' && char <= 'f' {
			hashCharCount++
		}

		if char == '%' {
			mightHaveEmailSign = true
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

	if mightHaveEmailSign && digitCount >= 2 {
		value = reEmail.ReplaceAllString(value, "emailAddress=_EMAIL_")
	}

	return value
}

func RetractSensitiveData(value string) string {
	if strings.Contains(value, "emailAddress=") {
		value = reEmail.ReplaceAllString(value, "emailAddress=_EMAIL_")
	}

	return value
}

type DefaultSpanConverter struct {
	current  map[uint64]string
	previous map[uint64]string
}

func (converter *DefaultSpanConverter) Convert(span *zipkincore.Span) *tracer.Span {
	if span.Name == "watch-config-key-values" {
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
			converted.Meta["http.url"] = RetractSensitiveData(url)
		}

		if status := converted.Meta["http.status_code"]; status != "" {
			if len(status) > 0 && '3' <= status[0] && status[0] <= '9' {
				if statusValue, err := strconv.Atoi(status); err == nil && statusValue >= 400 {
					converted.Error = int32(statusValue)
				}
			}
		}
	}

	extractInfoFromAnnotations(span, converted)

	// simplify some names
	if strings.HasPrefix(converted.Name, "http:") {
		converted.Service = converted.Name[5:]

	} else if converted.Name == "transaction" {
		converted.Service = "oracle"

	} else if sql := converted.Meta["sql"]; sql != "" {
		delete(converted.Meta, "sql")
		converted.Resource = sql
		converted.Service = "sql"

	} else if strings.HasPrefix(converted.Name, "redis:") {
		converted.Service = "redis"

		if key := converted.Meta["redis.key"]; key != "" {
			converted.Resource = SimplifyResourceName(key)

			// the hash is not really important later on. lets not spam datadog with it.
			delete(converted.Meta, "redis.key")
		}

	} else if converted.Service == "core-services" {
		lc := converted.Meta["lc"]

		switch {
		case strings.Contains(converted.Meta["http.url"], ":6080/"):
			converted.Service = "iwg-restrictor"
			converted.Name = iwgNameFromResource(converted.Resource, converted.Name)
			converted.Resource = dropDomainFromUrl(converted.Resource)

		case strings.Contains(converted.Meta["http.url"], ":2080/"):
			converted.Service = "instant-win-game"
			converted.Name = "iwg-game"
			converted.Resource = dropDomainFromUrl(converted.Resource)

		case lc != "" && lc != "servlet" && lc != "HttpClient":
			delete(converted.Meta, "lc")
			converted.Service = lc
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

	if lc := converted.Meta["lc"]; lc != "" {
		delete(converted.Meta, "lc")

		if converted.Name == "" {
			converted.Name = lc
		}

		if lc == "consul" {
			converted.Service = "consul"
		}
	}

	if converted.Name == "transaction" || converted.Service == "redis" {
		converted.Type = "db"
	} else {
		converted.Type = "http"
	}

	if converted.Name == "hystrix" {
		converted.Resource = converted.Meta["thread"]
	} else if converted.Name == "" {
		logrus.Warnf("Could not get a name for this span: %+v converted: %+v", span, converted)
	}

	// if name and service differ than the overview page in datadog will only show the one with
	// most of the time spend. This is why we just rename it to the service here so that we can get a nice
	// overview of all resources belonging to the service. Can be removed in the future when datadog is changing things
	converted.Name = converted.Service

	// initialize history maps for span -> parent assignment
	const parentLookupMapSize = 40000
	if len(converter.current) >= parentLookupMapSize || converter.current == nil {
		converter.previous = converter.current
		converter.current = make(map[uint64]string, parentLookupMapSize)
	}

	// remember the service for a short while
	converter.current[converted.SpanID] = converted.Service

	return converted
}

func extractInfoFromAnnotations(span *zipkincore.Span, converted *tracer.Span) {
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

	if converted.Start == 0 {
		logrus.Warn("Span had no start/duration, guessing from annotations.")
		converted.Start = 1000 * minTimestamp
		converted.Duration = 1000 * (maxTimestamp - minTimestamp)
	}
}

func dropDomainFromUrl(url string) string {
	switch {
	case strings.HasPrefix(url, "http://"):
		index := strings.IndexRune(url[7:], '/')
		return url[7+index:]

	case strings.HasPrefix(url, "https://"):
		index := strings.IndexRune(url[8:], '/')
		return url[8+index:]
	}

	return url
}

func iwgNameFromResource(resource string, fallback string) string {
	splittedUrl := strings.Split(strings.Trim(resource, "/"), "/")
	if len(splittedUrl) == 1 && splittedUrl[0] != "" {
		return splittedUrl[0]
	} else if len(splittedUrl) > 1 {
		return splittedUrl[len(splittedUrl)-2] + "/" + splittedUrl[len(splittedUrl)-1]
	}
	return fallback
}
