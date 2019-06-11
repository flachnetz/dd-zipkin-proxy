package proxy

import (
	"time"
)

type Span struct {
	Id     Id `json:"id"`
	Parent Id `json:"parent"`
	Trace  Id `json:"trace"`

	Name    string `json:"name"`
	Service string `json:"service"`

	Timestamp Timestamp     `json:"timestamp"`
	Duration  time.Duration `json:"duration"`

	Tags map[string]string `json:"tags,omitempty"`

	Timings Timings `json:"timings"`
}

type Timings struct {
	CS Timestamp `json:"cs,omitempty"`
	CR Timestamp `json:"cr,omitempty"`
	SS Timestamp `json:"ss,omitempty"`
	SR Timestamp `json:"sr,omitempty"`
}

func NewSpan(name string, trace, id, parent Id) Span {
	return Span{
		Id:     id,
		Parent: parent,
		Trace:  trace,
		Name:   name,
	}
}

func NewRootSpan(name string, trace, id Id) Span {
	return NewSpan(name, trace, id, trace)
}

func (span *Span) HasParent() bool {
	return !span.IsRoot()
}

func (span *Span) IsRoot() bool {
	return span.Parent == span.Id
}

func (span *Span) AddTag(key, value string) {
	if span.Tags == nil {
		span.Tags = map[string]string{}
	}

	span.Tags[key] = value
}

func (span *Span) AddTiming(key string, ns Timestamp) {
	switch key {
	case "cs":
		span.Timings.CS = ns
	case "cr":
		span.Timings.CR = ns
	case "sr":
		span.Timings.SR = ns
	case "ss":
		span.Timings.SS = ns
	}
}
