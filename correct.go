package zipkinproxy

import (
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
	"time"
)

const bufferTime = 10 * time.Second

type tree struct {
	// parent-id to span
	nodes   map[int64][]*zipkincore.Span
	updated time.Time
}

func newTree() *tree {
	return &tree{
		nodes:   make(map[int64][]*zipkincore.Span),
		updated: time.Now(),
	}
}

func (tree *tree) AddSpan(newSpan *zipkincore.Span) {
	parentId := newSpan.GetParentID()
	if spans := tree.nodes[parentId]; spans != nil {
		var spanToUpdate *zipkincore.Span
		// check if we already have a span like this.
		for _, span := range spans {
			if span.ID == newSpan.ID {
				spanToUpdate = span
				break
			}
		}

		if spanToUpdate != nil {
			mergeSpansInPlace(spanToUpdate, newSpan)
		} else {
			// a new span, just add it to the list of spans
			tree.nodes[parentId] = append(spans, newSpan)
		}
	} else {
		// no span with this parent, we can just add it
		tree.nodes[parentId] = []*zipkincore.Span{newSpan}
	}

	tree.updated = time.Now()
}

// gets the root of this tree, or nil, if no root exists.
func (tree *tree) Root() *zipkincore.Span {
	nodes := tree.nodes[0]
	if len(nodes) == 1 {
		return nodes[0]
	} else {
		return nil
	}
}

// gets the children of the given span in this tree.
func (tree *tree) ChildrenOf(span *zipkincore.Span) []*zipkincore.Span {
	return tree.nodes[span.ID]
}

func ErrorCorrectSpans(spans <-chan *zipkincore.Span, output chan<- *zipkincore.Span) {
	traces := make(map[int64]*tree)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case span, ok := <-spans:
			// stream was closed, stop now
			if !ok {
				return
			}

			trace := traces[span.TraceID]
			if trace == nil {
				trace = newTree()
				traces[span.TraceID] = trace
			}

			trace.AddSpan(span)

		case <-ticker.C:
			finishTraces(traces, output)
		}

	}
}

func finishTraces(traces map[int64]*tree, output chan<- *zipkincore.Span) {
	deadline := time.Now().Add(-bufferTime)
	for traceID, trace := range traces {
		if trace.updated.After(deadline) {
			continue
		}

		delete(traces, traceID)

		// if we have a root, try do error correction
		root := trace.Root()
		if root != nil {
			correctTreeTimings(trace, root, 0)
		}

		// send all the spans to the output channel
		for _, spans := range trace.nodes {
			for _, span := range spans {
				output <- span
			}
		}
	}
}

func correctTreeTimings(tree *tree, node *zipkincore.Span, offset int64) {
	if offset != 0 && node.Timestamp != nil {
		*node.Timestamp += offset
	}

	var clientRecv, clientSent, serverRecv, serverSent int64
	for _, an := range node.Annotations {
		switch an.Value {
		case "cs":
			clientSent = an.Timestamp

		case "cr":
			clientRecv = an.Timestamp

		case "sr":
			serverRecv = an.Timestamp

		case "ss":
			serverSent = an.Timestamp
		}
	}

	if clientRecv != 0 && clientSent != 0 && serverRecv != 0 && serverSent != 0 {
		// calculate the offset for children based on the fact, that
		// sr must occur after cs and ss must occur before cr.
		offset += ((clientRecv - clientSent) - (serverSent - serverRecv)) / 2
		node.Timestamp = &clientSent

		log.Infof("Children of span %d in trace %d will get offset %s", node.ID, node.TraceID, time.Duration(offset)*time.Microsecond)

	} else if clientSent != 0 && serverRecv != 0 {
		// we only know the timestamps of server + client, so use those to adjust
		offset += (clientSent - serverRecv)
		node.Timestamp = &clientSent

		log.Infof("Children of span %d in trace %d will get offset %s", node.ID, node.TraceID, time.Duration(offset)*time.Microsecond)
	}

	for _, child := range tree.ChildrenOf(node) {
		correctTreeTimings(tree, child, offset)
	}
}

func mergeSpansInPlace(spanToUpdate *zipkincore.Span, newSpan *zipkincore.Span) {
	// FIXME this is just for our broken data
	if spanToUpdate.Timestamp != nil && newSpan.Timestamp == nil {
		for _, an := range newSpan.Annotations {
			if an.Value == "sr" {
				clientCopy := *an
				clientCopy.Value = "cs"
				clientCopy.Timestamp = *spanToUpdate.Timestamp
				spanToUpdate.Annotations = append(spanToUpdate.Annotations, &clientCopy)
			}
		}
	} else if spanToUpdate.Timestamp == nil && newSpan.Timestamp != nil {
		spanToUpdate.Timestamp = newSpan.Timestamp
		spanToUpdate.Annotations = append(spanToUpdate.Annotations,
			&zipkincore.Annotation{Value: "cs", Timestamp: *newSpan.Timestamp})
	}

	// merge annotations
	if len(newSpan.Annotations) > 0 {
		spanToUpdate.Annotations = append(spanToUpdate.Annotations, newSpan.Annotations...)
	}

	// merge binary annotations
	if len(newSpan.BinaryAnnotations) > 0 {
		spanToUpdate.BinaryAnnotations = append(spanToUpdate.BinaryAnnotations, newSpan.BinaryAnnotations...)
	}
}
