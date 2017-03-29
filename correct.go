package zipkinproxy

import (
	"github.com/Sirupsen/logrus"
	"github.com/openzipkin/zipkin-go-opentracing/_thrift/gen-go/zipkincore"
	"sort"
	"time"
)

const bufferTime = 10 * time.Second

type tree struct {
	// parent-id to span
	nodes     map[int64][]*zipkincore.Span
	updated   time.Time
	nodeCount uint16
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
		idx := sort.Search(len(spans), func(i int) bool {
			return newSpan.ID >= spans[i].ID
		})

		var spanToUpdate *zipkincore.Span
		if idx < len(spans) && spans[idx].ID == newSpan.ID {
			spanToUpdate = spans[idx]
		}

		if spanToUpdate != nil {
			mergeSpansInPlace(spanToUpdate, newSpan)
		} else {
			// a new span, just add it to the list of spans
			tree.nodes[parentId] = insertSpan(spans, idx, newSpan)
			tree.nodeCount++
		}
	} else {
		// no span with this parent, we can just add it
		tree.nodes[parentId] = []*zipkincore.Span{newSpan}
		tree.nodeCount++
	}

	tree.updated = time.Now()
}

func (tree *tree) GetSpan(parentId, spanId int64) *zipkincore.Span {
	spans := tree.nodes[parentId]
	idx := sort.Search(len(spans), func(i int) bool {
		return spanId >= spans[i].ID
	})

	if idx < len(spans) && spans[idx].ID == spanId {
		return spans[idx]
	}

	return nil
}

func insertSpan(spans []*zipkincore.Span, idx int, span *zipkincore.Span) []*zipkincore.Span {
	spans = append(spans, nil)
	copy(spans[idx+1:], spans[idx:])
	spans[idx] = span
	return spans
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

func ErrorCorrectSpans(spanChannel <-chan *zipkincore.Span, output chan<- *zipkincore.Span) {
	traces := make(map[int64]*tree)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case span, ok := <-spanChannel:
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
		traceTooLarge := trace.nodeCount > 4096
		if !traceTooLarge && trace.updated.After(deadline) {
			continue
		}

		delete(traces, traceID)

		if traceTooLarge {
			logrus.Warnf("Trace with %d nodes is too large.", trace.nodeCount)
			continue
		}

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

	var clientService, serverService string
	var clientRecv, clientSent, serverRecv, serverSent int64
	for _, an := range node.Annotations {
		if len(an.Value) == 2 {
			switch an.Value {
			case "cs":
				clientSent = an.Timestamp + offset
				if an.Host != nil {
					clientService = an.Host.ServiceName
				}

			case "cr":
				clientRecv = an.Timestamp + offset

			case "sr":
				serverRecv = an.Timestamp + offset
				if an.Host != nil {
					serverService = an.Host.ServiceName
				}

			case "ss":
				serverSent = an.Timestamp + offset
			}
		}
	}

	//        _________________________
	//       |_cs________|_____________| cr
	//                   |
	//                   |--| <-  (ss+sr)/2 - (cr+cs)/2. If the server is left of the client, this difference is
	//                      |     positive. We need to substract the clients average from the servers average time
	//                      |     to get the corrected time in "client time."
	//            __________|__________
	//           |_sr_______|__________| ss

	if clientRecv != 0 && clientSent != 0 && serverRecv != 0 && serverSent != 0 {
		screw := (serverRecv+serverSent)/2 - (clientRecv+clientSent)/2
		log.Infof("Found time screw of %s between c=%s and s=%s for span '%s'",
			time.Duration(screw)*time.Microsecond,
			clientService, serverService, node.Name)

		// calculate the offset for children based on the fact, that
		// sr must occur after cs and ss must occur before cr.
		offset -= screw
		node.Timestamp = &clientSent

		// update the duration using the client info.
		duration := clientRecv - clientSent
		node.Duration = &duration

	} else if clientSent != 0 && serverRecv != 0 {
		// we only know the timestamps of server + client, so use those to adjust
		offset -= serverRecv - clientSent
		node.Timestamp = &clientSent
	}

	for _, child := range tree.ChildrenOf(node) {
		correctTreeTimings(tree, child, offset)
	}
}

func mergeSpansInPlace(spanToUpdate *zipkincore.Span, newSpan *zipkincore.Span) {
	// update id only if not yet set
	if newSpan.ParentID != nil && spanToUpdate.ParentID == nil {
		spanToUpdate.ParentID = newSpan.ParentID
	}

	// if the new span was send from a server then we want to priority the annotations
	// of the client span. Becuase of this, we'll add the new spans annotations in front of
	// the old spans annotations - sounds counter-intuitive?
	// It is not, if you think of it as "the last value wins!" - like settings values in a map.
	newSpanIsServer := hasAnnotation(newSpan, "sr")

	// merge annotations
	if len(newSpan.Annotations) > 0 {
		if newSpanIsServer {
			// prepend the new annotations to the spanToUpdate ones
			spans := make([]*zipkincore.Annotation, 0, len(spanToUpdate.Annotations)+len(newSpan.Annotations))
			spans = append(spans, newSpan.Annotations...)
			spans = append(spans, spanToUpdate.Annotations...)
			spanToUpdate.Annotations = spans

		} else {
			spanToUpdate.Annotations = append(spanToUpdate.Annotations, newSpan.Annotations...)
		}
	}

	// merge binary annotations
	if len(newSpan.BinaryAnnotations) > 0 {
		if newSpanIsServer {
			// prepend the new annotations to the spanToUpdate ones
			spans := make([]*zipkincore.BinaryAnnotation, 0, len(spanToUpdate.BinaryAnnotations)+len(newSpan.BinaryAnnotations))
			spans = append(spans, newSpan.BinaryAnnotations...)
			spans = append(spans, spanToUpdate.BinaryAnnotations...)
			spanToUpdate.BinaryAnnotations = spans

		} else {
			spanToUpdate.BinaryAnnotations = append(spanToUpdate.BinaryAnnotations, newSpan.BinaryAnnotations...)
		}
	}
}

func hasAnnotation(span *zipkincore.Span, name string) bool {
	for _, an := range span.Annotations {
		if an.Value == name {
			return true
		}
	}

	return false
}
