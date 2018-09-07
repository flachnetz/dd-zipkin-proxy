package zipkinproxy

import (
	"github.com/openzipkin/zipkin-go-opentracing/thrift/gen-go/zipkincore"
	"github.com/rcrowley/go-metrics"
	"sort"
	"time"
	"strings"
)

const bufferTime = 10 * time.Second
const maxSpans = 100000

var metricsSpansMerged metrics.Meter
var metricsTracesFinished metrics.Meter
var metricsTracesFinishedSize metrics.Histogram
var metricsTracesWithoutRoot metrics.Meter
var metricsTracesTooLarge metrics.Meter
var metricsTracesTooOld metrics.Meter
var metricsTracesInflight metrics.Gauge
var metricsSpansInflight metrics.Gauge
var metricsTracesCorrected metrics.Meter
var metricsReceivedBlacklistedSpan metrics.Meter

func init() {
	metricsSpansMerged = metrics.GetOrRegisterMeter("spans.merged", Metrics)
	metricsTracesCorrected = metrics.GetOrRegisterMeter("traces.corrected", Metrics)
	metricsTracesFinished = metrics.GetOrRegisterMeter("traces.finished", Metrics)
	metricsTracesWithoutRoot = metrics.GetOrRegisterMeter("traces.noroot", Metrics)
	metricsTracesTooLarge = metrics.GetOrRegisterMeter("traces.toolarge", Metrics)
	metricsTracesTooOld = metrics.GetOrRegisterMeter("traces.tooold", Metrics)
	metricsTracesInflight = metrics.GetOrRegisterGauge("traces.partial.count", Metrics)
	metricsSpansInflight = metrics.GetOrRegisterGauge("traces.partial.span.count", Metrics)
	metricsReceivedBlacklistedSpan = metrics.GetOrRegisterMeter("blacklist.span.received", Metrics)

	metricsTracesFinishedSize = metrics.GetOrRegisterHistogram("traces.finishedsize", Metrics,
		metrics.NewUniformSample(1024))
}

type none struct{}

type tree struct {
	// parent-id to span
	nodes     map[int64][]*zipkincore.Span
	started   time.Time
	updated   time.Time
	nodeCount uint16
}

func newTree() *tree {
	now := time.Now()
	return &tree{
		nodes:   make(map[int64][]*zipkincore.Span),
		started: now,
		updated: now,
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

// gets the parent of the given span in this tree.
func (tree *tree) ParentOf(span *zipkincore.Span) *zipkincore.Span {
	parentId := span.ParentID
	if parentId == nil {
		return nil
	}

	for _, nodes := range tree.nodes {
		for _, node := range nodes {
			if *parentId == node.ID {
				return node
			}
		}
	}

	return nil
}

func (tree *tree) Roots() []*zipkincore.Span {
	// we can quickly detect if we only have a single root
	if root := tree.Root(); root != nil {
		return []*zipkincore.Span{root}
	}

	byId := map[int64]*zipkincore.Span{}

	// map nodes by id
	for _, nodes := range tree.nodes {
		for _, node := range nodes {
			byId[node.ID] = node
		}
	}

	// now select all nodes with no parent
	var candidates []*zipkincore.Span
	for _, node := range byId {
		if _, ok := byId[node.GetParentID()]; !ok {
			candidates = append(candidates, node)
		}
	}

	return candidates
}

func ErrorCorrectSpans(spanChannel <-chan *zipkincore.Span, output chan<- *zipkincore.Span) {
	traces := make(map[int64]*tree)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// blacklisted trace ids.
	blacklistedTraces := map[int64]none{}

	for {
		select {
		case span, ok := <-spanChannel:
			// stream was closed, stop now
			if !ok {
				return
			}

			// check if trace is in black list
			if _, ok := blacklistedTraces[span.TraceID]; ok {
				metricsReceivedBlacklistedSpan.Mark(1)
				continue
			}

			trace := traces[span.TraceID]
			if trace == nil {
				trace = newTree()
				traces[span.TraceID] = trace
			}

			trace.AddSpan(span)

		case <-ticker.C:
			finishTraces(traces, blacklistedTraces, output)
		}
	}
}

func finishTraces(traces map[int64]*tree, blacklist map[int64]none, output chan<- *zipkincore.Span) {
	var spanCount int64

	deadlineUpdate := time.Now().Add(-bufferTime)
	deadlineStarted := time.Now().Add(-5 * bufferTime)

	for traceID, trace := range traces {
		traceTooLarge := trace.nodeCount > 8*1024
		updatedRecently := trace.updated.After(deadlineUpdate)
		traceTooOld := trace.started.Before(deadlineStarted)

		if !traceTooLarge && !traceTooOld && updatedRecently {
			spanCount += int64(trace.nodeCount)
			continue
		}

		metricsTracesFinishedSize.Update(int64(trace.nodeCount))

		delete(traces, traceID)

		if traceTooLarge {
			blacklist[traceID] = none{}
			log.Warnf("Trace %d with %d nodes is too large.", traceID, trace.nodeCount)
			debugPrintTrace(trace)

			metricsTracesTooLarge.Mark(1)
			continue
		}

		if traceTooOld {
			blacklist[traceID] = none{}
			log.Warnf("Trace %d with %d nodes is too old", traceID, trace.nodeCount)
			debugPrintTrace(trace)

			metricsTracesTooOld.Mark(1)
			continue
		}

		// if we have a root, try do error correction
		root := trace.Root()
		if root != nil {
			correctTreeTimings(trace, root, 0)
			metricsTracesCorrected.Mark(1)
		} else {
			// we don't have a root, what now?
			log.Warnf("No root for trace %d with %d spans", traceID, trace.nodeCount)
			debugPrintTrace(trace)

			metricsTracesWithoutRoot.Mark(1)
			continue
		}

		// send all the spans to the output channel
		for _, spans := range trace.nodes {
			for _, span := range spans {
				output <- span
			}
		}

		metricsTracesFinished.Mark(1)
	}

	// measure in-flight traces and spans
	metricsSpansInflight.Update(spanCount)
	metricsTracesInflight.Update(int64(len(traces)))

	// remove largest traces if we have too many in-flight spans
	if spanCount > maxSpans {
		log.Warnf("There are currently %d in-flight spans, cleaning suspicious traces now", spanCount)
		discardSuspiciousTraces(traces, maxSpans)
	}

	// limit size of blacklist by removing random values
	// iteration of maps in go is non-deterministic
	for id := range blacklist {
		if len(blacklist) < 1024 {
			break
		}

		delete(blacklist, id)
	}
}

func debugPrintTrace(trace *tree) {
	const maxLevel = 6
	const maxChildCount = 32

	roots := trace.Roots()
	if len(roots) == 0 {
		return
	}

	var printNode func(*zipkincore.Span, int)

	printNode = func(node *zipkincore.Span, level int) {
		space := strings.Repeat("  ", level)

		log.Warnf("%s%s [%x] (%s, %s, parent=%x)", space, node.Name, node.ID,
			time.Unix(0, node.GetTimestamp()*int64(time.Microsecond)),
			time.Duration(node.GetDuration())*time.Microsecond,
			node.GetParentID())

		children := trace.ChildrenOf(node)
		for idx, child := range children {
			if level == maxLevel {
				log.Warnf("%s  [...]")
				break
			}

			if idx == maxChildCount {
				log.Warnf("%s  [... %d more children]", space, len(children)-maxChildCount+1)
				break
			}

			printNode(child, level+1)
		}
	}

	for idx, root := range roots {
		log.Warnf("Trace %x, root #%d", roots[0].TraceID, idx)
		printNode(root, 0)
	}

	log.Warnln()
}

func discardSuspiciousTraces(trees map[int64]*tree, maxSpans int) {
	var spanCount int

	type trace struct {
		*tree
		id int64
	}

	traces := make([]trace, 0, len(trees))
	for id, tree := range trees {
		traces = append(traces, trace{tree, id})
		spanCount += int(tree.nodeCount)
	}

	// nothing to do here.
	if spanCount < maxSpans {
		return
	}

	// sort them descending by node count
	sort.Slice(traces, func(i, j int) bool {
		return traces[i].nodeCount > traces[j].nodeCount
	})

	log.Warnf("Need to discard about %d spans", spanCount-maxSpans)

	// remove the traces with the most spans.
	for _, trace := range traces {
		if spanCount < maxSpans {
			break
		}

		log.Warnf("Too many spans, discarding trace %d with %d spans", trace.id, trace.nodeCount)
		delete(trees, trace.id)
		spanCount -= int(trace.nodeCount)
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
		// screw in milliseconds
		screw := time.Duration((serverRecv+serverSent)/2-(clientRecv+clientSent)/2) * time.Microsecond

		if screw > 25*time.Millisecond {
			log.Debugf("Found time screw of %s between c=%s and s=%s for span '%s'",
				screw,
				clientService, serverService, node.Name)
		}

		// calculate the offset for children based on the fact, that
		// sr must occur after cs and ss must occur before cr.
		offset -= int64(screw / time.Microsecond)
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
	// of the client span. Because of this, we'll add the new spans annotations in front of
	// the old spans annotations - sounds counter-intuitive?
	// It is not if you think of it as "the last value wins!" - like settings values in a map.
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

	metricsSpansMerged.Mark(1)
}

func hasAnnotation(span *zipkincore.Span, name string) bool {
	for _, an := range span.Annotations {
		if an.Value == name {
			return true
		}
	}

	return false
}
