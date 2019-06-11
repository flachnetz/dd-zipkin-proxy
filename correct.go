package zipkinproxy

import (
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
	"sort"
	"strings"
	"time"
)

type Id = proxy.Id

const bufferTime = 10 * time.Second
const maxSpans = 75000

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
	metricsSpansMerged = metrics.GetOrRegisterMeter("spans.merged", nil)
	metricsTracesCorrected = metrics.GetOrRegisterMeter("traces.corrected", nil)
	metricsTracesFinished = metrics.GetOrRegisterMeter("traces.finished", nil)
	metricsTracesWithoutRoot = metrics.GetOrRegisterMeter("traces.noroot", nil)
	metricsTracesTooLarge = metrics.GetOrRegisterMeter("traces.toolarge", nil)
	metricsTracesTooOld = metrics.GetOrRegisterMeter("traces.tooold", nil)
	metricsTracesInflight = metrics.GetOrRegisterGauge("traces.partial.count", nil)
	metricsSpansInflight = metrics.GetOrRegisterGauge("traces.partial.span.count", nil)
	metricsReceivedBlacklistedSpan = metrics.GetOrRegisterMeter("blacklist.span.received", nil)

	metricsTracesFinishedSize = metrics.GetOrRegisterHistogram("traces.finishedsize", nil,
		metrics.NewUniformSample(1024))
}

type none struct{}

type tree struct {
	traceId Id

	// parent-id to span
	// byParent map[Id][]proxy.Span

	// child-id to parent-id
	// byChild map[Id]Id

	// spans not yet belonging to any parent
	spans SpanSlice

	started   time.Time
	updated   time.Time
	nodeCount uint16
}

func newTree(traceId Id) *tree {
	now := time.Now()
	return &tree{
		traceId: traceId,
		started: now,
		updated: now,
	}
}

func (tree *tree) Spans() []proxy.Span {
	return tree.spans
}

func (tree *tree) AddSpan(newSpan proxy.Span) {
	tree.updated = time.Now()

	// get a reference to the span if it already exists
	span := tree.spans.GetSpanRef(newSpan.Id)
	if span == nil {
		tree.spans = tree.spans.Append(newSpan)
		tree.nodeCount++
		return
	}

	if span.Parent.IsUnknown() {
		span.Parent = newSpan.Parent
	}

	mergeSpansInPlace(span, newSpan)
}

func (tree *tree) ByParent() map[Id][]*proxy.Span {
	result := map[Id][]*proxy.Span{}

	for idx := range tree.spans {
		span := &tree.spans[idx]
		result[span.Parent] = append(result[span.Parent], span)
	}

	return result
}

func (tree *tree) GetSpan(spanId Id) *proxy.Span {
	return tree.spans.GetSpanRef(spanId)
}

// gets the root of this tree, or nil, if no root exists.
func (tree *tree) Root() *proxy.Span {
	return tree.spans.GetSpanRef(tree.traceId)
}

// gets the children of the given span in this tree.
func (tree *tree) ChildrenOf(spanId Id) []*proxy.Span {
	var children []*proxy.Span
	for idx := range tree.spans {
		span := &tree.spans[idx]
		if span.Parent == spanId && !span.IsRoot() {
			children = append(children, span)
		}
	}

	return children
}

func (tree *tree) Roots() []*proxy.Span {
	byId := map[Id]struct{}{}
	for _, span := range tree.spans {
		byId[span.Id] = struct{}{}
	}

	// map nodes by id
	var candidates []*proxy.Span

	for idx := range tree.spans {
		span := &tree.spans[idx]

		if span.IsRoot() {
			candidates = append(candidates, span)
			continue
		}

		if _, ok := byId[span.Parent]; !ok {
			candidates = append(candidates, span)
			continue
		}
	}

	return candidates
}

func ErrorCorrectSpans(inputCh <-chan proxy.Span, outputCh chan<- proxy.Span) {
	traces := make(map[Id]*tree)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// blacklisted trace ids.
	blacklistedTraces := map[Id]none{}

	for {
		select {
		case span, ok := <-inputCh:
			// stream was closed, stop now
			if !ok {
				return
			}

			// ignore invalid spans
			if span.Trace == 0 || span.Id == 0 {
				continue
			}

			// check if trace is in black list
			if _, ok := blacklistedTraces[span.Trace]; ok {
				metricsReceivedBlacklistedSpan.Mark(1)
				continue
			}

			trace := traces[span.Trace]
			if trace == nil {
				trace = newTree(span.Trace)
				traces[span.Trace] = trace
			}

			trace.AddSpan(span)

		case <-ticker.C:
			finishTraces(traces, blacklistedTraces, outputCh)
		}
	}
}

func finishTraces(traces map[Id]*tree, blacklist map[Id]none, outputCh chan<- proxy.Span) {
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
			log.Warnf("Trace %x with %d nodes is too large.", traceID, trace.nodeCount)
			debugPrintTrace(trace)

			metricsTracesTooLarge.Mark(1)
			continue
		}

		if traceTooOld {
			blacklist[traceID] = none{}
			log.Warnf("Trace %x with %d nodes is too old", traceID, trace.nodeCount)
			debugPrintTrace(trace)

			metricsTracesTooOld.Mark(1)
			continue
		}

		// if we have a root, try do error correction
		roots := trace.Roots()
		if len(roots) > 1 && allTheSameParent(roots) {
			// add a fake root to the span and look for a new root span
			trace.AddSpan(createFakeRoot(roots))
			roots = trace.Roots()

			log.Debugf("Missing root, injecting fake-root span")
			debugPrintTrace(trace)
		}

		if len(roots) != 1 {
			// we don't have a root, what now?
			log.Debugf("No unique root for trace %x with %d spans", traceID, trace.nodeCount)
			debugPrintTrace(trace)

			metricsTracesWithoutRoot.Mark(1)
			continue
		}

		correctTreeTimings(trace, roots[0], 0)

		metricsTracesCorrected.Mark(1)

		// send all the spans to the output channel
		for _, span := range trace.Spans() {
			outputCh <- span
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

func createFakeRoot(spans []*proxy.Span) proxy.Span {
	firstTimestamp := spans[0].Timestamp
	lastTimestamp := spans[0].Timestamp.Add(spans[0].Duration)

	for _, span := range spans[1:] {
		if span.Timestamp < firstTimestamp {
			firstTimestamp = span.Timestamp
		}

		ts := span.Timestamp.Add(span.Duration)
		if ts > lastTimestamp {
			lastTimestamp = ts
		}
	}

	root := proxy.NewRootSpan("fake-root", spans[0].Trace, spans[0].Parent)

	root.Service = "fake-root"
	root.Timestamp = firstTimestamp
	root.Duration = time.Duration(lastTimestamp - firstTimestamp)

	return root
}

func allTheSameParent(spans []*proxy.Span) bool {
	for _, span := range spans {
		if spans[0].Parent != span.Parent {
			return false
		}
	}

	return true
}

func debugPrintTrace(trace *tree) {
	if logrus.GetLevel() != logrus.DebugLevel {
		return
	}

	const maxLevel = 6
	const maxChildCount = 32

	roots := trace.Roots()
	if len(roots) == 0 {
		return
	}

	var printNode func(*proxy.Span, int)

	printNode = func(node *proxy.Span, level int) {
		space := strings.Repeat("  ", level)

		log.Warnf("%s%s [%x] (%s, %s, parent=%x)", space, node.Name, node.Id,
			node.Timestamp.ToTime(), node.Duration, node.Parent)

		children := trace.ChildrenOf(node.Id)
		for idx, child := range children {
			if level == maxLevel {
				log.Warnf("%s  [...]", space)
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
		log.Warnf("Trace %x, root #%d", roots[0].Trace, idx)
		printNode(root, 0)
	}

	log.Warnln()
}

func discardSuspiciousTraces(trees map[Id]*tree, maxSpans int) {
	var spanCount int

	type trace struct {
		*tree
		id Id
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

func correctTreeTimings(tree *tree, node *proxy.Span, offset time.Duration) {
	if offset != 0 {
		node.Timestamp.AddInPlace(offset)
	}

	clientSent := node.Timings["cs"]
	clientRecv := node.Timings["cr"]
	serverSent := node.Timings["ss"]
	serverRecv := node.Timings["sr"]

	//        _________________________
	//       |_cs________|_____________| cr
	//                   |
	//                   |--| <-  (cr+cs)/2 - (ss+sr)/2. If the server is right of the client, this difference is
	//                      |     negative. We need to add the difference to the server time
	//                      |     to get the corrected time in "client time" under the idea that the
	//                      |     server span is centered in respect to the client span.
	//            __________|__________
	//           |_sr_______|__________| ss

	if clientRecv != 0 && clientSent != 0 && serverRecv != 0 && serverSent != 0 {
		// offset all timings
		clientSent.AddInPlace(offset)
		clientRecv.AddInPlace(offset)
		serverSent.AddInPlace(offset)
		serverRecv.AddInPlace(offset)

		// screw in milliseconds
		screw := time.Duration((clientRecv+clientSent)/2 - (serverRecv+serverSent)/2)

		if screw < -25*time.Millisecond || screw > 25*time.Millisecond {
			log.Debugf("Found time screw of %s for span '%s'", screw, node.Name)
		}

		// offset for child spans
		offset += screw

		// calculate the offset for children based on the fact, that
		// sr must occur after cs and ss must occur before cr.
		node.Timestamp = clientSent

		// update the duration using the client info.
		node.Duration = time.Duration(clientRecv - clientSent)

	} else if clientSent != 0 && serverRecv != 0 {
		// we only know the timestamps of server + client
		offset += time.Duration(clientSent - serverRecv)
		node.Timestamp = clientSent
	}

	children := tree.ChildrenOf(node.Id)
	for idx := range children {
		correctTreeTimings(tree, children[idx], offset)
	}
}

func mergeSpansInPlace(spanToUpdate *proxy.Span, newSpan proxy.Span) {
	_, newSpanIsServer := newSpan.Timings["sr"]

	if newSpanIsServer {
		// prefer values from newSpan (server span)
		if newSpan.Service != "" {
			spanToUpdate.Service = newSpan.Service
		}

		if newSpan.Name != "" {
			spanToUpdate.Name = newSpan.Name
		}

		// merge tags, prefer the ones from newSpan
		for key, value := range newSpan.Tags {
			spanToUpdate.AddTag(key, value)
		}

		for key, value := range newSpan.Timings {
			spanToUpdate.AddTiming(key, value)
		}
	} else {
		// merge tags, prefer the ones from the spanToUpdate (client)
		if spanToUpdate.Service == "" {
			spanToUpdate.Service = newSpan.Service
		}

		if spanToUpdate.Name == "" {
			spanToUpdate.Name = newSpan.Name
		}

		// backup client values, so we can overwrite server values from newSpan later
		clientTags := spanToUpdate.Tags
		clientTimings := spanToUpdate.Timings

		// merge tags
		spanToUpdate.Tags = nil
		for key, value := range newSpan.Tags {
			spanToUpdate.AddTag(key, value)
		}

		for key, value := range clientTags {
			spanToUpdate.AddTag(key, value)
		}

		// merge timings
		spanToUpdate.Timings = nil
		for key, value := range newSpan.Timings {
			spanToUpdate.AddTiming(key, value)
		}

		for key, value := range clientTimings {
			spanToUpdate.AddTiming(key, value)
		}
	}

	metricsSpansMerged.Mark(1)
}

type SpanSlice []proxy.Span

func (spans SpanSlice) GetSpanRef(spanId Id) *proxy.Span {
	idx := sort.Search(len(spans), func(i int) bool {
		return spanId >= spans[i].Id
	})

	if idx < len(spans) && spans[idx].Id == spanId {
		return &spans[idx]
	}

	return nil
}

func (spans SpanSlice) Append(span proxy.Span) []proxy.Span {
	idx := sort.Search(len(spans), func(i int) bool {
		return span.Id >= spans[i].Id
	})

	spans = append(spans, proxy.Span{})
	copy(spans[idx+1:], spans[idx:])
	spans[idx] = span

	return spans
}

func (spans SpanSlice) HasSpan(id proxy.Id) bool {
	return spans.GetSpanRef(id) != nil
}
