package zipkinproxy

import (
	"github.com/flachnetz/dd-zipkin-proxy/proxy"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"math/rand"
	"testing"
	"time"
)

func TestTree(t *testing.T) {
	RegisterTestingT(t)

	firstSpan := proxy.Span{Id: 1}
	secondSpan := proxy.Span{Id: 2, Parent: firstSpan.Id}

	tree := newTree(1)
	tree.AddSpan(firstSpan)
	tree.AddSpan(secondSpan)

	Expect(tree.ChildrenOf(firstSpan.Id)).To(HaveLen(1))
	Expect(tree.ChildrenOf(firstSpan.Id)[0]).To(Equal(&secondSpan))

	Expect(tree.Root()).To(Equal(&firstSpan))
	Expect(tree.Roots()).To(HaveLen(1))
	Expect(tree.Roots()[0]).To(Equal(&firstSpan))
}

func TestMergeSpansInPlace_Annotations(t *testing.T) {
	RegisterTestingT(t)

	firstSpan := proxy.Span{}
	firstSpan.AddTiming("cs", 1)

	secondSpan := proxy.Span{}
	secondSpan.AddTiming("ss", 2)

	mergeSpansInPlace(&firstSpan, secondSpan)

	Expect(firstSpan.Timings.CS).To(Equal(proxy.Timestamp(1)))
	Expect(firstSpan.Timings.SS).To(Equal(proxy.Timestamp(2)))
}

func TestMergeSpansInPlace_BinaryAnnotations(t *testing.T) {
	RegisterTestingT(t)

	// this is the server span
	firstSpan := proxy.Span{}
	firstSpan.AddTiming("sr", 0)
	firstSpan.AddTag("tag", "a")

	secondSpan := proxy.Span{}
	secondSpan.AddTag("tag", "b")

	mergeSpansInPlace(&firstSpan, secondSpan)

	Expect(firstSpan.Tags).To(HaveLen(1))
	Expect(firstSpan.Tags["tag"]).To(Equal("a"))
}

func TestMergeSpansInPlace_BinaryAnnotations_Reverse(t *testing.T) {
	RegisterTestingT(t)

	// this is the server span
	firstSpan := proxy.Span{}
	firstSpan.AddTag("tag", "a")

	secondSpan := proxy.Span{}
	secondSpan.AddTiming("sr", 1)
	secondSpan.AddTag("tag", "b")

	mergeSpansInPlace(&firstSpan, secondSpan)

	Expect(firstSpan.Tags).To(HaveLen(1))
	Expect(firstSpan.Tags["tag"]).To(Equal("b"))
}

func TestCorrectTimings(t *testing.T) {
	RegisterTestingT(t)

	for i := 0; i < 100; i++ {
		indices := rand.Perm(4)
		baseOffset := proxy.Timestamp(rand.Int31n(100000))

		scale := proxy.Timestamp(1 * time.Millisecond)
		client, sharedClient, sharedServer, server := threeSpans(100*scale, 200*scale, 1110*scale, 1190*scale)

		tree := newTree(client.Trace)

		if rand.Float32() < 0.5 {
			sharedServer.Parent = 0
		}

		// add spans in random order to the tree.
		spans := []proxy.Span{client, sharedClient, sharedServer, server}
		for idx := range spans {
			tree.AddSpan(spans[indices[idx]])
		}

		logrus.SetLevel(logrus.DebugLevel)
		debugPrintTrace(tree)

		correctTreeTimings(tree, tree.Root(), nil, time.Duration(baseOffset))

		clientSpan := tree.GetSpan(client.Id)
		Expect(clientSpan.Timestamp).To(BeEquivalentTo(proxy.Timestamp(baseOffset + 100*scale)))

		serverSpan := tree.GetSpan(server.Id)
		Expect(serverSpan.Timestamp).To(BeEquivalentTo(proxy.Timestamp(baseOffset + 110*scale)))

		shared := tree.GetSpan(sharedClient.Id)
		Expect(shared.Timestamp).To(BeEquivalentTo(proxy.Timestamp(baseOffset + 100*scale)))
	}
}

func threeSpans(cs, cr, sr, ss proxy.Timestamp) (proxy.Span, proxy.Span, proxy.Span, proxy.Span) {
	offset := proxy.Timestamp(100 * time.Millisecond)

	client := proxy.Span{Id: 1, Trace: 1, Parent: 1, Timestamp: cs, Duration: time.Duration(cr - cs)}
	client.AddTiming("cs", cs)
	client.AddTiming("cr", cr)

	sharedClient := proxy.Span{Id: 2, Trace: 1, Parent: client.Id, Timestamp: cs, Duration: time.Duration(cr - cs)}
	sharedClient.AddTiming("cs", cs)
	sharedClient.AddTiming("cr", cr)

	sharedServer := proxy.Span{Id: 2, Trace: 1, Parent: client.Id, Timestamp: sr + offset, Duration: time.Duration(ss - sr)}
	sharedServer.AddTiming("sr", sr+offset)
	sharedServer.AddTiming("ss", ss+offset)

	server := proxy.Span{Id: 3, Trace: 1, Parent: sharedServer.Id, Timestamp: sr + offset, Duration: time.Duration(ss - sr)}
	server.AddTiming("sr", sr+offset)
	server.AddTiming("ss", ss+offset)

	return client, sharedClient, sharedServer, server
}
