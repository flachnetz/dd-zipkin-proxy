package hyperjson

import (
	"bytes"
	"encoding/json"
	. "github.com/onsi/gomega"
	"strings"
	"testing"
)

func tokenOf(t Type, val string) Token {
	return Token{Type: t, Value: []byte(val)}
}

func stringTokenOf(val string) Token {
	return Token{TypeString, []byte(val)}
}

func BenchmarkParser_Skip(b *testing.B) {
	scratch := make([]byte, 4096)

	r := strings.NewReader("")
	for idx := 0; idx < b.N; idx++ {
		r.Reset(jsonString)

		p := NewWithReader(r, scratch)
		if err := p.Skip(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestParser_Read(t *testing.T) {
	g := NewGomegaWithT(t)

	dec := NewWithReader(strings.NewReader(jsonString), nil)

	doTest(g, dec)
}

func doTest(g *GomegaWithT, dec *Parser) {
	g.Expect(dec.NextType()).To(Equal(TypeObjectBegin))
	g.Expect(dec.ConsumeObjectBegin()).ToNot(HaveOccurred())
	g.Expect(dec.NextType()).To(Equal(TypeString))
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("number")))
	g.Expect(dec.ReadNumber()).To(Equal(tokenOf(TypeNumber, "123")))
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("numberZero")))
	g.Expect(dec.Skip()).ToNot(HaveOccurred())
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("boolean")))
	g.Expect(dec.ReadBoolean()).To(Equal(tokenOf(TypeBoolean, "true")))
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("nullValue")))
	g.Expect(dec.ReadNull()).To(Equal(tokenOf(TypeNull, "null")))
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("object")))
	g.Expect(dec.Skip()).ToNot(HaveOccurred())
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("arrayWithNumbers")))
	g.Expect(dec.Skip()).ToNot(HaveOccurred())
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("simple string")))
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("some simple string")))
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("string with escaped quote")))
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf(`The pharse "hello world" is used very often.`)))
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("string with backslash")))
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf(`the symbol '\' is a backslash.`)))
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("string with unicode")))
	g.Expect(dec.ReadString()).To(Equal(stringTokenOf("This is a bullet: \u2022, and this is a triangle: \u2023")))
	g.Expect(dec.ConsumeObjectEnd()).ToNot(HaveOccurred())
}

func TestParser_Compact(t *testing.T) {
	g := NewGomegaWithT(t)

	var buf bytes.Buffer
	g.Expect(json.Compact(&buf, []byte(jsonString))).ToNot(HaveOccurred())

	dec := NewWithReader(bytes.NewReader(buf.Bytes()), nil)

	doTest(g, dec)
}

func TestParser_Pretty(t *testing.T) {
	g := NewGomegaWithT(t)

	var buf bytes.Buffer
	g.Expect(json.Indent(&buf, []byte(jsonString), "", "  ")).ToNot(HaveOccurred())

	dec := NewWithReader(bytes.NewReader(buf.Bytes()), nil)

	doTest(g, dec)
}

const jsonString = `
{
	"number": 123,
	"numberZero": 0,
	"boolean": true,
	"nullValue": null,
	"object": {
		"name": "name as string",
		"age": 43,
		"adult": true
	},
	"arrayWithNumbers": [1, 2, 3],
	"simple string": "some simple string",
	"string with escaped quote": "The pharse \"hello world\" is used very often.",
	"string with backslash": "the symbol '\\' is a backslash.",
	"string with unicode": "This is a bullet: \u2022, and this is a triangle: \u2023"
}`
