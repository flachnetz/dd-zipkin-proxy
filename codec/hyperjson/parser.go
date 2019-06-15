package hyperjson

import (
	"github.com/pkg/errors"
	"io"
)

//go:generate stringer -type=Type
type Type uint8

const (
	TypeError Type = iota
	TypeNull
	TypeObjectBegin
	TypeObjectEnd
	TypeArrayBegin
	TypeArrayEnd
	TypeBoolean
	TypeNumber
	TypeString

	typeWhitespace
)

type Parser struct {
	b Buffer
}

func ForReader(r io.Reader, buf []byte) *Parser {
	return &Parser{
		b: Buffer{
			input:  r,
			buffer: buf,
		},
	}
}

func ForBytes(buf []byte) *Parser {
	return &Parser{
		b: Buffer{
			tail:   0,
			head:   len(buf),
			eof:    true,
			buffer: buf,
		},
	}
}

func (p *Parser) ConsumeObjectBegin() error {
	if err := p.skipWhitespace(); err != nil {
		return err
	}

	return p.consumeObjectBegin()
}

func (p *Parser) consumeObjectBegin() error {
	if p.mustCurrentType() != TypeObjectBegin {
		return p.wrongTypeError(TypeObjectBegin)
	}
	// skip the begin object symbol
	p.b.tail++
	return nil
}

func (p *Parser) ConsumeObjectEnd() error {
	if err := p.skipWhitespace(); err != nil {
		return err
	}

	return p.consumeObjectEnd()
}

func (p *Parser) consumeObjectEnd() error {
	if p.mustCurrentType() != TypeObjectEnd {
		return p.wrongTypeError(TypeObjectEnd)
	}
	// skip the '}' symbol
	p.b.tail++
	return nil
}

func (p *Parser) ConsumeArrayBegin() error {
	if err := p.skipWhitespace(); err != nil {
		return err
	}

	return p.consumeArrayBegin()
}

func (p *Parser) consumeArrayBegin() error {
	if p.mustCurrentType() != TypeArrayBegin {
		return p.wrongTypeError(TypeArrayBegin)
	}
	// skip the begin array symbol
	p.b.tail++
	return nil
}

func (p *Parser) ConsumeArrayEnd() error {
	if err := p.skipWhitespace(); err != nil {
		return err
	}

	return p.consumeArrayEnd()
}

func (p *Parser) consumeArrayEnd() error {
	if p.mustCurrentType() != TypeArrayEnd {
		return p.wrongTypeError(TypeArrayEnd)
	}
	// skip the ']' symbol
	p.b.tail++
	return nil
}

func (p *Parser) Read() (Token, error) {
	next, err := p.NextType()
	if err != nil {
		return errToken, err
	}

	switch next {
	case TypeString:
		return p.readString()

	case TypeNumber:
		return p.readNumber()

	case TypeBoolean:
		return p.readBoolean()

	case TypeNull:
		return p.readNull()

	default:
		return errToken, errors.Errorf("expected literal, got '%s'", next)
	}
}

func (p *Parser) ReadString() (Token, error) {
	if err := p.skipWhitespace(); err != nil {
		return errToken, err
	}

	return p.readString()
}

func (p *Parser) readString() (Token, error) {
	buf := &p.b
	ch := buf.Current()
	if ch != '"' {
		return errToken, p.wrongTypeError(TypeString)
	}
	buf.tail++
	var n int
	var escapeNext bool
	var stringHasEscaping bool
	for {
		avail := buf.Available()
		for ; n < avail; n++ {
			ch := buf.buffer[buf.tail+n]

			switch {
			case escapeNext:
				escapeNext = false

			case ch == '\\':
				escapeNext = true
				stringHasEscaping = true

			case ch == '"':
				// reached the end of the string
				token := Token{
					Type:  TypeString,
					Value: buf.buffer[buf.tail : buf.tail+n],
				}

				if stringHasEscaping {
					decoded, err := decodeStringInPlace(token.Value)
					if err != nil {
						return errToken, err
					}

					token.Value = decoded
				}

				// skip string and closing quote in original buffer
				buf.tail += n + 1
				return token, nil
			}
		}

		if err := buf.LoadMore(); err != nil {
			return errToken, err
		}
	}
}

func (p *Parser) ReadNumber() (Token, error) {
	if err := p.skipWhitespace(); err != nil {
		return errToken, err
	}

	return p.readNumber()
}

func (p *Parser) readNumber() (Token, error) {
	buf := &p.b
	ch := buf.Current()
	if !isCharDigit(ch) {
		return errToken, p.wrongTypeError(TypeNumber)
	}

	var n = 1
	for {
		avail := buf.Available()
		for ; n < avail; n++ {
			ch := buf.buffer[buf.tail+n]

			if !isCharDigit(ch) {
				token := Token{
					Type:  TypeNumber,
					Value: buf.buffer[buf.tail : buf.tail+n],
				}

				buf.tail += n

				return token, nil
			}
		}

		if err := buf.LoadMore(); err != nil {
			return errToken, err
		}
	}
}

func (p *Parser) NextType() (Type, error) {
	if err := p.skipWhitespace(); err != nil {
		return TypeError, err
	}

	return p.currentType()
}

func (p *Parser) currentType() (Type, error) {
	tokenType := symbolTable[p.b.Current()]

	if tokenType == 0 {
		return TypeError, errors.Errorf("unexpected character: '%c'", p.b.Current())
	}

	return tokenType, nil
}

func (p *Parser) mustCurrentType() Type {
	return symbolTable[p.b.Current()]
}

func (p *Parser) skipWhitespace() error {
	b := &p.b

	for {
		// skip whitespace directly in the buffer
		for idx := b.tail; idx < b.head; idx++ {
			if !isCharSpace(b.buffer[idx]) {
				b.tail = idx
				return nil
			}
		}

		if err := b.LoadMore(); err != nil {
			return err
		}
	}
}

func (p *Parser) readN(tokenType Type, first byte, n int) (Token, error) {
	if err := p.b.require(n); err != nil {
		return errToken, err
	}

	if ch := p.b.Current(); ch != first {
		return errToken, p.wrongTypeError(tokenType)
	}

	p.b.tail += n

	return Token{Type: tokenType, Value: p.b.buffer[p.b.tail-n : p.b.tail]}, nil
}

func (p *Parser) ReadNull() (Token, error) {
	if err := p.skipWhitespace(); err != nil {
		return errToken, err
	}

	return p.readNull()
}

func (p *Parser) readNull() (Token, error) {
	return p.readN(TypeNull, 'n', 4)
}

func (p *Parser) ReadBoolean() (Token, error) {
	if err := p.skipWhitespace(); err != nil {
		return errToken, err
	}

	return p.readBoolean()
}

func (p *Parser) readBoolean() (Token, error) {
	if p.b.Current() == 't' {
		return p.readN(TypeBoolean, 't', 4)
	} else {
		return p.readN(TypeBoolean, 'f', 5)
	}
}

var symbolTable = [256]Type{
	'\t':       typeWhitespace,
	'\n':       typeWhitespace,
	'\v':       typeWhitespace,
	'\f':       typeWhitespace,
	'\r':       typeWhitespace,
	' ':        typeWhitespace,
	':':        typeWhitespace,
	',':        typeWhitespace,
	Type(0x85): typeWhitespace,
	Type(0xA0): typeWhitespace,

	'-': TypeNumber,
	'0': TypeNumber,
	'1': TypeNumber,
	'2': TypeNumber,
	'3': TypeNumber,
	'4': TypeNumber,
	'5': TypeNumber,
	'6': TypeNumber,
	'7': TypeNumber,
	'8': TypeNumber,
	'9': TypeNumber,

	'n': TypeNull,
	't': TypeBoolean,
	'f': TypeBoolean,

	'"': TypeString,

	'{': TypeObjectBegin,
	'}': TypeObjectEnd,

	'[': TypeArrayBegin,
	']': TypeArrayEnd,
}

func isCharSpace(b byte) bool {
	return symbolTable[b] == typeWhitespace
}

func isCharDigit(b byte) bool {
	return symbolTable[b] == TypeNumber
}

func (p *Parser) wrongTypeError(expected Type) error {
	actual, err := p.currentType()
	if err != nil {
		return errors.Errorf(`expected token of type '%s', but found character '%c'`, expected, p.b.Current())
	}

	return errors.Errorf(`expected token of type '%s', but found '%s'`, expected, actual)
}
