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
	input  io.Reader
	buffer []byte
	tail   int
	head   int
	eof    bool
}

func NewWithReader(r io.Reader, buf []byte) *Parser {
	return &Parser{
		input:  r,
		buffer: buf,
	}
}

func NewWithBytes(buf []byte) *Parser {
	return &Parser{
		tail:   0,
		head:   len(buf),
		eof:    true,
		buffer: buf,
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
	p.tail++
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
	p.tail++
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
	p.tail++
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
	p.tail++
	return nil
}

func (p *Parser) ReadLiteral() (Token, error) {
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
	ch := p.current()
	if ch != '"' {
		return errToken, p.wrongTypeError(TypeString)
	}
	p.tail++
	var n int
	var escapeNext bool
	var stringHasEscaping bool
	for {
		avail := p.available()
		for ; n < avail; n++ {
			ch := p.buffer[p.tail+n]

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
					Value: p.buffer[p.tail : p.tail+n],
				}

				if stringHasEscaping {
					decoded, err := decodeStringInPlace(token.Value)
					if err != nil {
						return errToken, err
					}

					token.Value = decoded
				}

				// skip string and closing quote in original buffer
				p.tail += n + 1
				return token, nil
			}
		}

		if err := p.bufferMore(); err != nil {
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
	ch := p.current()
	if !isCharDigit(ch) {
		return errToken, p.wrongTypeError(TypeNumber)
	}

	var n = 1
	for {
		avail := p.available()
		for ; n < avail; n++ {
			ch := p.buffer[p.tail+n]

			if !isCharDigit(ch) && ch != '.' && ch != 'e' {
				token := Token{
					Type:  TypeNumber,
					Value: p.buffer[p.tail : p.tail+n],
				}

				p.tail += n

				return token, nil
			}
		}

		if err := p.bufferMore(); err != nil {
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
	tokenType := symbolTable[p.current()]

	if tokenType == 0 {
		return TypeError, errors.Errorf("unexpected character: '%c'", p.current())
	}

	return tokenType, nil
}

func (p *Parser) mustCurrentType() Type {
	return symbolTable[p.current()]
}

func (p *Parser) skipWhitespace() error {
	for {
		// skip whitespace directly in the buffer
		for ; p.tail < p.head; p.tail++ {
			if !isCharSpace(p.buffer[p.tail]) {
				return nil
			}
		}

		if err := p.bufferMore(); err != nil {
			return err
		}
	}
}

func (p *Parser) read1(tokenType Type) (Token, error) {
	if p.mustCurrentType() != tokenType {
		return errToken, p.wrongTypeError(tokenType)
	}

	p.tail++

	return Token{Type: tokenType, Value: p.buffer[p.tail-1 : p.tail]}, nil
}

func (p *Parser) readN(tokenType Type, first byte, n int) (Token, error) {
	if err := p.require(n); err != nil {
		return errToken, err
	}

	if ch := p.current(); ch != first {
		return errToken, p.wrongTypeError(tokenType)
	}

	p.tail += n

	return Token{Type: tokenType, Value: p.buffer[p.tail-n : p.tail]}, nil
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
	if p.current() == 't' {
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
		return errors.Errorf(`expected token of type '%s', but found character '%c'`, expected, p.current())
	}

	return errors.Errorf(`expected token of type '%s', but found '%s'`, expected, actual)
}

func (p *Parser) current() byte {
	return p.buffer[p.tail]
}

func (p *Parser) require(n int) error {
	for p.available() < n {
		missing := n - p.available()

		free := len(p.buffer) - p.available()
		if free < missing {
			buf := p.buffer

			// increase buffer size and copy existing data to new buffer
			p.buffer = make([]byte, p.available()+n)
			copy(p.buffer[:len(buf)], buf)
		}

		if err := p.bufferMore(); err != nil {
			return errors.WithMessagef(err, "require(%d)", n)
		}
	}

	return nil
}

// Number of bytes currently available in the buffer
func (p *Parser) available() int {
	return p.head - p.tail
}

// Load more data into the buffer.
func (p *Parser) bufferMore() error {
	if p.eof {
		return io.EOF
	}

	if p.buffer == nil {
		p.buffer = make([]byte, 4096)
	}

	// move existing bytes to the beginning of the buffer
	if p.tail > 0 {
		previousCount := p.available()
		copy(p.buffer[:previousCount], p.buffer[p.tail:p.head])
		p.tail = 0
		p.head = previousCount
	}

	// read more data into the rest
	count, err := p.input.Read(p.buffer[p.head:])
	if err == io.EOF {
		// stop in case of EOF
		p.eof = true
		if count == 0 {
			return io.EOF
		}

	} else if err != nil {
		return errors.WithMessage(err, "buffer more data")
	}

	p.head += count
	return nil
}
