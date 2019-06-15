package hyperjson

import (
	"github.com/pkg/errors"
	"unicode/utf8"
)

func decodeStringInPlace(input []byte) ([]byte, error) {
	decoded := input[:0:len(input)]

	for idx := 0; idx < len(input); idx++ {
		ch := input[idx]
		if ch != '\\' {
			decoded = append(decoded, ch)
			continue
		}

		if idx+1 >= len(input) {
			return nil, errors.New("premature end of input")
		}

		// skip to the backslash
		idx++
		ch = input[idx]

		if ch != 'u' {
			// append escaped character as is
			decoded = append(decoded, ch)
			continue
		}

		if idx+4 >= len(input) {
			return nil, errors.New("premature end of input while decoding unicode character")
		}

		// skip to the "u"
		idx++

		// decode the unicode
		var err error
		decoded, err = decodeUnicodeChar(decoded, input[idx:])
		if err != nil {
			return nil, err
		}

		// skip 3 of the consumed characters, the last one
		// will be skipped by the for loop
		idx += 3
	}

	return decoded, nil
}

func decodeUnicodeChar(target []byte, input []byte) ([]byte, error) {
	var r rune
	for _, c := range input[:4] {
		switch {
		case '0' <= c && c <= '9':
			c = c - '0'
		case 'a' <= c && c <= 'f':
			c = c - 'a' + 10
		case 'A' <= c && c <= 'F':
			c = c - 'A' + 10
		default:
			return nil, errors.Errorf("expected hexadecimal character but got '%c'", c)
		}

		r = r*16 + rune(c)
	}

	buf := target[len(target) : len(target)+4]
	n := utf8.EncodeRune(buf, r)
	return target[:len(target)+n], nil
}
