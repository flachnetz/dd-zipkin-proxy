package hyperjson

import (
	"github.com/flachnetz/dd-zipkin-proxy/cache"
	"github.com/modern-go/reflect2"
	"github.com/pkg/errors"
	"unsafe"
)

type ValueDecoder func(target unsafe.Pointer, p *Parser) error

func Uint64ValueDecoder(target unsafe.Pointer, p *Parser) error {
	tok, err := p.ReadNumber()
	if err != nil {
		return errors.WithMessage(err, "decode uint64 value")
	}

	var result uint64
	for _, ch := range tok.Value {
		if ch < '0' || ch > '9' {
			return errors.Errorf("Unexpected character in number: '%c'", ch)
		}

		result = result*10 + uint64(ch-'0')
	}

	// assign to target
	*(*uint64)(target) = result

	return nil
}

func StringValueDecoder(target unsafe.Pointer, p *Parser) error {
	tok, err := p.ReadString()
	if err != nil {
		return errors.WithMessage(err, "decode value")
	}

	// assign to target
	*(*string)(target) = cache.StringForByteSliceCopy(tok.Value)

	return nil
}

func MakeMapDecoder(keyDecoder, valueDecoder ValueDecoder) ValueDecoder {
	return func(target unsafe.Pointer, p *Parser) error {
		if err := p.ConsumeObjectBegin(); err != nil {
			return errors.WithMessage(err, "begin object")
		}

		var result map[string]string
		for {
			next, err := p.NextType()
			if err != nil {
				return err
			}

			if next == TypeObjectEnd {
				*(*map[string]string)(target) = result
				return p.ConsumeObjectEnd()
			}

			// decode the key to a string.
			var key string
			if err := keyDecoder(reflect2.NoEscape(unsafe.Pointer(&key)), p); err != nil {
				return err
			}

			// decode the value to a string
			var value string
			if err := valueDecoder(reflect2.NoEscape(unsafe.Pointer(&value)), p); err != nil {
				return err
			}

			if result == nil {
				result = make(map[string]string)
			}

			result[key] = value
		}
	}
}

func MakeSliceDecoder(sliceType reflect2.SliceType, decoder ValueDecoder) ValueDecoder {
	return func(target unsafe.Pointer, p *Parser) error {
		if err := p.ConsumeArrayBegin(); err != nil {
			return errors.WithMessage(err, "decoding "+sliceType.String())
		}

		var idx int
		for {
			// check the next token type
			next, err := p.NextType()
			if err != nil {
				return err
			}

			if next == TypeArrayEnd {
				if err := p.ConsumeArrayEnd(); err != nil {
					return errors.WithMessage(err, "decoding end of "+sliceType.String())
				}

				return nil
			}

			// grow slice so that the last element is available
			sliceType.UnsafeGrow(target, idx+1)

			// decode into the last element
			el := sliceType.UnsafeGetIndex(target, idx)
			if err := decoder(el, p); err != nil {
				return errors.WithMessage(err, "decoding element of type "+sliceType.Elem().String())
			}

			idx++
		}
	}
}

func MakeArrayDecoder(arrayType reflect2.ArrayType, decoder ValueDecoder) ValueDecoder {
	return func(target unsafe.Pointer, p *Parser) error {
		if err := p.ConsumeArrayBegin(); err != nil {
			return errors.WithMessage(err, "decoding "+arrayType.String())
		}

		var idx int

		for {
			// check the next token type
			next, err := p.NextType()
			if err != nil {
				return err
			}

			if next == TypeArrayEnd {
				if err := p.ConsumeArrayEnd(); err != nil {
					return errors.WithMessage(err, "decoding end of "+arrayType.String())
				}

				return nil
			}

			if idx < arrayType.Len() {
				el := arrayType.UnsafeGetIndex(target, idx)
				if err := decoder(el, p); err != nil {
					return errors.WithMessage(err, "decoding array element of type "+arrayType.Elem().String())
				}

			} else {
				if err := p.Skip(); err != nil {
					return errors.WithMessage(err, "skipping out of range array element of type "+arrayType.Elem().String())
				}
			}

			idx++
		}
	}
}

type Field struct {
	JsonName string
	Offset   uintptr
	Decoder  ValueDecoder
}

func MakeStructDecoder(fields []Field) ValueDecoder {
	var lookup func(name string) (Field, bool)

	switch len(fields) {
	case 1:
		field := fields[0]

		lookup = func(name string) (Field, bool) {
			if field.JsonName == name {
				return field, true
			} else {
				return Field{}, false
			}
		}
	default:
		lookupMap := make(map[string]Field, len(fields))
		for _, field := range fields {
			lookupMap[field.JsonName] = field
		}

		lookup = func(name string) (Field, bool) {
			field, ok := lookupMap[name]
			return field, ok
		}
	}

	return func(target unsafe.Pointer, p *Parser) error {
		if err := p.ConsumeObjectBegin(); err != nil {
			return errors.WithMessage(err, "begin object")
		}

		for {
			next, err := p.NextType()
			if err != nil {
				return err
			}

			if next == TypeObjectEnd {
				return p.ConsumeObjectEnd()
			}

			// decode the key to a string.
			keyToken, err := p.ReadString()
			if err != nil {
				return err
			}

			if field, ok := lookup(byteSliceToString(keyToken.Value)); ok {
				if err := field.Decoder(unsafe.Pointer(uintptr(target)+field.Offset), p); err != nil {
					return err
				}
			} else {
				if err := p.Skip(); err != nil {
					return err
				}
			}
		}
	}
}

func OffsetOf(value interface{}, field string) uintptr {
	st := reflect2.TypeOf(value).(reflect2.StructType)
	return st.FieldByName(field).Offset()
}

// Returns a string that shares the data with the given byte slice.
func byteSliceToString(bytes []byte) string {
	if bytes == nil {
		return ""
	}

	return *(*string)(unsafe.Pointer(&bytes))
}
