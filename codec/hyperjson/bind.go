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
		return errors.WithMessage(err, "decode timestamp value")
	}

	// assign to target
	*(*string)(target) = cache.StringForByteSliceCopy(tok.Value)

	return nil
}

func MakeMapDecoder(keyDecoder, valueDecoder ValueDecoder) ValueDecoder {
	return func(target unsafe.Pointer, p *Parser) error {
		tok, err := p.Read()
		if err != nil {
			return err
		}

		if tok.Type == TypeNull {
			return nil
		}

		if tok.Type == TypeObjectBegin {
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

		return errors.Errorf("Expected object, got %s", tok.Type)
	}
}

func MakeSliceDecoder(sliceType reflect2.SliceType, decoder ValueDecoder) ValueDecoder {
	return func(target unsafe.Pointer, p *Parser) error {
		if err := p.ConsumeArrayBegin(); err != nil {
			return err
		}

		idx := sliceType.UnsafeLengthOf(target)
		sliceType.UnsafeGrow(target, idx+1)

		el := sliceType.UnsafeGetIndex(target, idx)
		if err := decoder(el, p); err != nil {
			return err
		}

		return p.ConsumeArrayEnd()
	}
}

type Field struct {
	Offset  uintptr
	Decoder ValueDecoder
}

func MakeStructDecoder(fields map[string]Field) ValueDecoder {
	return func(target unsafe.Pointer, p *Parser) error {
		tok, err := p.Read()
		if err != nil {
			return errors.WithMessage(err, "begin object")
		}

		if tok.Type == TypeNull {
			return nil
		}

		if tok.Type == TypeObjectBegin {
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

				if field, ok := fields[byteSliceToString(keyToken.Value)]; ok {
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

		return errors.Errorf("expected object, got %s", tok.Type)
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
