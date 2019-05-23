package proxy

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

type Id int64

var _ json.Marshaler = new(Id)
var _ json.Unmarshaler = new(Id)

func (id *Id) Int64() int64 {
	return int64(id.OrZero())
}

func (id *Id) Uint64() uint64 {
	return uint64(id.OrZero())
}

func (id Id) IsUnknown() bool {
	return id == 0
}

func (id *Id) OrZero() Id {
	if id != nil {
		return *id
	} else {
		return 0
	}
}

func (id *Id) Or(other Id) Id {
	if id != nil {
		return *id
	} else {
		return other
	}
}

func (id *Id) Uint64OrNil() *uint64 {
	if id == nil || *id == 0 {
		return nil
	} else {
		value := uint64(*id)
		return &value
	}
}

func (id *Id) MarshalJSON() ([]byte, error) {
	value := int64(*id)

	bytes := [8]byte{
		byte((value >> 56) & 0xff),
		byte((value >> 48) & 0xff),
		byte((value >> 40) & 0xff),
		byte((value >> 32) & 0xff),
		byte((value >> 24) & 0xff),
		byte((value >> 16) & 0xff),
		byte((value >> 8) & 0xff),
		byte(value & 0xff),
	}

	var encoded [18]byte
	hex.Encode(encoded[1:], bytes[:])

	// the result is a json string
	encoded[0] = '"'
	encoded[17] = '"'

	return encoded[:], nil
}

func (id *Id) UnmarshalJSON(bytes []byte) error {
	if len(bytes) < 2 || bytes[0] != '"' || bytes[len(bytes)-1] != '"' {
		return errors.New("expected hex encoded string")
	}

	if len(bytes) > 18 {
		return errors.New("hex value too large")
	}

	var result int64
	for idx := 1; idx < len(bytes)-1; idx++ {
		c := bytes[idx]
		switch {
		case '0' <= c && c <= '9':
			result = (result << 4) | int64(c-'0')

		case 'a' <= c && c <= 'f':
			result = (result << 4) | int64(c-'a') + 10

		case 'A' <= c && c <= 'F':
			result = (result << 4) | int64(c-'A') + 10

		default:
			return fmt.Errorf("hex value must only contain [0-9a-f], got '%c'", c)
		}
	}

	*id = Id(result)

	return nil
}
