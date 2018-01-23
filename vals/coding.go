package vals

import (
	"encoding/json"
)

// UnmarshalJSON turns a slice of bytes into a Value
func UnmarshalJSON(data []byte) (v Value, err error) {
	switch ParseType(data) {
	case TypeString:
		s := String("")
		v = &s
	case TypeInteger:
		i := Integer(0)
		v = &i
	case TypeNumber:
		n := Number(0)
		v = &n
	case TypeBoolean:
		b := Boolean(false)
		v = &b
	case TypeObject:
		v = &Object{}
	case TypeArray:
		v = &Array{}
	case TypeNull:
		n := Null(false)
		v = &n
	}

	err = json.Unmarshal(data, v)
	return
}
