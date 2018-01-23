package vals

import (
	"bytes"
	"fmt"
	"reflect"
)

func Equal(a, b Value) bool {
	if a.Type() != b.Type() {
		return false
	}
	switch a.Type() {
	case TypeObject, TypeArray:
		return reflect.DeepEqual(a, b)
	case TypeNumber:
		return a.Number() == b.Number()
	case TypeInteger:
		return a.Integer() == b.Integer()
	case TypeBoolean:
		return a.Boolean() == b.Boolean()
	case TypeNull:
		return a.IsNull() == b.IsNull()
	case TypeString:
		return a.String() == b.String()
	}
	return false
}

// CompareTypeBytes compares two byte slices with a known type
// real on the real, this is a bit of a work in progress
// TODO - up tests
func CompareTypeBytes(a, b []byte, t Type) (int, error) {
	if len(a) == 0 && len(b) > 0 {
		return -1, nil
	} else if len(b) == 0 && len(a) > 0 {
		return 1, nil
	} else if len(b) == 0 && len(a) == 0 {
		return 0, nil
	}

	switch t {
	case TypeString:
		return bytes.Compare(a, b), nil
	case TypeInteger:
		return CompareIntegerBytes(a, b)
	case TypeNumber:
		return CompareNumberBytes(a, b)
	default:
		// TODO - other types
		return 0, fmt.Errorf("invalid type comparison")
	}
}

// CompareIntegerBytes compares two byte slices of interger data
func CompareIntegerBytes(a, b []byte) (int, error) {
	at, err := ParseInteger(a)
	if err != nil {
		return 0, err
	}
	bt, err := ParseInteger(b)
	if err != nil {
		return 0, err
	}
	if at > bt {
		return 1, nil
	} else if at == bt {
		return 0, nil
	}
	return -1, nil
}

// CompareNumberBytes compares two byte slices of float data
func CompareNumberBytes(a, b []byte) (int, error) {
	at, err := ParseNumber(a)
	if err != nil {
		return 0, err
	}
	bt, err := ParseNumber(b)
	if err != nil {
		return 0, err
	}
	if at > bt {
		return 1, nil
	} else if at == bt {
		return 0, nil
	}
	return -1, nil
}
