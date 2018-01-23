package vals

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// Type is a type of data, these types follow JSON type primitives,
// with an added type for whole numbers (integer)
type Type uint8

const (
	// TypeUnknown is the default datatype, making for easier
	// errors when a datatype is expected
	TypeUnknown Type = iota
	// TypeNull specifies the null type
	TypeNull
	// TypeInteger specifies whole numbers
	TypeInteger
	// TypeNumber specifies numbers with decimal value
	TypeNumber
	// TypeBoolean species true/false values
	TypeBoolean
	// TypeString speficies text values
	TypeString
	// TypeObject maps string keys to values
	TypeObject
	// TypeArray is an ordered list of values
	TypeArray
)

// NumDatatypes is the total count of data types, including unknown type
const NumDatatypes = 8

// TypeFromString takes a string & tries to return it's type
// defaulting to unknown if the type is unrecognized
func TypeFromString(t string) Type {
	got, ok := map[string]Type{
		"string":  TypeString,
		"integer": TypeInteger,
		"number":  TypeNumber,
		"boolean": TypeBoolean,
		"object":  TypeObject,
		"array":   TypeArray,
		"null":    TypeNull,
	}[t]
	if !ok {
		return TypeUnknown
	}

	return got
}

// ParseType examines a slice of bytes & attempts to determine
// it's type, starting with the more specific possible types, then falling
// back to more general types. ParseType always returns a type
func ParseType(value []byte) Type {
	if len(value) == 0 {
		return TypeString
	}

	if bytes.Equal(value, []byte("null")) {
		return TypeNull
	} else if IsInteger(value) {
		return TypeInteger
	} else if IsFloat(value) {
		return TypeNumber
	} else if IsBoolean(value) {
		return TypeBoolean
	}

	switch JSONArrayOrObject(value) {
	case "object":
		return TypeObject
	case "array":
		return TypeArray
	default:
		return TypeString
	}
}

// String satsfies the stringer interface
func (dt Type) String() string {
	s, ok := map[Type]string{
		TypeUnknown: "",
		TypeString:  "string",
		TypeInteger: "integer",
		TypeNumber:  "number",
		TypeBoolean: "boolean",
		TypeObject:  "object",
		TypeArray:   "array",
		TypeNull:    "null",
	}[dt]

	if !ok {
		return ""
	}

	return s
}

// MarshalJSON implements json.Marshaler on Type
func (dt Type) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, dt.String())), nil
}

// UnmarshalJSON implements json.Unmarshaler on Type
func (dt *Type) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("Filed type should be a string, got %s", data)
	}

	t := TypeFromString(s)

	if t == TypeUnknown {
		return fmt.Errorf("Unknown datatype '%s'", s)
	}

	*dt = t
	return nil
}

// Parse turns raw byte slices into data formatted according to the type receiver
func (dt Type) Parse(value []byte) (parsed interface{}, err error) {
	switch dt {
	case TypeString:
		parsed, err = ParseString(value)
	case TypeNumber:
		parsed, err = ParseNumber(value)
	case TypeInteger:
		parsed, err = ParseInteger(value)
	case TypeBoolean:
		parsed, err = ParseBoolean(value)
	case TypeArray:
		parsed, err = ParseJSON(value)
	case TypeObject:
		parsed, err = ParseJSON(value)
	default:
		return nil, errors.New("cannot parse unknown data type")
	}
	return
}

// ValueToString takes already-parsed values & converts them to a string
func (dt Type) ValueToString(value interface{}) (str string, err error) {
	switch dt {
	case TypeString:
		s, ok := value.(string)
		if !ok {
			err = fmt.Errorf("%v is not a %s value", value, dt.String())
			return
		}
		str = s
	case TypeInteger:
		num, ok := value.(int)
		if !ok {
			err = fmt.Errorf("%v is not an %s value", value, dt.String())
			return
		}
		str = strconv.FormatInt(int64(num), 10)
	case TypeNumber:
		num, ok := value.(float64)
		if !ok {
			err = fmt.Errorf("%v is not a %s value", value, dt.String())
			return
		}
		str = strconv.FormatFloat(float64(num), 'g', -1, 64)
	case TypeBoolean:
		val, ok := value.(bool)
		if !ok {
			err = fmt.Errorf("%v is not a %s value", value, dt.String())
			return
		}
		str = strconv.FormatBool(val)
	case TypeObject, TypeArray:
		data, e := json.Marshal(value)
		if e != nil {
			err = e
			return
		}
		str = string(data)
	default:
		err = fmt.Errorf("cannot get string value of unknown datatype")
		return
	}
	return
}

// ValueToBytes takes already-parsed values & converts them to a slice of bytes
func (dt Type) ValueToBytes(value interface{}) (data []byte, err error) {
	// TODO - for now we just wrap ToString
	str, err := dt.ValueToString(value)
	if err != nil {
		return nil, err
	}

	data = []byte(str)
	return
}

// ParseString converts raw bytes to a string value
func ParseString(value []byte) (string, error) {
	return string(value), nil
}

// ParseNumber converts raw bytes to a float64 value
func ParseNumber(value []byte) (float64, error) {
	return strconv.ParseFloat(string(value), 64)
}

// ParseInteger converts raw bytes to a int64 value
func ParseInteger(value []byte) (int64, error) {
	return strconv.ParseInt(string(value), 10, 64)
}

// ParseBoolean converts raw bytes to a bool value
func ParseBoolean(value []byte) (bool, error) {
	return strconv.ParseBool(string(value))
}

// JSONArrayOrObject examines bytes checking if the outermost
// closure is an array or object
func JSONArrayOrObject(value []byte) string {
	obji := bytes.IndexRune(value, '{')
	arri := bytes.IndexRune(value, '[')
	if obji == -1 && arri == -1 {
		return ""
	}
	if (obji < arri || arri == -1) && obji >= 0 {
		return "object"
	}
	return "array"
}

// ParseJSON converts raw bytes to a JSON value
func ParseJSON(value []byte) (interface{}, error) {
	t := JSONArrayOrObject(value)
	if t == "" {
		return nil, fmt.Errorf("invalid json data")
	}

	if t == "object" {
		p := map[string]interface{}{}
		err := json.Unmarshal(value, &p)
		return p, err
	}

	p := []interface{}{}
	err := json.Unmarshal(value, &p)
	return p, err
}

// IsInteger checks if a slice of bytes is an integer
func IsInteger(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	if value[0] == '[' || value[0] == '{' || !bytes.ContainsAny(value[0:1], "-+0123456789") {
		return false
	}
	if _, err := ParseInteger(value); err == nil || err.(*strconv.NumError).Err == strconv.ErrRange {
		return true
	}
	return false
}

// IsBoolean checks if a slice of bytes is a boolean value
func IsBoolean(value []byte) bool {
	switch string(value) {
	case "1", "0", "t", "f", "T", "F", "true", "false", "TRUE", "FALSE", "True", "False":
		return true
	default:
		return false
	}
}

// IsFloat checks if a slice of bytes is a float value
func IsFloat(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	if value[0] == '[' || value[0] == '{' || !bytes.ContainsAny(value[0:1], "-+0123456789") {
		return false
	}
	if _, err := ParseNumber(value); err == nil || err.(*strconv.NumError).Err == strconv.ErrRange {
		return true
	}
	return false
}
