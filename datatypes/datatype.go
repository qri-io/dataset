package datatypes

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// Type is a type of data
type Type int

const (
	// Unknown is the default datatype, making for easier
	// errors when a datatype is expected
	Unknown Type = iota
	// Any specifies any combindation of other datatypes are
	// permitted
	Any
	// String speficies text values
	String
	// Integer specifies whole numbers
	Integer
	// Float specifies numbers with decimal value
	Float
	// Boolean species true/false values
	Boolean
	// Date specifies point-in-time values
	Date
	// URL specifies Universal Resource Locations
	URL
	// JSON speficies Javascript Object Notation data
	JSON
)

// NumDataTypes is the total count of data types, including
// unknown type
const NumDatatypes = 8

// TypeFromString takes a string & tries to return it's type
// defaulting to unknown if the type is unrecognized
func TypeFromString(t string) Type {
	got, ok := map[string]Type{
		"any":     Any,
		"string":  String,
		"integer": Integer,
		"float":   Float,
		"boolean": Boolean,
		"date":    Date,
		"url":     URL,
		"json":    JSON,
	}[t]
	if !ok {
		return Unknown
	}

	return got
}

// ParseDatatype examines a slice of bytes & attempts to determine
// it's type, starting with the more specific possible types, then falling
// back to more general types. ParseDatatype always returns a type
// TODO - should write a version of MUCH faster funcs with "Is" prefix (IsObject, etc)
// that just return t/f. these funcs should aim to bail ASAP when proven false
func ParseDatatype(value []byte) Type {
	var err error
	if _, err = ParseInteger(value); err == nil {
		return Integer
	}
	if _, err = ParseFloat(value); err == nil {
		return Float
	}
	if _, err = ParseBoolean(value); err == nil {
		return Boolean
	}
	if _, err = ParseJSON(value); err == nil {
		return JSON
	}
	if _, err = ParseDate(value); err == nil {
		return Date
	}
	// if _, err = ParseURL(value); err == nil {
	// 	return URL
	// }
	if _, err = ParseString(value); err == nil {
		return String
	}
	return Any
}

// String satsfies the stringer interface
func (dt Type) String() string {
	s, ok := map[Type]string{
		Unknown: "",
		Any:     "any",
		String:  "string",
		Integer: "integer",
		Float:   "float",
		Boolean: "boolean",
		Date:    "date",
		URL:     "url",
		JSON:    "json",
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

	if t == Unknown {
		return fmt.Errorf("Unknown datatype '%s'", s)
	}

	*dt = t
	return nil
}

// Parse turns raw byte slices into data formatted according to the type receiver
func (dt Type) Parse(value []byte) (parsed interface{}, err error) {
	switch dt {
	case Any:
		parsed, err = ParseAny(value)
	case String:
		parsed, err = ParseString(value)
	case Float:
		parsed, err = ParseFloat(value)
	case Integer:
		parsed, err = ParseInteger(value)
	case Boolean:
		parsed, err = ParseBoolean(value)
	case Date:
		parsed, err = ParseDate(value)
	case URL:
		parsed, err = ParseURL(value)
	case JSON:
		parsed, err = ParseJSON(value)
	default:
		return nil, errors.New("cannot parse unknown data type")
	}
	return
}

// ValueToString takes already-parsed values & converts them to a string
func (dt Type) ValueToString(value interface{}) (str string, err error) {
	switch dt {
	case Any:
		// TODO
		return "", fmt.Errorf("converting 'any' value to string not yet supported")
	case String:
		s, ok := value.(string)
		if !ok {
			err = fmt.Errorf("%v is not a %s value", value, dt.String())
			return
		}
		str = s
	case Integer:
		num, ok := value.(int)
		if !ok {
			err = fmt.Errorf("%v is not an %s value", value, dt.String())
			return
		}
		str = strconv.FormatInt(int64(num), 10)
	case Float:
		num, ok := value.(float32)
		if !ok {
			err = fmt.Errorf("%v is not a %s value", value, dt.String())
			return
		}
		str = strconv.FormatFloat(float64(num), 'g', -1, 64)
	case Boolean:
		val, ok := value.(bool)
		if !ok {
			err = fmt.Errorf("%v is not a %s value", value, dt.String())
			return
		}
		str = strconv.FormatBool(val)
	case JSON:
		data, e := json.Marshal(value)
		if e != nil {
			err = e
			return
		}
		str = string(data)
	case Date:
		val, ok := value.(time.Time)
		if !ok {
			err = fmt.Errorf("%v is not a %s value", value, dt.String())
			return
		}
		str = val.Format(time.RFC3339)
	case URL:
		val, ok := value.(*url.URL)
		if !ok {
			err = fmt.Errorf("%v is not a %s value", value, dt.String())
			return
		}
		str = val.String()
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

// ParseAny is a work in progress :/
func ParseAny(value []byte) (interface{}, error) {
	// TODO
	return nil, nil
}

// ParseString converts raw bytes to a string value
func ParseString(value []byte) (string, error) {
	return string(value), nil
}

// ParseFloat converts raw bytes to a float64 value
func ParseFloat(value []byte) (float64, error) {
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

// ParseDate converts raw bytes to a time.Time value
func ParseDate(value []byte) (t time.Time, err error) {
	str := string(value)
	for _, format := range []string{
		time.RFC3339,
		time.RFC3339Nano,
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		time.RFC822,
		time.RFC822Z,
		time.RFC850,
		time.RFC1123,
		time.RFC1123Z,
	} {
		if t, err = time.Parse(format, str); err == nil {
			return
		}
	}
	return time.Now(), fmt.Errorf("invalid date: %s", str)
}

// ParseURL converts raw bytes to a *url.URL value
func ParseURL(value []byte) (*url.URL, error) {
	if !Relaxed.Match(value) {
		return nil, fmt.Errorf("invalid url: %s", string(value))
	}
	return url.Parse(string(value))
}

// JSONArrayOrObject examines bytes checking if the outermost
// closure is an array or object
func JSONArrayOrObject(value []byte) (string, error) {
	obji := bytes.IndexRune(value, '{')
	arri := bytes.IndexRune(value, '[')
	if obji == -1 && arri == -1 {
		return "", fmt.Errorf("invalid json data")
	}
	if (obji < arri || arri == -1) && obji >= 0 {
		return "object", nil
	}
	return "array", nil
}

// ParseJSON converts raw bytes to a JSON value
func ParseJSON(value []byte) (interface{}, error) {
	t, err := JSONArrayOrObject(value)
	if err != nil {
		return nil, err
	}

	if t == "object" {
		p := map[string]interface{}{}
		err = json.Unmarshal(value, &p)
		return p, err
	}

	p := []interface{}{}
	err = json.Unmarshal(value, &p)
	return p, err
}
