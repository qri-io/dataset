package vals

import (
	"encoding/json"
	"fmt"
)

// ConvertDecoded converts an interface that has been decoded into standard go types to a Value
func ConvertDecoded(d interface{}) (Value, error) {
	var err error
	if d == nil {
		return Null(true), nil
	}
	switch v := d.(type) {
	case uint8:
		return Integer(v), nil
	case uint16:
		return Integer(v), nil
	case uint32:
		return Integer(v), nil
	case uint64:
		return Integer(v), nil
	case float64:
		return Number(v), nil
	case int:
		return Integer(v), nil
	case int32:
		return Integer(int(v)), nil
	case int64:
		return Integer(int(v)), nil
	case string:
		return String(v), nil
	case bool:
		return Boolean(v), nil
	case []interface{}:
		arr := make(Array, len(v))
		for i, val := range v {
			arr[i], err = ConvertDecoded(val)
			if err != nil {
				return arr, err
			}
		}
		return &arr, nil
	case map[string]interface{}:
		obj := make(Object, len(v))
		for key, val := range v {
			obj[key], err = ConvertDecoded(val)
			if err != nil {
				return obj, err
			}
		}
		return &obj, nil
	case map[interface{}]interface{}:
		obj := make(Object, len(v))
		for keyi, val := range v {
			key, ok := keyi.(string)
			if !ok {
				return nil, fmt.Errorf("only strings may be used as keys. got %#v", keyi)
			}
			obj[key], err = ConvertDecoded(val)
			if err != nil {
				return obj, err
			}
		}
		return &obj, nil
	default:
		return nil, fmt.Errorf("unrecognized decoded type: %#v", v)
	}
}

// UnmarshalJSON turns a slice of JSON bytes into a Value
func UnmarshalJSON(data []byte) (v Value, err error) {
	switch ParseType(data) {
	case TypeObject:
		return unmarshalObject(data)
	case TypeArray:
		return unmarshalArray(data)
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
	case TypeNull:
		n := Null(true)
		v = &n
	}

	err = json.Unmarshal(data, v)
	return
}

type decodeObj map[string]json.RawMessage

func unmarshalObject(data []byte) (Value, error) {
	do := decodeObj{}
	if err := json.Unmarshal(data, &do); err != nil {
		return nil, err
	}

	obj := make(Object, len(do))
	for key, rm := range do {
		val, err := UnmarshalJSON([]byte(rm))
		if err != nil {
			return nil, err
		}
		switch t := val.(type) {
		case *String:
			obj[key] = *t
		case *Number:
			obj[key] = *t
		case *Integer:
			obj[key] = *t
		case *Null:
			obj[key] = *t
		case Object:
			obj[key] = t
		case Array:
			obj[key] = t
		case *Boolean:
			obj[key] = *t
		}
	}

	return obj, nil
}

type decodeArray []json.RawMessage

func unmarshalArray(data []byte) (Value, error) {
	da := decodeArray{}
	if err := json.Unmarshal(data, &da); err != nil {
		return nil, err
	}

	arr := make(Array, len(da))
	for i, rm := range da {
		val, err := UnmarshalJSON([]byte(rm))
		if err != nil {
			return nil, err
		}
		switch t := val.(type) {
		case *String:
			arr[i] = *t
		case *Number:
			arr[i] = *t
		case *Integer:
			arr[i] = *t
		case *Null:
			arr[i] = *t
		case Object:
			arr[i] = t
		case Array:
			arr[i] = t
		case *Boolean:
			arr[i] = *t
		}
	}

	return arr, nil
}
