package vals

import (
	"encoding/json"
)

// UnmarshalJSON turns a slice of bytes into a Value
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
		n := Null(false)
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
		if val, err := UnmarshalJSON([]byte(rm)); err != nil {
			return nil, err
		} else {
			switch t := val.(type) {
			case *String:
				obj[key] = *t
			case *Number:
				obj[key] = *t
			case *Integer:
				obj[key] = *t
			case *Null:
				obj[key] = *t
			case *Object:
				obj[key] = *t
			case *Array:
				obj[key] = *t
			case *Boolean:
				obj[key] = *t
			}
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
		if val, err := UnmarshalJSON([]byte(rm)); err != nil {
			return nil, err
		} else {
			switch t := val.(type) {
			case *String:
				arr[i] = *t
			case *Number:
				arr[i] = *t
			case *Integer:
				arr[i] = *t
			case *Null:
				arr[i] = *t
			case *Object:
				arr[i] = *t
			case *Array:
				arr[i] = *t
			case *Boolean:
				arr[i] = *t
			}
		}
	}

	return arr, nil
}
