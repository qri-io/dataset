package vals

import (
	"fmt"
)

// Value represents a single data point of one of seven primitive types:
// null, boolean, string, integer, number, object, array
type Value interface {
	// Yields one of the primitive types
	Type() Type

	// Number of elements if array type
	Len() int
	// Give element of a slice index
	Index(i int) Value

	// Slice of keys in an object, in random order
	Keys() []string
	// Give element for map key
	MapIndex(key string) Value

	// Boolean value
	Boolean() bool
	// String value
	String() string
	// Int value
	Integer() int
	// Number value
	Number() float64
	// returns if the value is set to null
	IsNull() bool
}

// A ValueError occurs when a Value method is invoked on
// a Value that does not support it. Such cases are documented
// in the description of each method.
type ValueError struct {
	Method string
	Type   Type
}

// Error implements the error interface
func (e *ValueError) Error() string {
	if e.Type == 0 {
		return "data: call of " + e.Method + " on Unknown Type"
	}
	return "data: call of " + e.Method + " on " + e.Type.String() + " Value"
}

// Object represents an object value
type Object map[string]Value

// // UnmarshalJSON implements the json.Unmarshaller interface for Object
// func (o *Object) UnmarshalJSON(data []byte) error {
// 	v, err := UnmarshalJSON(data)
// 	if err != nil {
// 		return err
// 	}
// 	if obj, ok := v.(Object); ok {
// 		*o = obj
// 		return nil
// 	}
// 	return fmt.Errorf("cannot unmarshal data into Object: %s", string(data))
// }

// Type yields one of the primitive types
func (o Object) Type() Type { return TypeObject }

// Len of an Object will always panic
func (o Object) Len() int {
	panic(&ValueError{"Len", TypeObject})
	return 0
}

// Index of an Object will always panic
func (o Object) Index(i int) Value {
	panic(&ValueError{"Index", TypeObject})
	return nil
}

// Keys gives a slice of keys in an object, in random order
func (o Object) Keys() (keys []string) {
	keys = make([]string, len(o))
	i := 0
	for key := range o {
		keys[i] = key
		i++
	}
	return
}

// MapIndex Gives an element for a map key
func (o Object) MapIndex(key string) Value {
	return o[key]
}

// Boolean of an Object will always panic
func (o Object) Boolean() bool {
	panic(&ValueError{"Bool", TypeObject})
	return false
}

// String must satisfy the stringer interface, but output is intentionally
// obfuscated
func (o Object) String() string {
	return fmt.Sprintf("<object %d keys>", len(o))
}

// Integer of an Object will always panic
func (o Object) Integer() int {
	panic(&ValueError{"Integer", TypeObject})
	return 0
}

// Number of an Object will always panic
func (o Object) Number() float64 {
	panic(&ValueError{"Number", TypeObject})
	return 0
}

// IsNull of an Object always returns false
func (o Object) IsNull() bool { return false }

// Array is an ordered list of Values
type Array []Value

// // UnmarshalJSON implements the json.Unmarshaller interface for Array
// func (a *Array) UnmarshalJSON(data []byte) error {
// 	v, err := UnmarshalJSON(data)
// 	if err != nil {
// 		return err
// 	}
// 	if arr, ok := v.(Array); ok {
// 		*a = arr
// 		return nil
// 	}
// 	return fmt.Errorf("cannot unmarshal data into Array: %s", string(data))
// }

// Type Notifies others this is of Array type
func (a Array) Type() Type { return TypeArray }

// Len returns the length of the array
func (a Array) Len() int { return len(a) }

// Index gives the value of a slice index
func (a Array) Index(i int) Value { return a[i] }

// Keys of Array will always panic
func (a Array) Keys() []string {
	panic(&ValueError{"Keys", TypeArray})
	return nil
}

// MapIndex of Array will always Panic
func (a Array) MapIndex(key string) Value {
	panic(&ValueError{"MapIndex", TypeArray})
	return nil
}

// Boolean of Array will always panic
func (a Array) Boolean() bool {
	panic(&ValueError{"Boolean", TypeArray})
	return false
}

// String of Array must satisfy the stringer interface, but output is intentionally obfuscated
func (a Array) String() string {
	return fmt.Sprintf("<%s>", TypeArray.String())
}

// Integer of Array will always panic
func (a Array) Integer() int {
	panic(&ValueError{"Int", TypeArray})
	return 0
}

// Number of Array will always panic
func (a Array) Number() float64 {
	panic(&ValueError{"Number", TypeArray})
	return 0
}

// IsNull of Array always returns false
func (a Array) IsNull() bool { return false }

// String represents a string value
type String string

// // UnmarshalJSON implements the json.Unmarshaller interface for String
// func (s *String) UnmarshalJSON(data []byte) error {
// 	v, err := UnmarshalJSON(data)
// 	if err != nil {
// 		return err
// 	}
// 	if str, ok := v.(String); ok {
// 		*s = str
// 		return nil
// 	}
// 	return fmt.Errorf("cannot unmarshal data into String: %s", string(data))
// }

// Type declares this value is of String type
func (s String) Type() Type { return TypeString }

// Len of String will always panic
func (s String) Len() int {
	panic(&ValueError{"Len", TypeString})
	return 0
}

// Index of String will always panic
func (s String) Index(i int) Value {
	panic(&ValueError{"Index", TypeString})
	return nil
}

// Keys of String will always panic
func (s String) Keys() []string {
	panic(&ValueError{"Keys", TypeString})
	return nil
}

// MapIndex of String will always Panic
func (s String) MapIndex(key string) Value {
	panic(&ValueError{"MapIndex", TypeString})
	return nil
}

// Boolean of String will always panic
func (s String) Boolean() bool {
	panic(&ValueError{"Boolean", TypeString})
	return false
}

// String returns String as a string
// Say string one more time. I dare you.
// ...
// string
func (s String) String() string {
	return string(s)
}

// Integer of String will always panic
func (s String) Integer() int {
	panic(&ValueError{"Int", TypeString})
	return 0
}

// Number of String will always panic
func (s String) Number() float64 {
	panic(&ValueError{"Number", TypeString})
	return 0
}

// IsNull of String always returns false
func (s String) IsNull() bool { return false }

// Integer represents a whole number
type Integer int

// // UnmarshalJSON implements the json.Unmarshaller interface for Integer
// func (i *Integer) UnmarshalJSON(data []byte) error {
// 	v, err := UnmarshalJSON(data)
// 	if err != nil {
// 		return err
// 	}
// 	if in, ok := v.(Integer); ok {
// 		*i = in
// 		return nil
// 	}
// 	return fmt.Errorf("cannot unmarshal data into Integer: %s", string(data))
// }

// Type declares this value is of Integer type
func (i Integer) Type() Type { return TypeInteger }

// Len of Integer will always panic
func (i Integer) Len() int {
	panic(&ValueError{"Len", TypeInteger})
	return 0
}

// Index of Integer will always panic
func (i Integer) Index(j int) Value {
	panic(&ValueError{"Index", TypeInteger})
	return nil
}

// Keys of Integer will always panic
func (i Integer) Keys() []string {
	panic(&ValueError{"Keys", TypeInteger})
	return nil
}

// MapIndex of Integer will always Panic
func (i Integer) MapIndex(key string) Value {
	panic(&ValueError{"MapIndex", TypeInteger})
	return nil
}

// Boolean of Integer will always panic
func (i Integer) Boolean() bool {
	panic(&ValueError{"Boolean", TypeInteger})
	return false
}

// String of Integer must satisfy the stringer interface, but output is intentionally obfuscated
func (i Integer) String() string {
	return fmt.Sprintf("<%s>", TypeInteger.String())
}

// Integer returns this number formatted as an int
func (i Integer) Integer() int {
	return int(i)
}

// Number of Integer returns int formatted as a float64
func (i Integer) Number() float64 {
	return float64(i)
}

// IsNull of Integer always returns false
func (i Integer) IsNull() bool { return false }

// Number represents a floating point number
type Number float64

// // UnmarshalJSON implements the json.Unmarshaller interface for Number
// func (n *Number) UnmarshalJSON(data []byte) error {
// 	v, err := UnmarshalJSON(data)
// 	if err != nil {
// 		return err
// 	}
// 	if num, ok := v.(Number); ok {
// 		*n = num
// 		return nil
// 	}
// 	return fmt.Errorf("cannot unmarshal data into Number: %s", string(data))
// }

// Type declares this value is of Number type
func (n Number) Type() Type { return TypeNumber }

// Len of Number will always panic
func (n Number) Len() int {
	panic(&ValueError{"Len", TypeNumber})
	return 0
}

// Index of Number will always panic
func (n Number) Index(i int) Value {
	panic(&ValueError{"Index", TypeNumber})
	return nil
}

// Keys of Number will always panic
func (n Number) Keys() []string {
	panic(&ValueError{"Keys", TypeNumber})
	return nil
}

// MapIndex of Number will always Panic
func (n Number) MapIndex(key string) Value {
	panic(&ValueError{"MapIndex", TypeNumber})
	return nil
}

// Boolean of Number will always panic
func (n Number) Boolean() bool {
	panic(&ValueError{"Boolean", TypeNumber})
	return false
}

// String of Number must satisfy the stringer interface, but output is intentionally obfuscated
func (n Number) String() string {
	return fmt.Sprintf("<%s>", TypeNumber.String())
}

// Integer of Number will always panic
// TODO - should we allow this? rounding is nice. maybe.
func (n Number) Integer() int {
	panic(&ValueError{"Int", TypeNumber})
	return 0
}

// Number of Number will always panic
func (n Number) Number() float64 {
	return float64(n)
}

// IsNull of Number always returns false
func (n Number) IsNull() bool { return false }

// Boolean represents true/false values
type Boolean bool

// // UnmarshalJSON implements the json.Unmarshaller interface for Array
// func (b *Boolean) UnmarshalJSON(data []byte) error {
// 	v, err := UnmarshalJSON(data)
// 	if err != nil {
// 		return err
// 	}
// 	if bol, ok := v.(Boolean); ok {
// 		*b = bol
// 		return nil
// 	}
// 	return fmt.Errorf("cannot unmarshal data into Boolean: %s", string(data))
// }

// Type declares this value is of Boolean type
func (b Boolean) Type() Type { return TypeBoolean }

// Len of Boolean will always panic
func (b Boolean) Len() int {
	panic(&ValueError{"Len", TypeBoolean})
	return 0
}

// Index of Boolean will always panic
func (b Boolean) Index(i int) Value {
	panic(&ValueError{"Index", TypeBoolean})
	return nil
}

// Keys of Boolean will always panic
func (b Boolean) Keys() []string {
	panic(&ValueError{"Keys", TypeBoolean})
	return nil
}

// MapIndex of Boolean will always Panic
func (b Boolean) MapIndex(key string) Value {
	panic(&ValueError{"MapIndex", TypeBoolean})
	return nil
}

// Boolean of Boolean will always panic
func (b Boolean) Boolean() bool {
	return bool(b)
}

// String of Boolean must satisfy the stringer interface, but output is intentionally obfuscated
func (b Boolean) String() string {
	return fmt.Sprintf("<%s %t>", TypeBoolean.String(), bool(b))
}

// Integer of Boolean will always panic
func (b Boolean) Integer() int {
	panic(&ValueError{"Int", TypeBoolean})
	return 0
}

// Number of Boolean will always panic
func (b Boolean) Number() float64 {
	panic(&ValueError{"Number", TypeBoolean})
	return 0
}

// IsNull of Boolean always returns false
func (b Boolean) IsNull() bool { return false }

// Null represents explicit null values
type Null bool

// // UnmarshalJSON implements the json.Unmarshaller interface for Array
// func (n *Null) UnmarshalJSON(data []byte) error {
// 	v, err := UnmarshalJSON(data)
// 	if err != nil {
// 		return err
// 	}
// 	if nul, ok := v.(Null); ok {
// 		*n = nul
// 		return nil
// 	}
// 	return fmt.Errorf("cannot unmarshal data into Null: %s", string(data))
// }

// MarshalJSON implements the json.Marshaler interface for Null
func (n Null) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

// Type declares this value is of Null type
func (n Null) Type() Type { return TypeNull }

// Len of Null will always panic
func (n Null) Len() int {
	panic(&ValueError{"Len", TypeNull})
	return 0
}

// Index of Null will always panic
func (n Null) Index(i int) Value {
	panic(&ValueError{"Index", TypeNull})
	return nil
}

// Keys of Null will always panic
func (n Null) Keys() []string {
	panic(&ValueError{"Keys", TypeNull})
	return nil
}

// MapIndex of Null will always Panic
func (n Null) MapIndex(key string) Value {
	panic(&ValueError{"MapIndex", TypeNull})
	return nil
}

// Boolean of Null will always panic
func (n Null) Boolean() bool {
	panic(&ValueError{"Boolean", TypeNull})
	return false
}

// String of Null must satisfy the stringer interface, but output is intentionally obfuscated
func (n Null) String() string {
	return fmt.Sprintf("<%s>", TypeNull.String())
}

// Integer of Null will always panic
func (n Null) Integer() int {
	panic(&ValueError{"Int", TypeNull})
	return 0
}

// Number of Null will always panic
func (n Null) Number() float64 {
	panic(&ValueError{"Number", TypeNull})
	return 0
}

// IsNull of Null always returns true
func (n Null) IsNull() bool { return true }
