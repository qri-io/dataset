package vals

// ObjectValue is a special value that represents a value in the context of a parent object
// It wraps a value, adding a property "Key" that holds the value's key in the parent object
type ObjectValue struct {
	Key string
	Value
}

// NewObjectValue allocates a new Object Value
func NewObjectValue(key string, v Value) Value {
	return ObjectValue{key, v}
}
