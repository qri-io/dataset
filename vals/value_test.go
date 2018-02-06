package vals

import (
	"testing"
)

func ExecValueMethod(v Value, methodName string) {
	switch methodName {
	case "Type":
		v.Type()
	case "Len":
		v.Len()
	case "Index":
		v.Index(0)
	case "Keys":
		v.Keys()
	case "MapIndex":
		v.MapIndex("abc")
	case "Boolean":
		v.Boolean()
	case "String":
		v.String()
	case "Integer":
		v.Integer()
	case "Number":
		v.Number()
	case "IsNull":
		v.IsNull()
	}
}

func TestNumber(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		// {"Type", "data: call of Type on number Value"},
		{"Len", "data: call of Len on number Value"},
		{"Index", "data: call of Index on number Value"},
		{"Keys", "data: call of Keys on number Value"},
		{"MapIndex", "data: call of MapIndex on number Value"},
		{"Boolean", "data: call of Boolean on number Value"},
		// {"String", "data: call of String on number Value"},
		{"Integer", "data: call of Integer on number Value"},
		// {"Number", "data: call of Integer on number Value"},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err.Error() != c.expectedErr {
							t.Errorf("case %d error mismatch. expected: '%s', got '%s'", i, c.expectedErr, err.Error())
						}
					} else {
						t.Errorf("%s paniced with a non-error", c.methodName)
					}
				} else {
					t.Errorf("expected invalid call to %s to panic", c.methodName)
				}
			}()
			var num Number = 33.333
			ExecValueMethod(num, c.methodName)
		}()
	}
}

func TestInteger(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		// {"Type", "data: call of Type on integer Value"},
		{"Len", "data: call of Len on integer Value"},
		{"Index", "data: call of Index on integer Value"},
		{"Keys", "data: call of Keys on integer Value"},
		{"MapIndex", "data: call of MapIndex on integer Value"},
		{"Boolean", "data: call of Boolean on integer Value"},
		// {"String", "data: call of String on integer Value"},
		// {"Integer", "data: call of Integer on integer Value"},
		// {"Number", "data: call of Integer on integer Value"},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err.Error() != c.expectedErr {
							t.Errorf("case %d error mismatch. expected: '%s', got '%s'", i, c.expectedErr, err.Error())
						}
					} else {
						t.Errorf("%s paniced with a non-error", c.methodName)
					}
				} else {
					t.Errorf("expected invalid call to %s to panic", c.methodName)
				}
			}()
			var testInt Integer = 42
			ExecValueMethod(testInt, c.methodName)
		}()
	}
}

func TestBoolean(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		// {"Type", "data: call of Type on boolean Value"},
		{"Len", "data: call of Len on boolean Value"},
		{"Index", "data: call of Index on boolean Value"},
		{"Keys", "data: call of Keys on boolean Value"},
		{"MapIndex", "data: call of MapIndex on boolean Value"},
		// {"Boolean", "data: call of Boolean on boolean Value"},
		// {"String", "data: call of String on boolean Value"},
		{"Integer", "data: call of Integer on boolean Value"},
		{"Number", "data: call of Number on boolean Value"},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err.Error() != c.expectedErr {
							t.Errorf("case %d error mismatch. expected: '%s', got '%s'", i, c.expectedErr, err.Error())
						}
					} else {
						t.Errorf("%s paniced with a non-error", c.methodName)
					}
				} else {
					t.Errorf("expected invalid call to %s to panic", c.methodName)
				}
			}()
			var testBool Boolean = true
			ExecValueMethod(testBool, c.methodName)
		}()
	}
}

func TestString(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		// {"Type", "data: call of Type on string Value"},
		{"Len", "data: call of Len on string Value"},
		{"Index", "data: call of Index on string Value"},
		{"Keys", "data: call of Keys on string Value"},
		{"MapIndex", "data: call of MapIndex on string Value"},
		{"Boolean", "data: call of Boolean on string Value"},
		// {"String", "data: call of String on string Value"},
		{"Integer", "data: call of Integer on string Value"},
		{"Number", "data: call of Number on string Value"},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err.Error() != c.expectedErr {
							t.Errorf("case %d error mismatch. expected: '%s', got '%s'", i, c.expectedErr, err.Error())
						}
					} else {
						t.Errorf("%s paniced with a non-error", c.methodName)
					}
				} else {
					t.Errorf("expected invalid call to %s to panic", c.methodName)
				}
			}()
			var s String = "qriqriqri"
			ExecValueMethod(s, c.methodName)
		}()
	}
}

func TestArray(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		// {"Type", "data: call of Type on array Value"},
		// {"Len", "data: call of Len on array Value"},
		// {"Index", "data: call of Index on array Value"},
		{"Keys", "data: call of Keys on array Value"},
		{"MapIndex", "data: call of MapIndex on array Value"},
		{"Boolean", "data: call of Boolean on array Value"},
		// {"String", "data: call of String on array Value"},
		{"Integer", "data: call of Integer on array Value"},
		{"Number", "data: call of Number on array Value"},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err.Error() != c.expectedErr {
							t.Errorf("case %d error mismatch. expected: '%s', got '%s'", i, c.expectedErr, err.Error())
						}
					} else {
						t.Errorf("%s paniced with a non-error", c.methodName)
					}
				} else {
					t.Errorf("expected invalid call to %s to panic", c.methodName)
				}
			}()
			var num1 Number = 99.99
			var num2 Number = 98.89
			arr := &Array{num1, num2}
			ExecValueMethod(*arr, c.methodName)
		}()
	}
}

func TestObject(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		// {"Type", "data: call of Type on object Value"},
		{"Len", "data: call of Len on object Value"},
		{"Index", "data: call of Index on object Value"},
		// {"Keys", "data: call of Keys on object Value"},
		// {"MapIndex", "data: call of MapIndex on object Value"},
		{"Boolean", "data: call of Boolean on object Value"},
		// {"String", "data: call of String on object Value"},
		{"Integer", "data: call of Integer on object Value"},
		{"Number", "data: call of Number on object Value"},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err.Error() != c.expectedErr {
							t.Errorf("case %d error mismatch. expected: '%s', got '%s'", i, c.expectedErr, err.Error())
						}
					} else {
						t.Errorf("%s paniced with a non-error", c.methodName)
					}
				} else {
					t.Errorf("expected invalid call to %s to panic", c.methodName)
				}
			}()
			// var num1 Number = 99.99
			// var num2 Number = 98.89
			// msv := make(map[string]Number)
			obj := &Object{}
			// msv["num1"] = num1
			// *obj = Object(msv)
			ExecValueMethod(*obj, c.methodName)
		}()
	}
}

func TestNull(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		// {"Type", "data: call of Type on null Value"},
		{"Len", "data: call of Len on null Value"},
		{"Index", "data: call of Index on null Value"},
		{"Keys", "data: call of Keys on null Value"},
		{"MapIndex", "data: call of MapIndex on null Value"},
		{"Boolean", "data: call of Boolean on null Value"},
		// {"String", "data: call of String on null Value"},
		{"Integer", "data: call of Integer on null Value"},
		{"Number", "data: call of Number on null Value"},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err.Error() != c.expectedErr {
							t.Errorf("case %d error mismatch. expected: '%s', got '%s'", i, c.expectedErr, err.Error())
						}
					} else {
						t.Errorf("%s paniced with a non-error", c.methodName)
					}
				} else {
					t.Errorf("expected invalid call to %s to panic", c.methodName)
				}
			}()
			var nullVal Null = Null(true)
			ExecValueMethod(nullVal, c.methodName)
		}()
	}
}
