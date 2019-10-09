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
		// TODO (b5) - go vet is upset about this:
		// v.String()
	case "Integer":
		v.Integer()
	case "Number":
		v.Number()
	case "IsNull":
		v.IsNull()
	}
}

func TestNumberPanic(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		{"Len", "data: call of Len on number Value"},
		{"Index", "data: call of Index on number Value"},
		{"Keys", "data: call of Keys on number Value"},
		{"MapIndex", "data: call of MapIndex on number Value"},
		{"Boolean", "data: call of Boolean on number Value"},
		{"Integer", "data: call of Integer on number Value"},
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
			var testNum Number = 33.333
			ExecValueMethod(testNum, c.methodName)
		}()
	}
}

func TestIntegerPanic(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		{"Len", "data: call of Len on integer Value"},
		{"Index", "data: call of Index on integer Value"},
		{"Keys", "data: call of Keys on integer Value"},
		{"MapIndex", "data: call of MapIndex on integer Value"},
		{"Boolean", "data: call of Boolean on integer Value"},
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

func TestBooleanPanic(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		{"Len", "data: call of Len on boolean Value"},
		{"Index", "data: call of Index on boolean Value"},
		{"Keys", "data: call of Keys on boolean Value"},
		{"MapIndex", "data: call of MapIndex on boolean Value"},
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

func TestStringPanic(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		{"Len", "data: call of Len on string Value"},
		{"Index", "data: call of Index on string Value"},
		{"Keys", "data: call of Keys on string Value"},
		{"MapIndex", "data: call of MapIndex on string Value"},
		{"Boolean", "data: call of Boolean on string Value"},
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
			var testString String = "qriqriqri"
			ExecValueMethod(testString, c.methodName)
		}()
	}
}

func TestArrayPanic(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		// {"Len", "data: call of Len on array Value"},
		// {"Index", "data: call of Index on array Value"},
		{"Keys", "data: call of Keys on array Value"},
		{"MapIndex", "data: call of MapIndex on array Value"},
		{"Boolean", "data: call of Boolean on array Value"},
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
			testArr := &Array{num1, num2}
			ExecValueMethod(*testArr, c.methodName)
		}()
	}
}

func TestObjectPanic(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		{"Len", "data: call of Len on object Value"},
		{"Index", "data: call of Index on object Value"},
		// {"Keys", "data: call of Keys on object Value"},
		// {"MapIndex", "data: call of MapIndex on object Value"},
		{"Boolean", "data: call of Boolean on object Value"},
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
			testObj := &Object{}

			ExecValueMethod(*testObj, c.methodName)
		}()
	}
}

func TestNullPanic(t *testing.T) {
	cases := []struct {
		methodName  string
		expectedErr string
	}{
		{"Len", "data: call of Len on null Value"},
		{"Index", "data: call of Index on null Value"},
		{"Keys", "data: call of Keys on null Value"},
		{"MapIndex", "data: call of MapIndex on null Value"},
		{"Boolean", "data: call of Boolean on null Value"},
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
			testNull := Null(true)
			ExecValueMethod(testNull, c.methodName)
		}()
	}
}

func TestTypeMethods(t *testing.T) {
	var testNum Number = 33.333
	var testInt Integer = 42
	var testBool Boolean = true
	var testArr Array
	var testString String = "qriqriqri"
	var testObj Object
	testNull := Null(true)
	cases := []struct {
		val          Value
		expectedType Type
	}{
		{testNum, TypeNumber},
		{testInt, TypeInteger},
		{testBool, TypeBoolean},
		{testArr, TypeArray},
		{testString, TypeString},
		{testObj, TypeObject},
		{testNull, TypeNull},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err != nil {
							t.Errorf("case %d error mismatch: valid function should not have paniced", i)
						}
					}
				}
			}()
			got := c.val.Type()
			if got != c.expectedType {
				t.Errorf("case %d response mismatch: expected: '%s', got: '%s'", i, c.expectedType, got)
			}
		}()
	}
}

func TestStringMethods(t *testing.T) {
	var testNum Number = 33.333
	var testInt Integer = 42
	var testBool Boolean = true
	var testArr Array
	var testString String = "qriqriqri"
	var testObj Object
	testNull := Null(true)
	cases := []struct {
		val            Value
		expectedOutput string
	}{
		{testNum, "<number>"},
		{testInt, "<integer>"},
		{testBool, "<boolean true>"},
		{testArr, "<array>"},
		{testString, "qriqriqri"},
		{testObj, "<object 0 keys>"},
		{testNull, "<null>"},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err != nil {
							t.Errorf("case %d error mismatch: valid function should not have paniced", i)
						}
					}
				}
			}()
			got := c.val.String()
			if got != c.expectedOutput {
				t.Errorf("case %d response mismatch: expected: '%s', got: '%s'", i, c.expectedOutput, got)
			}
		}()
	}
}

func TestIsNullMethods(t *testing.T) {
	var testNum Number = 33.333
	var testInt Integer = 42
	var testBool Boolean = true
	var testArr Array
	var testString String = "qriqriqri"
	var testObj Object
	testNull := Null(true)
	cases := []struct {
		val            Value
		expectedOutput bool
	}{
		{testNum, false},
		{testInt, false},
		{testBool, false},
		{testArr, false},
		{testString, false},
		{testObj, false},
		{testNull, true},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err != nil {
							t.Errorf("case %d error mismatch: valid function should not have paniced", i)
						}
					}
				}
			}()
			got := c.val.IsNull()
			if got != c.expectedOutput {
				t.Errorf("case %d response mismatch: expected: '%t', got: '%t'", i, c.expectedOutput, got)
			}
		}()
	}
}

func TestNumberMethods(t *testing.T) {
	var testNum Number = 33.333
	var testInt Integer = 42
	cases := []struct {
		val            Value
		expectedOutput float64
	}{
		{testNum, 33.333},
		{testInt, 42.0},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err != nil {
							t.Errorf("case %d error mismatch: valid function should not have paniced", i)
						}
					}
				}
			}()
			got := c.val.Number()
			if got != c.expectedOutput {
				t.Errorf("case %d response mismatch: expected: '%v, got: '%v'", i, c.expectedOutput, got)
			}
		}()
	}
}

func TestIntegerMethod(t *testing.T) {
	var testInt Integer = 42
	cases := []struct {
		val            Value
		expectedOutput int
	}{
		{testInt, 42},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err != nil {
							t.Errorf("case %d error mismatch: valid function should not have paniced", i)
						}
					}
				}
			}()
			got := c.val.Integer()
			if got != c.expectedOutput {
				t.Errorf("case %d response mismatch: expected: '%v, got: '%v'", i, c.expectedOutput, got)
			}
		}()
	}
}

func TestBooleanMethod(t *testing.T) {
	var testBool Boolean = true
	cases := []struct {
		val            Value
		expectedOutput bool
	}{
		{testBool, true},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err != nil {
							t.Errorf("case %d error mismatch: valid function should not have paniced", i)
						}
					}
				}
			}()
			got := c.val.Boolean()
			if got != c.expectedOutput {
				t.Errorf("case %d response mismatch: expected: '%v, got: '%v'", i, c.expectedOutput, got)
			}
		}()
	}
}

func TestArrayIndexMethod(t *testing.T) {
	var num1 Number = 99.99
	var num2 Number = 98.89
	testArr1 := &Array{num1, num2}
	testArr2 := &Array{}
	cases := []struct {
		val            Value
		index          int
		expectedOutput Value
		expectedError  string
	}{
		{*testArr1, 0, num1, ""},
		{*testArr1, 1, num2, ""},
		{*testArr1, 2, num2, "runtime error: index out of range [2] with length 2"},
		{*testArr2, 0, num2, "runtime error: index out of range [0] with length 0"},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err != nil {
							if err.Error() != c.expectedError {
								t.Errorf("case %d error mismatch: expected: '%s', got: '%s'", i, c.expectedError, err.Error())
							}
						}
					}
				}
			}()
			got := c.val.Index(c.index)
			if got != c.expectedOutput {
				t.Errorf("case %d response mismatch: expected: '%v, got: '%v'", i, c.expectedOutput, got)
			}
		}()
	}
}

func TestArrayLenMethod(t *testing.T) {
	var num1 Number = 99.99
	var num2 Number = 98.89
	testArr1 := &Array{num1, num2}
	testArr2 := &Array{}
	cases := []struct {
		val            Value
		expectedOutput int
		expectedError  string
	}{
		{*testArr1, 2, ""},
		{*testArr2, 0, ""},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err != nil {
							if err.Error() != c.expectedError {
								t.Errorf("case %d error mismatch: expected: '%s', got: '%s'", i, c.expectedError, err.Error())
							}
						}
					}
				}
			}()
			got := c.val.Len()
			if got != c.expectedOutput {
				t.Errorf("case %d response mismatch: expected: '%v, got: '%v'", i, c.expectedOutput, got)
			}
		}()
	}
}

// This test is not super useful but can't think of another way to
// cover this func 100% at this time
func TestValueErrorError(t *testing.T) {
	e := &ValueError{"abc", TypeUnknown}
	cases := []struct {
		input    *ValueError
		expected string
	}{
		{e, "data: call of abc on Unknown Type"},
	}
	for i, c := range cases {
		got := c.input.Error()
		if got != c.expected {
			t.Errorf("case %d response mismatch: expected: '%s, got: '%s'", i, c.expected, got)
		}
	}
}

func TestObjectMapIndexMethod(t *testing.T) {
	var num1 Number = 99.99
	var num2 Number = 98.89
	// msn := map[string]Number{"a": num1, "b": num2}
	// testObj1 := Object(msn)
	testObj1 := Object{"a": num1, "b": num2}
	cases := []struct {
		val            Value
		key            string
		expectedOutput Value
		expectedError  string
	}{
		{testObj1, "a", num1, ""},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err != nil {
							if err.Error() != c.expectedError {
								t.Errorf("case %d error mismatch: expected: '%s', got: '%s'", i, c.expectedError, err.Error())
							}
						}
					}
				}
			}()
			got := c.val.MapIndex(c.key)
			if got != c.expectedOutput {
				t.Errorf("case %d response mismatch: expected: '%s, got: '%s'", i, c.expectedOutput, got)
			}
		}()
	}
}

func TestObjectKeysMethod(t *testing.T) {
	var num1 Number = 99.99
	testObj1 := Object{"a": num1}
	cases := []struct {
		val            Value
		expectedOutput []string
		expectedError  string
	}{
		{testObj1, []string{"a"}, ""},
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						if err != nil {
							if err.Error() != c.expectedError {
								t.Errorf("case %d error mismatch: expected: '%s', got: '%s'", i, c.expectedError, err.Error())
							}
						}
					}
				}
			}()
			got := c.val.Keys()

			if got[0] != c.expectedOutput[0] {
				t.Errorf("case %d response mismatch: expected: '%s, got: '%s'", i, c.expectedOutput, got)
			}
		}()
	}
}
