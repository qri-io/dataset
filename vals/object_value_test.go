package vals

import (
	"testing"
)

func TestNewObjectValue(t *testing.T) {
	v := NewObjectValue("foo", String(""))
	if v.Type() != TypeString {
		t.Errorf("type mismatch. expected: %s. got: %s", TypeString, v.Type())
	}

	if ov, ok := v.(ObjectValue); ok {
		if ov.Key != "foo" {
			t.Errorf("key mismatch. expected: %s, got: %s", "foo", ov.Key)
		}
	} else {

		t.Errorf("expected ObjectValue type")
	}
}
