package dataset

import (
	"fmt"
	"testing"
)

func TestVariableName(t *testing.T) {
	// cases := []struct{
	// }
}

func CompareLicense(a, b *License) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil || a != nil && b == nil {
		return fmt.Errorf("License mistmatch: %s != %s", a, b)
	}

	if a.Type != b.Type {
		return fmt.Errorf("type mismatch: '%s' != '%s'", a.Type, b.Type)
	}

	return nil
}
