package dataset

import (
	"fmt"
)

func CompareFormatConfigs(a, b FormatConfig) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil || a != nil && b == nil {
		return fmt.Errorf("FormatConfig mismatch: %s != %s", a, b)
	}

	if a.Format() != b.Format() {
		return fmt.Errorf("FormatConfig mistmatch %s != %s", a.Format(), b.Format())
	}

	// TODO - exhaustive check

	return nil
}
