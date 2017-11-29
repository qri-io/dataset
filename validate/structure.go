package validate

import (
	"fmt"

	"github.com/qri-io/dataset"
)

// Structure checks that a dataset structure is valid for use
// returning the first error encountered, nil if the Structure is valid
func Structure(s *dataset.Structure) error {
	checkedFieldNames := map[string]bool{}
	fields := s.Schema.Fields
	for _, field := range fields {
		if err := ValidName(field.Name); err != nil {
			return err
		}
		seen := checkedFieldNames[field.Name]
		if seen {
			return fmt.Errorf("error: cannot use the same name, '%s' more than once", field.Name)
		}
		checkedFieldNames[field.Name] = true
	}
	return nil
}
