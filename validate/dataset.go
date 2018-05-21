package validate

import (
	"fmt"

	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

// Dataset checks that a dataset is valid for use
// returning the first error encountered, nil if valid
func Dataset(ds *dataset.Dataset) error {
	if ds == nil {
		return nil
	}

	// if ds.Abstract != nil {
	// 	if err := dataset.CompareDatasets(ds.Abstract, dataset.Abstract(ds)); err != nil {
	// 		return fmt.Errorf("abstract field is not an abstract dataset. %s", err.Error())
	// 	}
	// }

	if ds.Commit == nil {
		err := fmt.Errorf("commit is required")
		log.Debug(err.Error())
		return err
	} else if err := Commit(ds.Commit); err != nil {
		err := fmt.Errorf("commit: %s", err.Error())
		log.Debug(err.Error())
		return err
	}
	if ds.Structure == nil {
		err := fmt.Errorf("structure is required")
		log.Debug(err.Error())
		return err
	} else if err := Structure(ds.Structure); err != nil {
		return fmt.Errorf("structure: %s", err.Error())
	}

	return nil
}

// Commit checks that a dataset Commit is valid for use
// returning the first error encountered, nil if valid
func Commit(cm *dataset.Commit) error {
	if cm == nil {
		return nil
	}

	if cm.Title == "" {
		// return fmt.Errorf("title is required")

	} else if len(cm.Title) > 100 {
		return fmt.Errorf("title is too long. %d length exceeds 100 character limit", len(cm.Title))
	}

	return nil
}

// Structure checks that a dataset structure is valid for use
// returning the first error encountered, nil if valid
func Structure(s *dataset.Structure) error {
	if s == nil {
		return nil
	}

	if s.Format == dataset.UnknownDataFormat {
		return fmt.Errorf("format is required")
	} else if s.Format == dataset.CSVDataFormat {
		if s.Schema == nil {
			return fmt.Errorf("csv data format requires a schema")
		}
	}

	if err := Schema(s.Schema); err != nil {
		return fmt.Errorf("schema: %s", err.Error())
	}

	return nil
}

// csvMetaSchema is a jsonschema for validating CSV schema definitions
var csvMetaSchema = jsonschema.Must(`{
  "type": "object",
  "properties": {
    "type": {
      "const": "array"
    },
    "items": {
      "type": "object",
      "properties": {
        "type": {
          "const": "array"
        },
        "items": {
          "type": "array",
          "items": {
            "type": "object",
            "minItems": 1,
            "properties": {
              "title": {
                "type": "string"
              },
              "type": true
            }
          }
        }
      }
    }
  }
}`)

// jsonMetaSchema is a jsonschema for validating JSON schema definitions
// var jsonMetaSchema = jsonschema.Must(``)

// Schema checks that a dataset schema is valid for use
// returning the first error encountered, nil if valid
func Schema(sch *jsonschema.RootSchema) error {
	if sch == nil {
		return fmt.Errorf("schema is required")
	}

	// if len(s.Fields) == 0 {
	// 	return fmt.Errorf("fields are required")
	// } else if err := Fields(s.Fields); err != nil {
	// 	return fmt.Errorf("fields: %s", err.Error())
	// }

	return nil
}

// Fields checks that a slice of dataset fields is valid for use
// returning the first error encountered, nil if valid
// func Fields(fields []*dataset.Field) error {
// 	if fields == nil {
// 		return nil
// 	}

// 	checkedFieldNames := map[string]bool{}
// 	for _, field := range fields {
// 		if err := ValidName(field.Name); err != nil {
// 			return err
// 		}
// 		seen := checkedFieldNames[field.Name]
// 		if seen {
// 			return fmt.Errorf("error: cannot use the same name, '%s' more than once", field.Name)
// 		}
// 		checkedFieldNames[field.Name] = true
// 	}
// 	return nil
// }
