package dataset

import (
	"fmt"
	"github.com/qri-io/jsonschema"
	"reflect"
)

// CompareDatasets checks if all fields of a dataset are equal,
// returning an error on the first, nil if equal
// Note that comparison does not examine the internal path property
func CompareDatasets(a, b *Dataset) error {
	if a == nil && b == nil {
		return nil
	}
	if a == nil && b != nil {
		return fmt.Errorf("nil: <nil> != <not nil>")
	} else if a != nil && b == nil {
		return fmt.Errorf("nil: <not nil> != <nil>")
	}

	if a.Qri.String() != b.Qri.String() {
		return fmt.Errorf("Qri: %s != %s", a.Qri, b.Qri)
	}

	if a.PreviousPath != b.PreviousPath {
		return fmt.Errorf("PreviousPath: %s != %s", a.PreviousPath, b.PreviousPath)
	}
	if a.BodyPath != b.BodyPath {
		return fmt.Errorf("BodyPath: %s != %s", a.BodyPath, b.BodyPath)
	}

	if err := CompareMetas(a.Meta, b.Meta); err != nil {
		return fmt.Errorf("Meta: %s", err.Error())
	}
	if err := CompareStructures(a.Structure, b.Structure); err != nil {
		return fmt.Errorf("Structure: %s", err.Error())
	}
	if err := CompareTransforms(a.Transform, b.Transform); err != nil {
		return fmt.Errorf("Transform: %s", err.Error())
	}
	if err := CompareCommits(a.Commit, b.Commit); err != nil {
		return fmt.Errorf("Commit: %s", err.Error())
	}

	return nil
}

// CompareMetas checks if all fields of a metadata struct are equal,
// returning an error on the first, nil if equal
// Note that comparison does not examine the internal path property
func CompareMetas(a, b *Meta) error {
	if a == nil && b == nil {
		return nil
	}
	if a == nil && b != nil {
		return fmt.Errorf("nil: <nil> != <not nil>")
	} else if a != nil && b == nil {
		return fmt.Errorf("nil: <not nil> != <nil>")
	}

	if a.Qri.String() != b.Qri.String() {
		return fmt.Errorf("Qri: %s != %s", a.Qri, b.Qri)
	}
	if a.Title != b.Title {
		return fmt.Errorf("Title: %s != %s", a.Title, b.Title)
	}
	if a.AccessPath != b.AccessPath {
		return fmt.Errorf("AccessPath: %s != %s", a.AccessPath, b.AccessPath)
	}
	if a.DownloadPath != b.DownloadPath {
		return fmt.Errorf("DownloadPath: %s != %s", a.DownloadPath, b.DownloadPath)
	}
	if a.AccrualPeriodicity != b.AccrualPeriodicity {
		return fmt.Errorf("AccrualPeriodicity: %s != %s", a.AccrualPeriodicity, b.AccrualPeriodicity)
	}
	if a.ReadmePath != b.ReadmePath {
		return fmt.Errorf("ReadmePath: %s != %s", a.ReadmePath, b.ReadmePath)
	}
	if a.Description != b.Description {
		return fmt.Errorf("Description: %s != %s", a.Description, b.Description)
	}
	if a.HomePath != b.HomePath {
		return fmt.Errorf("HomePath: %s != %s", a.HomePath, b.HomePath)
	}
	if a.Identifier != b.Identifier {
		return fmt.Errorf("Identifier: %s != %s", a.Identifier, b.Identifier)
	}
	if err := CompareLicenses(a.License, b.License); err != nil {
		return fmt.Errorf("License: %s", err)
	}
	if a.Version != b.Version {
		return fmt.Errorf("Version: %s != %s", a.Version, b.Version)
	}
	if err := CompareStringSlices(a.Keywords, b.Keywords); err != nil {
		return fmt.Errorf("Keywords: %s", err.Error())
	}
	// if a.Contributors != b.Contributors {
	//  return fmt.Errorf("Contributors: %s != %s", a.Contributors, b.Contributors)
	// }
	if err := CompareStringSlices(a.Language, b.Language); err != nil {
		return fmt.Errorf("Language: %s", err.Error())
	}
	if err := CompareStringSlices(a.Theme, b.Theme); err != nil {
		return fmt.Errorf("Theme: %s", err.Error())
	}

	// TODO - currently we're ignoring abitrary metadata differences
	// if err := compare.MapStringInterface(a.Meta(), b.Meta()); err != nil {
	// 	return fmt.Errorf("meta: %s", err.Error())
	// }
	return nil
}

// CompareStructures checks if all fields of two structure pointers are equal,
// returning an error on the first, nil if equal
// Note that comparison does not examine the internal path property
func CompareStructures(a, b *Structure) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil {
		return fmt.Errorf("nil: <nil> != <not nil>")
	} else if a != nil && b == nil {
		return fmt.Errorf("nil: <not nil> != <nil>")
	}

	if a.Qri != b.Qri {
		return fmt.Errorf("Qri: %s != %s", a.Qri, b.Qri)
	}
	if a.Format != b.Format {
		return fmt.Errorf("Format: %s != %s", a.Format, b.Format)
	}
	if a.Length != b.Length {
		return fmt.Errorf("Length: %d != %d", a.Length, b.Length)
	}
	if a.Checksum != b.Checksum {
		return fmt.Errorf("Checksum: %s != %s", a.Checksum, b.Checksum)
	}
	if a.Entries != b.Entries {
		return fmt.Errorf("Entries: %d != %d", a.Entries, b.Entries)
	}
	if a.Encoding != b.Encoding {
		return fmt.Errorf("Encoding: %s != %s", a.Encoding, b.Encoding)
	}
	if a.Compression != b.Compression {
		return fmt.Errorf("Compression: %s != %s", a.Compression, b.Compression)
	}

	if (a.FormatConfig != nil && b.FormatConfig == nil) || (a.FormatConfig == nil && b.FormatConfig != nil) {
		return fmt.Errorf("FormatConfig nil mismatch")
	} else if a.FormatConfig != nil && b.FormatConfig != nil && !reflect.DeepEqual(a.FormatConfig.Map(), b.FormatConfig.Map()) {
		return fmt.Errorf("FormatConfig mismatch")
	}

	if err := CompareSchemas(a.Schema, b.Schema); err != nil {
		return fmt.Errorf("Schema: %s", err.Error())
	}

	return nil
}

// CompareVisConfigs checks if all fields of two VisConfig pointers are equal,
// returning an error on the first, nil if equal
// Note that comparison does not examine the internal path property
func CompareVisConfigs(a, b *VisConfig) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil {
		return fmt.Errorf("nil: <nil> != <not nil>")
	} else if a != nil && b == nil {
		return fmt.Errorf("nil: <not nil> != <nil>")
	}
	if a.Qri != b.Qri {
		return fmt.Errorf("Qri: %s != %s", a.Qri, b.Qri)
	}
	if a.Format != b.Format {
		return fmt.Errorf("Format: %s != %s", a.Format, b.Format)
	}
	// if a.BodyPath != b.BodyPath {
	// 	return fmt.Errorf("BodyPath: %s != %s", a.BodyPath, b.BodyPath)
	// }
	if !reflect.DeepEqual(a.Visualizations, b.Visualizations) {
		return fmt.Errorf("Visualizations not equal")
	}
	return nil
}

// CompareSchemas checks if all fields of two Schema pointers are equal,
// returning an error on the first, nil if equal
// Note that comparison does not examine the internal path property
func CompareSchemas(a, b *jsonschema.RootSchema) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil {
		return fmt.Errorf("nil: <nil> != <not nil>")
	} else if a != nil && b == nil {
		return fmt.Errorf("nil: <not nil> != <nil>")
	}

	// if err := CompareStringSlices(a.PrimaryKey, b.PrimaryKey); err != nil {
	// 	return fmt.Errorf("PrimaryKey: %s", err.Error())
	// }

	// if a.Fields == nil && b.Fields != nil || a.Fields != nil && b.Fields == nil {
	// 	return fmt.Errorf("Fields: %s != %s", a.Fields, b.Fields)
	// }
	// if a.Fields == nil && b.Fields == nil {
	// 	return nil
	// }
	// if len(a.Fields) != len(b.Fields) {
	// 	return fmt.Errorf("Fields: %d != %d", len(a.Fields), len(b.Fields))
	// }
	// for i, af := range a.Fields {
	// 	bf := b.Fields[i]
	// 	if err := CompareFields(af, bf); err != nil {
	// 		return fmt.Errorf("Fields: element %d: %s", i, err.Error())
	// 	}
	// }

	return nil
}

// CompareCommits checks if all fields of a Commit are equal,
// returning an error on the first, nil if equal
// Note that comparison does not examine the internal path property
func CompareCommits(a, b *Commit) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil {
		return fmt.Errorf("nil: <nil> != <not nil>")
	} else if a != nil && b == nil {
		return fmt.Errorf("nil: <not nil> != <nil>")
	}

	if a.Qri != b.Qri {
		return fmt.Errorf("Qri: %s != %s", a.Qri, b.Qri)
	}
	if a.Title != b.Title {
		return fmt.Errorf("Title: %s != %s", a.Title, b.Title)
	}
	if !a.Timestamp.Equal(b.Timestamp) {
		return fmt.Errorf("Timestamp: %s != %s", a.Timestamp, b.Timestamp)
	}
	if a.Signature != b.Signature {
		return fmt.Errorf("Signature: %s != %s", a.Signature, b.Signature)
	}
	if a.Message != b.Message {
		return fmt.Errorf("Message: %s != %s", a.Message, b.Message)
	}

	return nil
}

// CompareTransforms checks if all fields of two transform pointers are equal,
// returning an error on the first, nil if equal
// Note that comparison does not examine the internal path property
func CompareTransforms(a, b *Transform) error {
	if a == nil && b == nil {
		return nil
	} else if a == nil && b != nil {
		return fmt.Errorf("nil: <nil> != <not nil>")
	} else if a != nil && b == nil {
		return fmt.Errorf("nil: <not nil> != <nil>")
	}

	if a.Qri.String() != b.Qri.String() {
		return fmt.Errorf("Qri: %s != %s", a.Qri, b.Qri)
	}
	if a.Syntax != b.Syntax {
		return fmt.Errorf("Syntax: %s != %s", a.Syntax, b.Syntax)
	}
	if a.SyntaxVersion != b.SyntaxVersion {
		return fmt.Errorf("SyntaxVersion: %s != %s", a.SyntaxVersion, b.SyntaxVersion)
	}
	if a.ScriptPath != b.ScriptPath {
		return fmt.Errorf("ScriptPath: %s != %s", a.ScriptPath, b.ScriptPath)
	}
	if err := CompareStructures(a.Structure, b.Structure); err != nil {
		return fmt.Errorf("Structure: %s", err.Error())
	}
	// TODO - currently not examining config settings
	if a.Resources == nil && b.Resources == nil {
		return nil
	} else if a.Resources == nil && b.Resources != nil || a.Resources != nil && b.Resources == nil {
		return fmt.Errorf("Resources: %v != %v", a.Resources, b.Resources)
	}
	for key, dsa := range a.Resources {
		dsb := b.Resources[key]
		if err := CompareDatasets(dsa, dsb); err != nil {
			return fmt.Errorf("Resource '%s': %s", key, err.Error())
		}
	}

	return nil
}

// CompareLicenses checks if all fields in two License pointers are equal,
// returning an error if unequal
func CompareLicenses(a, b *License) error {
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

// CompareStringSlices confirms two string slices are the same size, contain
// the same values, in the same order
func CompareStringSlices(a, b []string) error {
	if len(a) != len(b) {
		return fmt.Errorf("length: %d != %d", len(a), len(b))
	}
	for i, s := range a {
		if s != b[i] {
			return fmt.Errorf("element %d: %s != %s", i, s, b[i])
		}
	}
	return nil
}
