package dataset

import (
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/dataset/compression"
	// "github.com/qri-io/dataset/datatypes"
	"github.com/qri-io/jsonschema"
	"testing"
	"time"
)

func TestCompareDatasets(t *testing.T) {
	cases := []struct {
		a, b *Dataset
		err  string
	}{
		{nil, nil, ""},
		{AirportCodes, AirportCodes, ""},
		{&Dataset{Qri: "a"}, &Dataset{Qri: "b"}, "Qri: a != b"},
		{&Dataset{PreviousPath: "a"}, &Dataset{PreviousPath: "b"}, "PreviousPath: a != b"},
		{&Dataset{BodyPath: "a"}, &Dataset{BodyPath: "b"}, "BodyPath: a != b"},
		{&Dataset{}, &Dataset{Structure: &Structure{}}, "Structure: nil: <nil> != <not nil>"},
		{&Dataset{}, &Dataset{Transform: &Transform{}}, "Transform: nil: <nil> != <not nil>"},
		{&Dataset{}, &Dataset{Commit: &Commit{}}, "Commit: nil: <nil> != <not nil>"},
	}

	for i, c := range cases {
		err := CompareDatasets(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareMetas(t *testing.T) {
	cases := []struct {
		a, b *Meta
		err  string
	}{
		{nil, nil, ""},
		{AirportCodes.Meta, AirportCodes.Meta, ""},
		{&Meta{Qri: "a"}, &Meta{Qri: "b"}, "Qri: a != b"},
		{&Meta{Title: "a"}, &Meta{Title: "b"}, "Title: a != b"},
		{&Meta{AccessPath: "a"}, &Meta{AccessPath: "b"}, "AccessPath: a != b"},
		{&Meta{DownloadPath: "a"}, &Meta{DownloadPath: "b"}, "DownloadPath: a != b"},
		{&Meta{AccrualPeriodicity: "a"}, &Meta{AccrualPeriodicity: "b"}, "AccrualPeriodicity: a != b"},
		{&Meta{ReadmePath: "a"}, &Meta{ReadmePath: "b"}, "ReadmePath: a != b"},
		{&Meta{Description: "a"}, &Meta{Description: "b"}, "Description: a != b"},
		{&Meta{HomePath: "a"}, &Meta{HomePath: "b"}, "HomePath: a != b"},
		{&Meta{Identifier: "a"}, &Meta{Identifier: "b"}, "Identifier: a != b"},
		// TODO
		// {&Meta{License: &License{}}, &Meta{Version: "b"}, "Version: a != b"},
		{&Meta{Version: "a"}, &Meta{Version: "b"}, "Version: a != b"},
		{&Meta{Keywords: []string{"a"}}, &Meta{Keywords: []string{"b"}}, "Keywords: element 0: a != b"},
		{&Meta{Language: []string{"a"}}, &Meta{Language: []string{"b"}}, "Language: element 0: a != b"},
		{&Meta{Theme: []string{"a"}}, &Meta{Theme: []string{"b"}}, "Theme: element 0: a != b"},
	}

	for i, c := range cases {
		err := CompareMetas(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareStructures(t *testing.T) {
	cases := []struct {
		a, b *Structure
		err  string
	}{
		{nil, nil, ""},
		{AirportCodes.Structure, AirportCodes.Structure, ""},
		{nil, AirportCodes.Structure, "nil: <nil> != <not nil>"},
		{AirportCodes.Structure, nil, "nil: <not nil> != <nil>"},
		{&Structure{Qri: "a"}, &Structure{Qri: "b"}, "Qri: a != b"},
		{&Structure{Length: 0}, &Structure{Length: 1}, "Length: 0 != 1"},
		{&Structure{Entries: 0}, &Structure{Entries: 1}, "Entries: 0 != 1"},
		{&Structure{Checksum: "a"}, &Structure{Checksum: "b"}, "Checksum: a != b"},
		{&Structure{Format: CSVDataFormat}, &Structure{Format: UnknownDataFormat}, "Format: csv != "},
		{&Structure{Encoding: "a"}, &Structure{Encoding: "b"}, "Encoding: a != b"},
		{&Structure{Compression: compression.None}, &Structure{Compression: compression.Tar}, "Compression:  != tar"},
		{&Structure{}, &Structure{Schema: &jsonschema.RootSchema{}}, "Schema: nil: <nil> != <not nil>"},
	}

	for i, c := range cases {
		err := CompareStructures(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareVisConfigs(t *testing.T) {
	cases := []struct {
		a, b *VisConfig
		err  string
	}{
		{nil, nil, ""},
		{&VisConfig{Qri: "a", Format: "b", Visualizations: []interface{}{1, 2, 3}}, &VisConfig{Qri: "a", Format: "b", Visualizations: []interface{}{1, 2, 3}}, ""},
		{&VisConfig{}, nil, "nil: <not nil> != <nil>"},
		{nil, &VisConfig{}, "nil: <nil> != <not nil>"},
		{&VisConfig{Qri: "a"}, &VisConfig{Qri: "b"}, "Qri: a != b"},
		{&VisConfig{Format: "a"}, &VisConfig{Format: "b"}, "Format: a != b"},
		{&VisConfig{Visualizations: []interface{}{"hey", "sup"}}, &VisConfig{Visualizations: "test"}, "Visualizations not equal"},
		{&VisConfig{Visualizations: []interface{}{}}, &VisConfig{Visualizations: []interface{}{}}, ""},
	}

	for i, c := range cases {
		err := CompareVisConfigs(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareCommits(t *testing.T) {
	c1 := &Commit{
		path:    datastore.NewKey("/foo"),
		Title:   "foo",
		Message: "message",
		Qri:     KindCommit,
		Author:  &User{ID: "foo"},
	}

	cases := []struct {
		a, b *Commit
		err  string
	}{
		{nil, nil, ""},
		{c1, c1, ""},
		{c1, nil, "nil: <not nil> != <nil>"},
		{nil, c1, "nil: <nil> != <not nil>"},
		{&Commit{}, &Commit{}, ""},
		{
			&Commit{Timestamp: time.Date(2001, 01, 01, 01, 0, 0, 0, time.UTC)},
			&Commit{Timestamp: time.Date(2002, 01, 01, 01, 0, 0, 0, time.UTC)},
			"Timestamp: 2001-01-01 01:00:00 +0000 UTC != 2002-01-01 01:00:00 +0000 UTC",
		},
		{&Commit{Title: "a"}, &Commit{Title: "b"}, "Title: a != b"},
		{&Commit{Message: "a"}, &Commit{Message: "b"}, "Message: a != b"},
		{&Commit{Qri: "a"}, &Commit{Qri: "b"}, "Qri: a != b"},
		{&Commit{Signature: "a"}, &Commit{Signature: "b"}, "Signature: a != b"},
	}

	for i, c := range cases {
		err := CompareCommits(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareTransforms(t *testing.T) {
	t1 := &Transform{
		Qri:           KindTransform,
		Syntax:        "skylark",
		SyntaxVersion: "1000.0.0",
		ScriptPath:    "foo.sky",
		Structure:     AirportCodes.Structure,
		Resources: map[string]*Dataset{
			"airports": AirportCodes,
		},
	}
	cases := []struct {
		a, b *Transform
		err  string
	}{
		{nil, nil, ""},
		{t1, t1, ""},
		{t1, nil, "nil: <not nil> != <nil>"},
		{nil, t1, "nil: <nil> != <not nil>"},
		{&Transform{}, &Transform{}, ""},
		{&Transform{Qri: "a"}, &Transform{Qri: "b"}, "Qri: a != b"},
		{&Transform{Syntax: "a"}, &Transform{Syntax: "b"}, "Syntax: a != b"},
		{&Transform{SyntaxVersion: "a"}, &Transform{SyntaxVersion: "b"}, "SyntaxVersion: a != b"},
		{&Transform{ScriptPath: "a"}, &Transform{ScriptPath: "b"}, "ScriptPath: a != b"},
		{&Transform{}, &Transform{Structure: AirportCodes.Structure}, "Structure: nil: <nil> != <not nil>"},
		{&Transform{}, &Transform{Resources: map[string]*Dataset{}}, "Resources: map[] != map[]"},
		{&Transform{Resources: map[string]*Dataset{
			"airports": AirportCodes,
		}}, &Transform{Resources: map[string]*Dataset{}}, "Resource 'airports': nil: <not nil> != <nil>"},
	}

	for i, c := range cases {
		err := CompareTransforms(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareLicenses(t *testing.T) {
	cases := []struct {
		a, b *License
		err  string
	}{
		{nil, nil, ""},
		{nil, nil, ""},
	}

	for i, c := range cases {
		err := CompareLicenses(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareStringSlices(t *testing.T) {
	cases := []struct {
		a, b []string
		err  string
	}{
		{nil, nil, ""},
		{[]string{}, []string{}, ""},
		{nil, []string{}, ""},
		{[]string{"a"}, []string{"a"}, ""},
		{[]string{"a", "b"}, []string{"a"}, "length: 2 != 1"},
		{[]string{"a", "b"}, []string{"a", "c"}, "element 1: b != c"},
	}

	for i, c := range cases {
		err := CompareStringSlices(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}
