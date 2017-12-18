package dataset

import (
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/dataset/compression"
	"github.com/qri-io/dataset/datatypes"
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
		{NewDatasetRef(datastore.NewKey("a")), NewDatasetRef(datastore.NewKey("b")), "Path: /a != /b"},
		{&Dataset{Kind: "a"}, &Dataset{Kind: "b"}, "Kind: a != b"},
		{
			&Dataset{Timestamp: time.Date(2001, 01, 01, 01, 0, 0, 0, time.UTC)},
			&Dataset{Timestamp: time.Date(2002, 01, 01, 01, 0, 0, 0, time.UTC)},
			"Timestamp: 2001-01-01 01:00:00 +0000 UTC != 2002-01-01 01:00:00 +0000 UTC",
		},
		{&Dataset{Length: 0}, &Dataset{Length: 1}, "Length: 0 != 1"},
		{&Dataset{Rows: 0}, &Dataset{Rows: 1}, "Rows: 0 != 1"},
		{&Dataset{Title: "a"}, &Dataset{Title: "b"}, "Title: a != b"},
		{&Dataset{AccessURL: "a"}, &Dataset{AccessURL: "b"}, "AccessURL: a != b"},
		{&Dataset{DownloadURL: "a"}, &Dataset{DownloadURL: "b"}, "DownloadURL: a != b"},
		{&Dataset{AccrualPeriodicity: "a"}, &Dataset{AccrualPeriodicity: "b"}, "AccrualPeriodicity: a != b"},
		{&Dataset{Readme: "a"}, &Dataset{Readme: "b"}, "Readme: a != b"},
		{&Dataset{Author: nil}, &Dataset{Author: &User{}}, "Author: %!s(*dataset.User=<nil>) != &{  }"},
		{&Dataset{Image: "a"}, &Dataset{Image: "b"}, "Image: a != b"},
		{&Dataset{Description: "a"}, &Dataset{Description: "b"}, "Description: a != b"},
		{&Dataset{Homepage: "a"}, &Dataset{Homepage: "b"}, "Homepage: a != b"},
		{&Dataset{IconImage: "a"}, &Dataset{IconImage: "b"}, "IconImage: a != b"},
		{&Dataset{Identifier: "a"}, &Dataset{Identifier: "b"}, "Identifier: a != b"},
		// TODO
		// {&Dataset{License: &License{}}, &Dataset{Version: "b"}, "Version: a != b"},
		{&Dataset{Version: "a"}, &Dataset{Version: "b"}, "Version: a != b"},
		{&Dataset{Keywords: []string{"a"}}, &Dataset{Keywords: []string{"b"}}, "Keywords: element 0: a != b"},
		{&Dataset{Language: []string{"a"}}, &Dataset{Language: []string{"b"}}, "Language: element 0: a != b"},
		{&Dataset{Theme: []string{"a"}}, &Dataset{Theme: []string{"b"}}, "Theme: element 0: a != b"},
		{&Dataset{QueryString: "a"}, &Dataset{QueryString: "b"}, "QueryString: a != b"},
		{&Dataset{Previous: datastore.NewKey("a")}, &Dataset{Previous: datastore.NewKey("b")}, "Previous: /a != /b"},
		{&Dataset{Data: "a"}, &Dataset{Data: "b"}, "Data: a != b"},
		{&Dataset{}, &Dataset{Structure: &Structure{}}, "Structure: nil: <nil> != <not nil>"},
		{&Dataset{}, &Dataset{Transform: &Transform{}}, "Transform: nil: <nil> != <not nil>"},
		{&Dataset{}, &Dataset{AbstractTransform: &Transform{}}, "AbstractTransform: nil: <nil> != <not nil>"},
		{&Dataset{}, &Dataset{Commit: &CommitMsg{}}, "Commit: nil: %!s(*dataset.CommitMsg=<nil>) != &{{} %!s(*dataset.User=<nil>)   }"},
	}

	for i, c := range cases {
		err := CompareDatasets(c.a, c.b)
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
		{&Structure{Kind: "a"}, &Structure{Kind: "b"}, "Kind: a != b"},
		{&Structure{Format: CSVDataFormat}, &Structure{Format: UnknownDataFormat}, "Format: csv != "},
		{&Structure{Encoding: "a"}, &Structure{Encoding: "b"}, "Encoding: a != b"},
		{&Structure{Compression: compression.None}, &Structure{Compression: compression.Tar}, "Compression:  != tar"},
		{&Structure{}, &Structure{Schema: &Schema{}}, "Schema: nil: %!s(*dataset.Schema=<nil>) != &{[] []}"},
	}

	for i, c := range cases {
		err := CompareStructures(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareSchemas(t *testing.T) {
	cases := []struct {
		a, b *Schema
		err  string
	}{
		{nil, nil, ""},
		{AirportCodes.Structure.Schema, AirportCodes.Structure.Schema, ""},
		{nil, &Schema{}, "nil: %!s(*dataset.Schema=<nil>) != &{[] []}"},
		{&Schema{PrimaryKey: FieldKey{"a"}}, &Schema{PrimaryKey: FieldKey{"b"}}, "PrimaryKey: element 0: a != b"},
		{&Schema{}, &Schema{Fields: []*Field{}}, "Fields: [] != []"},
		{&Schema{}, &Schema{Fields: []*Field{&Field{Name: "a"}}}, "Fields: [] != [%!s(*dataset.Field=&{a 0 <nil>  <nil>  })]"},
		{&Schema{Fields: []*Field{&Field{Name: "a"}}}, &Schema{Fields: []*Field{&Field{Name: "b"}}}, "Fields: element 0: name: a != b"},
	}

	for i, c := range cases {
		err := CompareSchemas(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareFields(t *testing.T) {
	f := &Field{
		Name:         "a",
		Type:         datatypes.String,
		MissingValue: "foo",
		Format:       "fmt",
		Title:        "a",
		Description:  "a",
	}

	cases := []struct {
		a, b *Field
		err  string
	}{
		{nil, nil, ""},
		{f, f, ""},
		{nil, f, "nil: %!s(*dataset.Field=<nil>) != &{a string foo fmt %!s(*dataset.FieldConstraints=<nil>) a a}"},
	}

	for i, c := range cases {
		err := CompareFields(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareCommitMsgs(t *testing.T) {
	c1 := &CommitMsg{
		path:    datastore.NewKey("/foo"),
		Title:   "foo",
		Message: "message",
		Kind:    KindCommitMsg,
		Author:  &User{ID: "foo"},
	}

	cases := []struct {
		a, b *CommitMsg
		err  string
	}{
		{nil, nil, ""},
		{c1, c1, ""},
		{&CommitMsg{}, &CommitMsg{}, ""},
		{nil, c1, "nil: %!s(*dataset.CommitMsg=<nil>) != &{{/foo} %!s(*dataset.User=&{foo  }) qri:cm:0 message foo}"},
		{&CommitMsg{Title: "a"}, &CommitMsg{Title: "b"}, "Title: a != b"},
		{&CommitMsg{Message: "a"}, &CommitMsg{Message: "b"}, "Message: a != b"},
		{&CommitMsg{Kind: "a"}, &CommitMsg{Kind: "b"}, "Kind: a != b"},
	}

	for i, c := range cases {
		err := CompareCommitMsgs(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareTransforms(t *testing.T) {
	t1 := &Transform{
		Kind:       KindTransform,
		Syntax:     "sql",
		AppVersion: "1000.0.0",
		Data:       "select * from airports limit 10",
		Structure:  AirportCodes.Structure,
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
		{NewTransformRef(datastore.NewKey("a")), NewTransformRef(datastore.NewKey("b")), "path: /a != /b"},
		{&Transform{Kind: "a"}, &Transform{Kind: "b"}, "Kind: a != b"},
		{&Transform{Syntax: "a"}, &Transform{Syntax: "b"}, "Syntax: a != b"},
		{&Transform{AppVersion: "a"}, &Transform{AppVersion: "b"}, "AppVersion: a != b"},
		{&Transform{Data: "a"}, &Transform{Data: "b"}, "Data: a != b"},
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
