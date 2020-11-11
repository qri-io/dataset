package dataset

import (
	"testing"
	"time"

	"github.com/qri-io/dataset/compression"
)

func TestCompareDatasets(t *testing.T) {
	cases := []struct {
		a, b *Dataset
		err  string
	}{
		{nil, nil, ""},
		{nil, AirportCodes, "nil: <nil> != <not nil>"},
		{AirportCodes, nil, "nil: <not nil> != <nil>"},
		{AirportCodes, AirportCodes, ""},
		{&Dataset{Qri: "a"}, &Dataset{Qri: "b"}, "Qri: a != b"},
		{&Dataset{PreviousPath: "a"}, &Dataset{PreviousPath: "b"}, "PreviousPath: a != b"},
		{&Dataset{BodyPath: "a"}, &Dataset{BodyPath: "b"}, "BodyPath: a != b"},
		{&Dataset{}, &Dataset{Structure: &Structure{}}, "Structure: nil: <nil> != <not nil>"},
		{&Dataset{}, &Dataset{Transform: &Transform{}}, "Transform: nil: <nil> != <not nil>"},
		{&Dataset{}, &Dataset{Commit: &Commit{}}, "Commit: nil: <nil> != <not nil>"},
		{&Dataset{}, &Dataset{Stats: &Stats{}}, "Stats: nil: <nil> != <not nil>"},
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
		{nil, AirportCodes.Meta, "nil: <nil> != <not nil>"},
		{AirportCodes.Meta, nil, "nil: <not nil> != <nil>"},
		{AirportCodes.Meta, AirportCodes.Meta, ""},
		{&Meta{Qri: "a"}, &Meta{Qri: "b"}, "Qri: a != b"},
		{&Meta{Title: "a"}, &Meta{Title: "b"}, "Title: a != b"},
		{&Meta{AccessURL: "a"}, &Meta{AccessURL: "b"}, "AccessURL: a != b"},
		{&Meta{DownloadURL: "a"}, &Meta{DownloadURL: "b"}, "DownloadURL: a != b"},
		{&Meta{AccrualPeriodicity: "a"}, &Meta{AccrualPeriodicity: "b"}, "AccrualPeriodicity: a != b"},
		{&Meta{ReadmeURL: "a"}, &Meta{ReadmeURL: "b"}, "ReadmeURL: a != b"},
		{&Meta{Description: "a"}, &Meta{Description: "b"}, "Description: a != b"},
		{&Meta{HomeURL: "a"}, &Meta{HomeURL: "b"}, "HomeURL: a != b"},
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
		{&Structure{Depth: 0}, &Structure{Depth: 1}, "Depth: 0 != 1"},
		{&Structure{Format: "csv"}, &Structure{Format: ""}, "Format: csv != "},
		{&Structure{Encoding: "a"}, &Structure{Encoding: "b"}, "Encoding: a != b"},
		{&Structure{Compression: ""}, &Structure{Compression: compression.Tar.String()}, "Compression:  != tar"},
		{&Structure{}, &Structure{Schema: map[string]interface{}{}}, "Schema: nil: <nil> != <not nil>"},
	}

	for i, c := range cases {
		err := CompareStructures(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareVizs(t *testing.T) {
	cases := []struct {
		a, b *Viz
		err  string
	}{
		{nil, nil, ""},
		{&Viz{Qri: "a", Format: "b", ScriptPath: "c"}, &Viz{Qri: "a", Format: "b", ScriptPath: "c"}, ""},
		{&Viz{}, nil, "nil: <not nil> != <nil>"},
		{nil, &Viz{}, "nil: <nil> != <not nil>"},
		{&Viz{Qri: "a"}, &Viz{Qri: "b"}, "Qri: a != b"},
		{&Viz{Format: "a"}, &Viz{Format: "b"}, "Format: a != b"},
		{&Viz{ScriptPath: "a"}, &Viz{ScriptPath: "b"}, "ScriptPath: a != b"},
	}

	for i, c := range cases {
		err := CompareVizs(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareCommits(t *testing.T) {
	c1 := &Commit{
		Path:    "/foo",
		Title:   "foo",
		Message: "message",
		Qri:     KindCommit.String(),
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
		Qri:           KindTransform.String(),
		Syntax:        "starlark",
		SyntaxVersion: "1000.0.0",
		ScriptPath:    "foo.star",
		Resources: map[string]*TransformResource{
			"airports": &TransformResource{Path: AirportCodes.Path},
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
		{&Transform{Resources: map[string]*TransformResource{
			"airports": &TransformResource{Path: AirportCodes.Path},
		}}, &Transform{Resources: map[string]*TransformResource{}}, "Resource 'airports': nil: <not nil> != <nil>"},
	}

	for i, c := range cases {
		err := CompareTransforms(c.a, c.b)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error: expected: '%s', got: '%s'", i, c.err, err)
		}
	}
}

func TestCompareTransformResources(t *testing.T) {
	tr1 := &TransformResource{Path: "foo"}
	tr2 := &TransformResource{Path: "bar"}
	cases := []struct {
		a, b *TransformResource
		err  string
	}{
		{nil, nil, ""},
		{tr1, tr1, ""},
		{tr1, nil, "nil: <not nil> != <nil>"},
		{nil, tr1, "nil: <nil> != <not nil>"},
		{tr1, tr2, "Path mismatch. foo != bar"},
	}

	for i, c := range cases {
		err := CompareTransformResources(c.a, c.b)
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
