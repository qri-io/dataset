package dataset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func compareTransforms(a, b *Transform) string {
	return cmp.Diff(a, b, cmpopts.IgnoreUnexported(Transform{}))
}

func TestTransformDropTransientValues(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestTransformDropDerivedValues(t *testing.T) {
	tf := &Transform{
		Path: "/ipfs/QmHash",
		Qri:  "oh you know it's qri",
	}

	tf.DropDerivedValues()

	if diff := compareTransforms(tf, &Transform{}); diff != "" {
		t.Errorf("expected dropping a struct only derived values to be empty. diff (-want +got):\n%s", diff)
	}
}

func TestTransformScript(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestTransformOpenScriptFile(t *testing.T) {
	t.Log("TODO (b5)")
}

func TestTransformAssign(t *testing.T) {
	expect := &Transform{
		Path:          "path",
		Syntax:        "a",
		SyntaxVersion: "b",
		Steps: []*TransformStep{
			{Name: "h", Path: "i", Syntax: "j", Category: "k", Script: "l"},
		},
		Config: map[string]interface{}{
			"foo": "bar",
		},
		Resources: map[string]*TransformResource{
			"a": &TransformResource{Path: "/path/to/a"},
		},
	}
	got := &Transform{
		Syntax:        "no",
		SyntaxVersion: "change",
		Config: map[string]interface{}{
			"foo": "baz",
		},
		Resources: nil,
	}

	got.Assign(&Transform{
		Syntax:        "a",
		SyntaxVersion: "b",
		Config: map[string]interface{}{
			"foo": "bar",
		},
		Resources: nil,
	}, &Transform{
		Path: "path",
		Resources: map[string]*TransformResource{
			"a": &TransformResource{Path: "/path/to/a"},
		},
	}, &Transform{
		Steps: []*TransformStep{
			{Name: "h", Path: "i", Syntax: "j", Category: "k", Script: "l"},
		},
	})

	if diff := compareTransforms(expect, got); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}

	got.Assign(nil, nil)
	if diff := compareTransforms(expect, got); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}

	emptyTf := &Transform{}
	emptyTf.Assign(expect)
	if diff := compareTransforms(expect, emptyTf); diff != "" {
		t.Errorf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestTransformShallowCompare(t *testing.T) {
	cases := []struct {
		a, b   *Transform
		expect bool
	}{
		{nil, nil, true},
		{nil, &Transform{}, false},
		{&Transform{}, nil, false},

		{&Transform{Path: "a"}, &Transform{Path: "NOT_A"}, true},

		{&Transform{Qri: "a"}, &Transform{Qri: "b"}, false},
		{&Transform{Syntax: "a"}, &Transform{Syntax: "b"}, false},
		{&Transform{ScriptBytes: []byte("a")}, &Transform{ScriptBytes: []byte("b")}, false},
		{&Transform{ScriptPath: "a"}, &Transform{ScriptPath: "b"}, false},

		{
			&Transform{Qri: "a", Syntax: "b", SyntaxVersion: "c", ScriptBytes: []byte("d"), ScriptPath: "e", Secrets: map[string]string{"f": "f"}, Config: map[string]interface{}{"g": "g"}, Resources: map[string]*TransformResource{"h": nil}},
			&Transform{Qri: "a", Syntax: "b", SyntaxVersion: "c", ScriptBytes: []byte("d"), ScriptPath: "e", Secrets: map[string]string{"f": "f"}, Config: map[string]interface{}{"g": "g"}, Resources: map[string]*TransformResource{"h": nil}},
			true,
		},

		{
			&Transform{Qri: "a", Syntax: "b", SyntaxVersion: "c", Secrets: map[string]string{"f": "f"}, Config: map[string]interface{}{"g": "g"}, Steps: []*TransformStep{{Name: "h", Path: "i", Syntax: "j", Category: "k", Script: "l"}}, Resources: map[string]*TransformResource{"h": nil}},
			&Transform{Qri: "a", Syntax: "b", SyntaxVersion: "c", Secrets: map[string]string{"f": "f"}, Config: map[string]interface{}{"g": "g"}, Steps: []*TransformStep{{Name: "h", Path: "i", Syntax: "j", Category: "k", Script: "l"}}, Resources: map[string]*TransformResource{"h": nil}},
			true,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			got := c.a.ShallowCompare(c.b)
			if c.expect != got {
				t.Errorf("wanted %t, got %t", c.expect, got)
			}
		})
	}
}

func TestTransformUnmarshalJSON(t *testing.T) {
	cases := []struct {
		str       string
		transform *Transform
		err       string
	}{
		{`{}`, &Transform{}, ""},
		{`{"resources":{"foo": "/not/a/real/path"}}`, &Transform{Resources: map[string]*TransformResource{"foo": &TransformResource{Path: "/not/a/real/path"}}}, ""},
		{`{"resources":{"foo": { "path":     "/not/a/real/path"`, &Transform{}, "unexpected end of JSON input"},
		{`{"resources":{"foo": { "path":"/not/a/real/path"}}}`, &Transform{Resources: map[string]*TransformResource{"foo": &TransformResource{Path: "/not/a/real/path"}}}, ""},
	}

	for i, c := range cases {
		got := &Transform{}
		err := json.Unmarshal([]byte(c.str), got)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}

		if diff := compareTransforms(c.transform, got); diff != "" {
			t.Errorf("case %d transform mismatch (-want +got):\n%s", i, diff)
			continue
		}
	}

	strq := &Transform{}
	path := "/path/to/transform"
	if err := json.Unmarshal([]byte(`"`+path+`"`), strq); err != nil {
		t.Errorf("unmarshal string path error: %s", err.Error())
		return
	}

	if strq.Path != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, strq.Path)
		return
	}
}

func TestTransformMarshalJSONObject(t *testing.T) {
	cases := []struct {
		q   *Transform
		out string
		err error
	}{
		{&Transform{}, `{"qri":"tf:0"}`, nil},
		{&Transform{Syntax: "sql", ScriptPath: "foo.star"}, `{"qri":"tf:0","scriptPath":"foo.star","syntax":"sql"}`, nil},
		{&Transform{Syntax: "starlark", Steps: []*TransformStep{
			{Syntax: "starlark", Category: "download", Name: "download", Script: `# get the popular baby names dataset as a csv
		def download(ctx):
			csvDownloadUrl = "https://data.cityofnewyork.us/api/views/25th-nujf/rows.csv?accessType=DOWNLOAD"
			return http.get(csvDownloadUrl).body()`,
			},
			{Name: "transform", Syntax: "starlark", Category: "transform", Script: `# set the body
def transform(ds, ctx):
	# ctx.download is whatever download() returned
	csv = ctx.download
	# set the dataset body
	ds.set_body(csv, parse_as='csv')`,
			},
		}}, `{"qri":"tf:0","steps":[{"name":"download","syntax":"starlark","category":"download","script":"# get the popular baby names dataset as a csv\n\t\tdef download(ctx):\n\t\t\tcsvDownloadUrl = \"https://data.cityofnewyork.us/api/views/25th-nujf/rows.csv?accessType=DOWNLOAD\"\n\t\t\treturn http.get(csvDownloadUrl).body()"},{"name":"transform","syntax":"starlark","category":"transform","script":"# set the body\ndef transform(ds, ctx):\n\t# ctx.download is whatever download() returned\n\tcsv = ctx.download\n\t# set the dataset body\n\tds.set_body(csv, parse_as='csv')"}],"syntax":"starlark"}`, nil},
	}

	for i, c := range cases {
		data, err := json.Marshal(c.q)
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		if string(data) != c.out {
			t.Errorf("case %d result mismatch. expected: %s, got: %s", i, c.out, string(data))
			continue
		}
	}

	strbytes, err := json.Marshal(&Transform{Path: "/path/to/transform"})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(strbytes, []byte("\"/path/to/transform\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(strbytes), "\"/path/to/transform\"")
	}
}

func TestTransformMarshalJSON(t *testing.T) {
	cases := []struct {
		q   *Transform
		out string
		err error
	}{
		{&Transform{}, `{"qri":"tf:0"}`, nil},
		{&Transform{Syntax: "sql", ScriptPath: "foo.star"}, `{"qri":"tf:0","scriptPath":"foo.star","syntax":"sql"}`, nil},
		{&Transform{Syntax: "starlark", Steps: []*TransformStep{
			{Syntax: "starlark", Category: "download", Name: "download", Script: `# get the popular baby names dataset as a csv
		def download(ctx):
			csvDownloadUrl = "https://data.cityofnewyork.us/api/views/25th-nujf/rows.csv?accessType=DOWNLOAD"
			return http.get(csvDownloadUrl).body()`,
			},
			{Name: "transform", Syntax: "starlark", Category: "transform", Script: `# set the body
def transform(ds, ctx):
	# ctx.download is whatever download() returned
	csv = ctx.download
	# set the dataset body
	ds.set_body(csv, parse_as='csv')`,
			},
		}}, `{"qri":"tf:0","steps":[{"name":"download","syntax":"starlark","type":"download","value":"# get the popular baby names dataset as a csv\n\t\tdef download(ctx):\n\t\t\tcsvDownloadUrl = \"https://data.cityofnewyork.us/api/views/25th-nujf/rows.csv?accessType=DOWNLOAD\"\n\t\t\treturn http.get(csvDownloadUrl).body()"},{"name":"transform","syntax":"starlark","type":"transform","value":"# set the body\ndef transform(ds, ctx):\n\t# ctx.download is whatever download() returned\n\tcsv = ctx.download\n\t# set the dataset body\n\tds.set_body(csv, parse_as='csv')"}],"syntax":"starlark"}`, nil},
	}

	for i, c := range cases {
		data, err := json.Marshal(c.q)
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: %s, got: %s", i, c.err, err)
			continue
		}
		check := &map[string]interface{}{}
		err = json.Unmarshal(data, check)
		if err != nil {
			t.Errorf("case %d error: failed to unmarshal to object: %s", i, err.Error())
			continue
		}
	}

}

func TestTransformIsEmpty(t *testing.T) {
	cases := []struct {
		tf       *Transform
		expected bool
	}{
		{&Transform{Qri: KindTransform.String()}, true},
		{&Transform{Path: "foo"}, true},
		{&Transform{}, true},
		{&Transform{Syntax: "foo"}, false},
		{&Transform{SyntaxVersion: "0"}, false},
		{&Transform{ScriptPath: "foo"}, false},
		{&Transform{Config: nil}, true},
		{&Transform{Config: map[string]interface{}{}}, false},
		{&Transform{Resources: nil}, true},
		{&Transform{Resources: map[string]*TransformResource{}}, false},
		{&Transform{Steps: []*TransformStep{}}, false},
	}

	for i, c := range cases {
		if c.tf.IsEmpty() != c.expected {
			t.Errorf("case %d improperly reported transform as empty == %v", i, c.expected)
			continue
		}
	}
}
