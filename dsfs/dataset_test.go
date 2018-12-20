package dsfs

import (
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-crypto"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
)

// Test Private Key. peerId: QmZePf5LeXow3RW5U1AgEiNbW46YnRGhZ7HPvm1UmPFPwt
var testPk = []byte(`CAASpgkwggSiAgEAAoIBAQC/7Q7fILQ8hc9g07a4HAiDKE4FahzL2eO8OlB1K99Ad4L1zc2dCg+gDVuGwdbOC29IngMA7O3UXijycckOSChgFyW3PafXoBF8Zg9MRBDIBo0lXRhW4TrVytm4Etzp4pQMyTeRYyWR8e2hGXeHArXM1R/A/SjzZUbjJYHhgvEE4OZy7WpcYcW6K3qqBGOU5GDMPuCcJWac2NgXzw6JeNsZuTimfVCJHupqG/dLPMnBOypR22dO7yJIaQ3d0PFLxiDG84X9YupF914RzJlopfdcuipI+6gFAgBw3vi6gbECEzcohjKf/4nqBOEvCDD6SXfl5F/MxoHurbGBYB2CJp+FAgMBAAECggEAaVOxe6Y5A5XzrxHBDtzjlwcBels3nm/fWScvjH4dMQXlavwcwPgKhy2NczDhr4X69oEw6Msd4hQiqJrlWd8juUg6vIsrl1wS/JAOCS65fuyJfV3Pw64rWbTPMwO3FOvxj+rFghZFQgjg/i45uHA2UUkM+h504M5Nzs6Arr/rgV7uPGR5e5OBw3lfiS9ZaA7QZiOq7sMy1L0qD49YO1ojqWu3b7UaMaBQx1Dty7b5IVOSYG+Y3U/dLjhTj4Hg1VtCHWRm3nMOE9cVpMJRhRzKhkq6gnZmni8obz2BBDF02X34oQLcHC/Wn8F3E8RiBjZDI66g+iZeCCUXvYz0vxWAQQKBgQDEJu6flyHPvyBPAC4EOxZAw0zh6SF/r8VgjbKO3n/8d+kZJeVmYnbsLodIEEyXQnr35o2CLqhCvR2kstsRSfRz79nMIt6aPWuwYkXNHQGE8rnCxxyJmxV4S63GczLk7SIn4KmqPlCI08AU0TXJS3zwh7O6e6kBljjPt1mnMgvr3QKBgQD6fAkdI0FRZSXwzygx4uSg47Co6X6ESZ9FDf6ph63lvSK5/eue/ugX6p/olMYq5CHXbLpgM4EJYdRfrH6pwqtBwUJhlh1xI6C48nonnw+oh8YPlFCDLxNG4tq6JVo071qH6CFXCIank3ThZeW5a3ZSe5pBZ8h4bUZ9H8pJL4C7yQKBgFb8SN/+/qCJSoOeOcnohhLMSSD56MAeK7KIxAF1jF5isr1TP+rqiYBtldKQX9bIRY3/8QslM7r88NNj+aAuIrjzSausXvkZedMrkXbHgS/7EAPflrkzTA8fyH10AsLgoj/68mKr5bz34nuY13hgAJUOKNbvFeC9RI5g6eIqYH0FAoGAVqFTXZp12rrK1nAvDKHWRLa6wJCQyxvTU8S1UNi2EgDJ492oAgNTLgJdb8kUiH0CH0lhZCgr9py5IKW94OSM6l72oF2UrS6PRafHC7D9b2IV5Al9lwFO/3MyBrMocapeeyaTcVBnkclz4Qim3OwHrhtFjF1ifhP9DwVRpuIg+dECgYANwlHxLe//tr6BM31PUUrOxP5Y/cj+ydxqM/z6papZFkK6Mvi/vMQQNQkh95GH9zqyC5Z/yLxur4ry1eNYty/9FnuZRAkEmlUSZ/DobhU0Pmj8Hep6JsTuMutref6vCk2n02jc9qYmJuD7iXkdXDSawbEG6f5C4MUkJ38z1t1OjA==`)

func init() {
	data, err := base64.StdEncoding.DecodeString(string(testPk))
	if err != nil {
		log.Error(err.Error())
		panic(err)
	}
	testPk = data
}

func TestLoadDataset(t *testing.T) {
	store := cafs.NewMapstore()
	dsData, err := ioutil.ReadFile("testdata/all_fields/input.dataset.json")
	if err != nil {
		t.Errorf("error loading test dataset: %s", err.Error())
		return
	}
	ds := &dataset.Dataset{}
	if err := ds.UnmarshalJSON(dsData); err != nil {
		t.Errorf("error unmarshaling test dataset: %s", err.Error())
		return
	}
	body, err := ioutil.ReadFile("testdata/all_fields/body.csv")
	if err != nil {
		t.Errorf("error loading test body: %s", err.Error())
		return
	}

	df := cafs.NewMemfileBytes("all_fields.csv", body)

	apath, err := WriteDataset(store, ds, df, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	_, err = LoadDataset(store, apath)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	cases := []struct {
		ds  *dataset.Dataset
		err string
	}{
		{dataset.NewDatasetRef(datastore.NewKey("/bad/path")),
			"error loading dataset: error getting file bytes: datastore: key not found"},
		{&dataset.Dataset{
			Meta: dataset.NewMetaRef(datastore.NewKey("/bad/path")),
		}, "error loading dataset metadata: error loading metadata file: datastore: key not found"},
		{&dataset.Dataset{
			Structure: dataset.NewStructureRef(datastore.NewKey("/bad/path")),
		}, "error loading dataset structure: error loading structure file: datastore: key not found"},
		{&dataset.Dataset{
			Structure: dataset.NewStructureRef(datastore.NewKey("/bad/path")),
		}, "error loading dataset structure: error loading structure file: datastore: key not found"},
		{&dataset.Dataset{
			Transform: dataset.NewTransformRef(datastore.NewKey("/bad/path")),
		}, "error loading dataset transform: error loading transform raw data: datastore: key not found"},
		{&dataset.Dataset{
			Commit: dataset.NewCommitRef(datastore.NewKey("/bad/path")),
		}, "error loading dataset commit: error loading commit file: datastore: key not found"},
		{&dataset.Dataset{
			Viz: dataset.NewVizRef(datastore.NewKey("/bad/path")),
		}, "error loading dataset viz: error loading viz file: datastore: key not found"},
	}

	for i, c := range cases {
		path := c.ds.Path()
		if !c.ds.IsEmpty() {
			dsf, err := JSONFile(PackageFileDataset.String(), c.ds)
			if err != nil {
				t.Errorf("case %d error generating json file: %s", i, err.Error())
				continue
			}
			path, err = store.Put(dsf, true)
			if err != nil {
				t.Errorf("case %d error putting file in store: %s", i, err.Error())
				continue
			}
		}

		_, err = LoadDataset(store, path)
		if !(err != nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
	}

}

func TestCreateDataset(t *testing.T) {
	store := cafs.NewMapstore()
	prev := Timestamp
	// shameless call to timestamp to get the coverge points
	Timestamp()

	defer func() { Timestamp = prev }()
	Timestamp = func() time.Time { return time.Date(2001, 01, 01, 01, 01, 01, 01, time.UTC) }

	privKey, err := crypto.UnmarshalPrivateKey(testPk)
	if err != nil {
		t.Errorf("error unmarshaling private key: %s", err.Error())
		return
	}

	_, err = CreateDataset(store, nil, nil, nil, nil, nil, false)
	if err == nil {
		t.Errorf("expected call without prvate key to error")
		return
	}
	pkReqErrMsg := "private key is required to create a dataset"
	if err.Error() != pkReqErrMsg {
		t.Errorf("error mismatch. '%s' != '%s'", pkReqErrMsg, err.Error())
		return
	}

	cases := []struct {
		casePath   string
		resultPath string
		repoFiles  int // expected total count of files in repo after test execution
		err        string
	}{
		{"invalid_reference",
			"", 0, "error loading dataset commit: error loading commit file: datastore: key not found"},
		{"invalid",
			"", 0, "commit is required"},
		{"cities",
			"/map/QmPm1VvN3PjZLuA12NSEUTwCft8JruHPwcL2zmKf4SGnWd", 6, ""},
		{"all_fields",
			"/map/QmYHRKiQ52CETCBrMZR2c9hh1Je7292YBeD9gjQyWwEhtE", 14, ""},
		{"cities_no_commit_title",
			"/map/QmRXosHbnSXxVV7cFvnhTfCnzMcqjTj67fYVBKruLeRj9E", 16, ""},
		{"craigslist",
			"/map/QmUAn7Fm8KF2uVDSoafXfEvJj6EErRF9WxiCQtNED2k8HE", 20, ""},
	}

	for _, c := range cases {
		tc, err := dstest.NewTestCaseFromDir("testdata/" + c.casePath)
		if err != nil {
			t.Errorf("%s: error creating test case: %s", c.casePath, err)
			continue
		}

		// TODO - this should probs be auto-populated by dstest package
		if ts, ok := tc.TransformScriptFile(); ok {
			if tc.Input.Transform == nil {
				tc.Input.Transform = &dataset.Transform{}
			}
			tc.Input.Transform.Script = ts
		}

		// TODO - this should probs be auto-populated by dstest package
		if vs, ok := tc.VizScriptFile(); ok {
			if tc.Input.Viz == nil {
				tc.Input.Viz = &dataset.Viz{}
			}
			tc.Input.Viz.Script = vs
		}

		path, err := CreateDataset(store, tc.Input, nil, tc.BodyFile(), nil, privKey, false)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("%s: error mismatch. expected: '%s', got: '%s'", tc.Name, c.err, err)
			continue
		}

		if c.err == "" {
			resultPath := datastore.NewKey(c.resultPath)
			if !resultPath.Equal(path) {
				t.Errorf("%s: result path mismatch: expected: '%s', got: '%s'", tc.Name, resultPath, path)
			}

			if len(store.Files) != c.repoFiles {
				t.Errorf("%s: invalid number of mapstore entries: %d != %d", tc.Name, c.repoFiles, len(store.Files))
				_, err := store.Print()
				if err != nil {
					panic(err)
				}
				continue
			}

			ds, err := LoadDataset(store, resultPath)
			if err != nil {
				t.Errorf("%s: error loading dataset: %s", tc.Name, err.Error())
				continue
			}

			if tc.Expect != nil {
				if err := dataset.CompareDatasets(tc.Expect, ds); err != nil {
					t.Errorf("%s: dataset comparison error: %s", tc.Name, err.Error())
				}
			}
		}
	}

	// Case: no body or previous body files
	dsData, err := ioutil.ReadFile("testdata/cities/input.dataset.json")
	if err != nil {
		t.Errorf("case nil body and previous body files, error reading dataset file: %s", err.Error())
	}
	ds := &dataset.Dataset{}
	if err := ds.UnmarshalJSON(dsData); err != nil {
		t.Errorf("case nil body and previous body files, error unmarshaling dataset file: %s", err.Error())
	}

	if err != nil {
		t.Errorf("case nil body and previous body files, error reading data file: %s", err.Error())
	}
	expectedErr := "datafile or previous datafile needed"
	_, err = CreateDataset(store, ds, nil, nil, nil, privKey, false)
	if err.Error() != expectedErr {
		t.Errorf("case nil body and previous body files, error mismatch: expected '%s', got '%s'", expectedErr, err.Error())
	}

	// Case: no changes in dataset
	expectedErr = "error saving: no changes detected"
	dsPrev, err := LoadDataset(store, datastore.NewKey(cases[2].resultPath))
	ds.PreviousPath = cases[2].resultPath
	if err != nil {
		t.Errorf("case no changes in dataset, error loading previous dataset file: %s", err.Error())
	}

	bodyBytes, err := ioutil.ReadFile("testdata/cities/body.csv")
	if err != nil {
		t.Errorf("case no changes in dataset, error reading body file: %s", err.Error())
	}
	bodyFile := cafs.NewMemfileBytes("body.csv", bodyBytes)

	_, err = CreateDataset(store, ds, dsPrev, bodyFile, nil, privKey, false)
	if err != nil && err.Error() != expectedErr {
		t.Errorf("case no changes in dataset, error mismatch: expected '%s', got '%s'", expectedErr, err.Error())
	} else if err == nil {
		t.Errorf("case no changes in dataset, expected error got 'nil'")
	}

	if len(store.Files) != 20 {
		t.Errorf("case nil datafile and PreviousPath, invalid number of entries: %d != %d", 20, len(store.Files))
		_, err := store.Print()
		if err != nil {
			panic(err)
		}
	}
}

func TestWriteDataset(t *testing.T) {
	store := cafs.NewMapstore()
	prev := Timestamp
	defer func() { Timestamp = prev }()
	Timestamp = func() time.Time { return time.Date(2001, 01, 01, 01, 01, 01, 01, time.UTC) }

	if _, err := WriteDataset(store, nil, nil, true); err == nil || err.Error() != "cannot save empty dataset" {
		t.Errorf("didn't reject empty dataset: %s", err)
	}
	if _, err := WriteDataset(store, &dataset.Dataset{}, nil, true); err == nil || err.Error() != "cannot save empty dataset" {
		t.Errorf("didn't reject empty dataset: %s", err)
	}

	cases := []struct {
		infile    string
		bodyPath  string
		path      string
		repoFiles int // expected total count of files in repo after test execution
		err       string
	}{
		{"testdata/cities/input.dataset.json", "testdata/cities/body.csv", "/map/", 6, ""},
		{"testdata/all_fields/input.dataset.json", "testdata/all_fields/body.csv", "/map/", 12, ""},
	}

	for i, c := range cases {
		indata, err := ioutil.ReadFile(c.infile)
		if err != nil {
			t.Errorf("case %d error opening test infile: %s", i, err.Error())
			continue
		}

		body, err := ioutil.ReadFile(c.bodyPath)
		if err != nil {
			t.Errorf("case %d error reading body file: %s", i, err.Error())
			continue
		}
		df := cafs.NewMemfileBytes(filepath.Base(c.bodyPath), body)

		ds := &dataset.Dataset{}
		if err := ds.UnmarshalJSON(indata); err != nil {
			t.Errorf("case %d error unmarhshalling test file: %s ", i, err.Error())
			continue
		}

		got, err := WriteDataset(store, ds, df, true)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		// path := datastore.NewKey(c.path)
		// if !path.Equal(got) {
		// 	t.Errorf("case %d path mismatch. expected: '%s', got: '%s'", i, path, got)
		// 	continue
		// }

		// total count expected of files in repo after test execution
		if len(store.Files) != c.repoFiles {
			t.Errorf("case expected %d invalid number of entries: %d != %d", i, c.repoFiles, len(store.Files))
			str, err := store.Print()
			if err != nil {
				panic(err)
			}
			t.Log(str)
			continue
		}

		f, err := store.Get(got)
		if err != nil {
			t.Errorf("error getting dataset file: %s", err.Error())
			continue
		}

		ref := &dataset.Dataset{}
		if err := json.NewDecoder(f).Decode(ref); err != nil {
			t.Errorf("error decoding dataset json: %s", err.Error())
			continue
		}

		if ref.Transform != nil {
			if !ref.Transform.IsEmpty() {
				t.Errorf("expected stored dataset.Transform to be a reference")
			}
			ds.Transform.Assign(dataset.NewTransformRef(ref.Transform.Path()))
		}
		if ref.Meta != nil {
			if !ref.Meta.IsEmpty() {
				t.Errorf("expected stored dataset.Meta to be a reference")
			}
			// Abstract transforms aren't loaded
			ds.Meta.Assign(dataset.NewMetaRef(ref.Meta.Path()))
		}
		if ref.Structure != nil {
			if !ref.Structure.IsEmpty() {
				t.Errorf("expected stored dataset.Structure to be a reference")
			}
			ds.Structure.Assign(dataset.NewStructureRef(ref.Structure.Path()))
		}
		if ref.Viz != nil {
			if !ref.Viz.IsEmpty() {
				t.Errorf("expected stored dataset.Viz to be a reference")
			}
			ds.Viz.Assign(dataset.NewVizRef(ref.Viz.Path()))
		}
		ds.BodyPath = ref.BodyPath

		ds.Assign(dataset.NewDatasetRef(got))
		result, err := LoadDataset(store, got)
		if err != nil {
			t.Errorf("case %d unexpected error loading dataset: %s", i, err)
			continue
		}

		if err := dataset.CompareDatasets(ds, result); err != nil {
			t.Errorf("case %d comparison mismatch: %s", i, err.Error())

			d1, _ := ds.MarshalJSON()
			t.Log(string(d1))

			d, _ := result.MarshalJSON()
			t.Log(string(d))
			continue
		}
	}
}
