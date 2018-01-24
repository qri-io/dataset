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
	"github.com/qri-io/cafs/memfs"
	"github.com/qri-io/dataset"
)

// Test Private Key. peerId: QmZePf5LeXow3RW5U1AgEiNbW46YnRGhZ7HPvm1UmPFPwt
var testPk = []byte(`CAASpgkwggSiAgEAAoIBAQC/7Q7fILQ8hc9g07a4HAiDKE4FahzL2eO8OlB1K99Ad4L1zc2dCg+gDVuGwdbOC29IngMA7O3UXijycckOSChgFyW3PafXoBF8Zg9MRBDIBo0lXRhW4TrVytm4Etzp4pQMyTeRYyWR8e2hGXeHArXM1R/A/SjzZUbjJYHhgvEE4OZy7WpcYcW6K3qqBGOU5GDMPuCcJWac2NgXzw6JeNsZuTimfVCJHupqG/dLPMnBOypR22dO7yJIaQ3d0PFLxiDG84X9YupF914RzJlopfdcuipI+6gFAgBw3vi6gbECEzcohjKf/4nqBOEvCDD6SXfl5F/MxoHurbGBYB2CJp+FAgMBAAECggEAaVOxe6Y5A5XzrxHBDtzjlwcBels3nm/fWScvjH4dMQXlavwcwPgKhy2NczDhr4X69oEw6Msd4hQiqJrlWd8juUg6vIsrl1wS/JAOCS65fuyJfV3Pw64rWbTPMwO3FOvxj+rFghZFQgjg/i45uHA2UUkM+h504M5Nzs6Arr/rgV7uPGR5e5OBw3lfiS9ZaA7QZiOq7sMy1L0qD49YO1ojqWu3b7UaMaBQx1Dty7b5IVOSYG+Y3U/dLjhTj4Hg1VtCHWRm3nMOE9cVpMJRhRzKhkq6gnZmni8obz2BBDF02X34oQLcHC/Wn8F3E8RiBjZDI66g+iZeCCUXvYz0vxWAQQKBgQDEJu6flyHPvyBPAC4EOxZAw0zh6SF/r8VgjbKO3n/8d+kZJeVmYnbsLodIEEyXQnr35o2CLqhCvR2kstsRSfRz79nMIt6aPWuwYkXNHQGE8rnCxxyJmxV4S63GczLk7SIn4KmqPlCI08AU0TXJS3zwh7O6e6kBljjPt1mnMgvr3QKBgQD6fAkdI0FRZSXwzygx4uSg47Co6X6ESZ9FDf6ph63lvSK5/eue/ugX6p/olMYq5CHXbLpgM4EJYdRfrH6pwqtBwUJhlh1xI6C48nonnw+oh8YPlFCDLxNG4tq6JVo071qH6CFXCIank3ThZeW5a3ZSe5pBZ8h4bUZ9H8pJL4C7yQKBgFb8SN/+/qCJSoOeOcnohhLMSSD56MAeK7KIxAF1jF5isr1TP+rqiYBtldKQX9bIRY3/8QslM7r88NNj+aAuIrjzSausXvkZedMrkXbHgS/7EAPflrkzTA8fyH10AsLgoj/68mKr5bz34nuY13hgAJUOKNbvFeC9RI5g6eIqYH0FAoGAVqFTXZp12rrK1nAvDKHWRLa6wJCQyxvTU8S1UNi2EgDJ492oAgNTLgJdb8kUiH0CH0lhZCgr9py5IKW94OSM6l72oF2UrS6PRafHC7D9b2IV5Al9lwFO/3MyBrMocapeeyaTcVBnkclz4Qim3OwHrhtFjF1ifhP9DwVRpuIg+dECgYANwlHxLe//tr6BM31PUUrOxP5Y/cj+ydxqM/z6papZFkK6Mvi/vMQQNQkh95GH9zqyC5Z/yLxur4ry1eNYty/9FnuZRAkEmlUSZ/DobhU0Pmj8Hep6JsTuMutref6vCk2n02jc9qYmJuD7iXkdXDSawbEG6f5C4MUkJ38z1t1OjA==`)

func init() {
	data, err := base64.StdEncoding.DecodeString(string(testPk))
	if err != nil {
		panic(err)
	}
	testPk = data
}

func TestLoadDataset(t *testing.T) {
	store := memfs.NewMapstore()

	dsData, err := ioutil.ReadFile("testdata/complete.json")
	if err != nil {
		t.Errorf("error loading test dataset: %s", err.Error())
		return
	}
	ds := &dataset.Dataset{}
	if err := ds.UnmarshalJSON(dsData); err != nil {
		t.Errorf("error unmarshaling test dataset: %s", err.Error())
		return
	}

	data, err := ioutil.ReadFile("testdata/complete.csv")
	if err != nil {
		t.Errorf("error loading test data: %s", err.Error())
		return
	}

	df := memfs.NewMemfileBytes("complete.csv", data)

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
				t.Errorf("case %d error putting file in store", i, err.Error())
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
	store := memfs.NewMapstore()
	prev := timestamp
	// shameless call to timestamp to get the coverge points
	timestamp()

	defer func() { timestamp = prev }()
	timestamp = func() time.Time { return time.Date(2001, 01, 01, 01, 01, 01, 01, time.UTC) }

	privKey, err := crypto.UnmarshalPrivateKey(testPk)
	if err != nil {
		t.Errorf("error unmarshaling private key: %s", err.Error())
		return
	}

	_, err = CreateDataset(store, nil, nil, nil, false)
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
		dsPath       string
		dataPath     string
		dataFilename string
		resultPath   string
		repoFiles    int // expected total count of files in repo after test execution
		err          string
	}{
		{"testdata/bad/invalid_reference.json", "testdata/cities.csv", "", "", 0, "error loading dataset commit: error loading commit file: datastore: key not found"},
		{"testdata/bad/invalid.json", "testdata/cities.csv", "", "", 0, "commit is required"},
		{"testdata/cities.json", "testdata/cities.csv", "cities.csv", "/map/QmQAHgLH7biAnD3wChpfyBaz1HNUkUwzotttm6TE15smkG", 6, ""},
		{"testdata/complete.json", "testdata/complete.csv", "complete.csv", "/map/QmQ2CuZ8dbKqjyaKvoQwynXgqnxPKTywojNVJ2Jpj2yb6c", 13, ""},
	}

	for i, c := range cases {
		dsData, err := ioutil.ReadFile(c.dsPath)
		if err != nil {
			t.Errorf("case %d error reading dataset file: %s", i, err.Error())
			continue
		}
		ds := &dataset.Dataset{}
		if err := ds.UnmarshalJSON(dsData); err != nil {
			t.Errorf("case %d error unmarshaling dataset file: %s", err.Error())
			continue
		}

		data, err := ioutil.ReadFile(c.dataPath)
		if err != nil {
			t.Errorf("case %d error reading data file: %s", i, err.Error())
			continue
		}
		df := memfs.NewMemfileBytes(c.dataFilename, data)

		path, err := CreateDataset(store, ds, df, privKey, false)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if c.err == "" {
			resultPath := datastore.NewKey(c.resultPath)
			if !resultPath.Equal(path) {
				t.Errorf("case %d result path mismatch: expected: '%s', got: '%s'", i, resultPath, path)
			}

			if len(store.(memfs.MapStore)) != c.repoFiles {
				t.Errorf("case expected %d invalid number of entries: %d != %d", i, c.repoFiles, len(store.(memfs.MapStore)))
				_, err := store.(memfs.MapStore).Print()
				if err != nil {
					panic(err)
				}
				continue
			}

			// TODO - check that Stored datasets are what we expect!
			// str, err := store.(memfs.MapStore).Print()
			// if err != nil {
			// 	panic(err)
			// }
			// t.Error(str)
		}
	}
}

func TestWriteDataset(t *testing.T) {
	store := memfs.NewMapstore()
	prev := timestamp
	defer func() { timestamp = prev }()
	timestamp = func() time.Time { return time.Date(2001, 01, 01, 01, 01, 01, 01, time.UTC) }

	if _, err := WriteDataset(store, nil, nil, true); err == nil || err.Error() != "cannot save empty dataset" {
		t.Errorf("didn't reject empty dataset: %s", err)
	}
	if _, err := WriteDataset(store, &dataset.Dataset{}, nil, true); err == nil || err.Error() != "cannot save empty dataset" {
		t.Errorf("didn't reject empty dataset: %s", err)
	}

	cases := []struct {
		infile    string
		dataPath  string
		path      string
		repoFiles int // expected total count of files in repo after test execution
		err       string
	}{
		{"testdata/cities.json", "testdata/cities.csv", "/map/", 5, ""},
		{"testdata/complete.json", "testdata/complete.csv", "/map/", 13, ""},
	}

	for i, c := range cases {
		indata, err := ioutil.ReadFile(c.infile)
		if err != nil {
			t.Errorf("case %d error opening test infile: %s", i, err.Error())
			continue
		}

		data, err := ioutil.ReadFile(c.dataPath)
		if err != nil {
			t.Errorf("case %d error reading data file: %s", i, err.Error())
			continue
		}
		df := memfs.NewMemfileBytes(filepath.Base(c.dataPath), data)

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
		if len(store.(memfs.MapStore)) != c.repoFiles {
			t.Errorf("case expected %d invalid number of entries: %d != %d", i, c.repoFiles, len(store.(memfs.MapStore)))
			str, err := store.(memfs.MapStore).Print()
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

		if ref.Abstract != nil {
			if !ref.Abstract.IsEmpty() {
				t.Errorf("expected stored dataset.Abstract to be a reference")
			}
			// Abstract paths shouldnt' be loaded
			ds.Abstract = dataset.NewDatasetRef(ref.Abstract.Path())
		}

		if ref.Transform != nil {
			if !ref.Transform.IsEmpty() {
				t.Errorf("expected stored dataset.Transform to be a reference")
			}
			ds.Transform.Assign(dataset.NewTransformRef(ref.Transform.Path()))
		}
		if ref.AbstractTransform != nil {
			if !ref.AbstractTransform.IsEmpty() {
				t.Errorf("expected stored dataset.AbstractTransform to be a reference")
			}
			// Abstract transforms aren't loaded
			ds.AbstractTransform = dataset.NewTransformRef(ref.AbstractTransform.Path())
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
		ds.DataPath = ref.DataPath

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

// func TestPrepareDataset(t *testing.T) {
// 	store := memfs.NewMapstore()

// 	cases := []struct {
// 		in, out *dataset.Dataset
// 		err     string
// 	}{}

// 	for i, c := range cases {
// 		err := PrepareDataset(store, ds)
// 		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
// 			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
// 			continue
// 		}
// 	}
// }
