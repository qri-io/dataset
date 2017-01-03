package dsutil

import "testing"

func TestPackageDataset(t *testing.T) {
	t.Skip("TODO")

	// wd, err := os.Getwd()
	// if err != nil {
	// 	t.Error(err.Error())
	// 	return
	// }

	// ns := NewNamespaceFromPath(filepath.Join(wd, "test_data"))
	// if ns.RootDataset == nil {
	// 	t.Errorf("root didn't declare a dataset")
	// 	return
	// }

	// r, size, err := PackageDataset(store, ds)
	// r, size, err := ns.Package(dataset.NewAddress("local.b"))
	// if err != nil {
	// 	t.Errorf("error packaging dataset: %s", err.Error())
	// 	return
	// }

	// zr, err := zip.NewReader(r, size)
	// if err != nil {
	// 	t.Errorf("error creating zip reader: %s", err.Error())
	// 	return
	// }

	// for _, f := range zr.File {
	// 	rc, err := f.Open()
	// 	if err != nil {
	// 		t.Errorf("error opening file %s in package", f.Name)
	// 		break
	// 	}

	// 	if err := rc.Close(); err != nil {
	// 		t.Errorf("error closing file %s in package", f.Name)
	// 		break
	// 	}
	// }
}
