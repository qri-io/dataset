package preview

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dstest"
)

func TestCreatePreview(t *testing.T) {

	ctx := context.Background()

	_, err := CreatePreview(ctx, &dataset.Dataset{})

	if err == nil {
		t.Fatal(fmt.Errorf("expected empty dataset to error"))
	}

	tc, err := dstest.NewTestCaseFromDir("testdata/earthquakes")
	if err != nil {
		t.Fatal(err)
	}

	got, err := CreatePreview(ctx, tc.Input)
	if err != nil {
		t.Fatal(err)
	}

	rawBody, ok := got.Body.(json.RawMessage)
	if !ok {
		t.Fatal("expected preview body to assert to json.RawMessage")
	}

	body := [][]interface{}{}

	if err := json.Unmarshal(rawBody, &body); err != nil {
		t.Fatal(err)
	}
	got.Body = body

	if len(body) != 100 {
		t.Errorf("error: body length mismatch, expected 100 got %d", len(body))
	}

	// TODO (b5) - required adjustments for accurate comparison due to JSON serialization
	// issues. either solve the serialization issues or add options to dstest.CompareDatasets
	got.Body = []interface{}{}

	expect := dstest.LoadGoldenFile(t, "testdata/earthquakes/golden.dataset.json")

	if diff := dstest.CompareDatasets(expect, got); diff != "" {
		t.Errorf("result mismatch. (-want +got):\n%s", diff)
		dstest.UpdateGoldenFileIfEnvVarSet("testdata/earthquakes/golden.dataset.json", got)
	}

	// make sure you can create a preview of a dataset without a body file
	tc.Input.SetBodyFile(nil)

	got, err = CreatePreview(ctx, tc.Input)
	if err != nil {
		t.Fatalf("unexpected error creating a preview of a dataset without a body: %s", err)
	}
}
