package dataset

import "testing"

func TestAddressErrors(t *testing.T) {
	cases := []struct {
		ds     *Dataset
		errors []string
	}{
		{&Dataset{Address: NewAddress("a")}, nil},
		{&Dataset{Address: NewAddress("")}, []string{"address cannot be empty"}},
		{&Dataset{Address: NewAddress("a"), Datasets: []*Dataset{&Dataset{Address: NewAddress("a", "b")}}}, nil},
		{&Dataset{Address: NewAddress("a"), Datasets: []*Dataset{&Dataset{Address: NewAddress("a")}}}, []string{"duplicate address: a"}},
		{&Dataset{Address: NewAddress("a"), Datasets: []*Dataset{&Dataset{Address: NewAddress("b")}}}, []string{"b cannot be a child of a"}},
		{&Dataset{Address: NewAddress("a.b"), Datasets: []*Dataset{&Dataset{Address: NewAddress("b")}}}, []string{"b cannot be a child of a.b"}},
		{&Dataset{Address: NewAddress("a.b.c"), Datasets: []*Dataset{&Dataset{Address: NewAddress("a.b")}}}, []string{"a.b cannot be a child of a.b.c"}},
	}

	for i, c := range cases {
		got := AddressErrors(c.ds, &[]Address{})

		if len(c.errors) != len(got) {
			t.Errorf("case %d error length mismatch. expected %d errors, got %d", i, len(c.errors), len(got))
			t.Error(got)
			continue
		}

		for j, str := range c.errors {
			if got[j].Error() != str {
				t.Errorf("case %d returned error %d mismatch. expected %s. got %s", i, j, str, got[j].Error())
			}
		}
	}
}
