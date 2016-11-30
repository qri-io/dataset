package dataset

import "fmt"

func AddressErrors(a *Dataset, prev *[]Address) (errs []error) {
	if a.Address == nil {
		errs = append(errs, fmt.Errorf("address cannot be empty"))
		return
	}

	if a.Address.IsEmpty() {
		errs = append(errs, fmt.Errorf("address cannot be empty"))
	}

	if err := checkDup(a.Address, prev); err != nil {
		errs = append(errs, err)
	}

	for _, ds := range a.Datasets {
		if err := checkDup(ds.Address, prev); err != nil {
			errs = append(errs, err)
		} else {
			if !a.Address.IsAncestor(ds.Address) {
				errs = append(errs, fmt.Errorf("%s cannot be a child of %s", ds.Address.String(), a.Address.String()))
			} else if a.Address.Equal(ds.Address) {
				errs = append(errs, fmt.Errorf("%s cannot be a child of %s", ds.Address.String(), a.Address.String()))
			}
		}

		if ds.Datasets != nil {
			errs = append(errs, AddressErrors(ds, prev)...)
		}
	}

	return
}

func checkDup(adr Address, prev *[]Address) error {
	for _, p := range *prev {
		if adr.Equal(p) {
			return fmt.Errorf("duplicate address: %s", adr)
		}
	}
	*prev = append(*prev, adr)
	return nil
}
