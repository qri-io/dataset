// datapackage implements the frictionless data DataPackage format http://specs.frictionlessdata.io/data-packages
package datapackage

import (
	"encoding/json"

	"github.com/qri-io/jsontable"
)

type DataPackage struct {
	Name             Name                       `json:"name"`
	Title            string                     `json:"title"`
	Description      string                     `json:"description,omitempty"`
	Homepage         string                     `json:"description, omitempty"`
	License          *License                   `json:"license,omitempty"`
	Version          Version                    `json:"version,omitempty"`
	Format           string                     `json:"format,omitempty"`
	Keywords         []string                   `json:"keywords,omitempty"`
	DataDependencies map[Name]Version           `json:"dataDependcies"`
	Author           *Person                    `json:"author,omitempty"`
	Contributors     []*Person                  `json:"contributors,omitempty"`
	Sources          []*Source                  `json:"sources,omitempty"`
	Resources        []*Resource                `json:"resources"`
	Schemas          map[Name]*jsontable.Schema `json:"schemas,omitempty"`
}

// separate type for marshalling into
type _datapackage DataPackage

// MarshalJSON is a custom JSON implementation that delivers a uuid-string if the
// model is blank, or a full object if data is present
func (d DataPackage) MarshalJSON() ([]byte, error) {
	// // if we only have the Id, but not created & updated values
	// // values, there's a very good chance this model hasn't been
	// // read from the db, so let's return just an id string as a stub
	// if d.Created == 0 && d.Updated == 0 && d.Id != "" {
	// 	return []byte(fmt.Sprintf(`"%s"`, d.Id)), nil
	// }

	// remove schemas, as they're supposed to be stored
	// for i, r := range d.Resources {
	// 	r.Schema = nil
	// }

	return json.Marshal(_datapackage(d))
}

// UnmarhalJSON can marshal in two forms: just an id string, or an object containing a full data model
func (d *DataPackage) UnmarshalJSON(data []byte) error {
	ds := _datapackage{}
	if err := json.Unmarshal(data, &ds); err != nil {
		return err
	}

	*d = DataPackage(ds)

	// place schemas on individual resources
	for name, s := range d.Schemas {
		if name != "" {
			for _, r := range d.Resources {
				if r.Name == name && r.Schema == nil {
					r.Schema = s
				}
			}
		}
	}

	return nil
}
