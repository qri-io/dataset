package dataset

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
)

// Meta contains all human-readable metadata about a dataset
type Metadata struct {
	// private storage for reference to this object
	path datastore.Key
	// meta holds additional arbitrarty metadata not covered by the spec
	// when encoding & decoding json values here will be hoisted into the
	// Dataset object
	meta map[string]interface{}

	// Url to access the dataset
	AccessPath string `json:"accessPath,omitempty"`
	// The frequency with which dataset changes. Must be an ISO 8601 repeating duration
	AccrualPeriodicity string `json:"accrualPeriodicity,omitempty"`
	// Citations is a slice of assets used to build this dataset
	Citations []*Citation `json:"citations"`
	// Contribute
	Contributors []*User `json:"contributors,omitempty"`
	// Description follows the DCAT sense of the word, it should be around a paragraph of
	// human-readable text
	Description string `json:"description,omitempty"`
	// Url that should / must lead directly to the data itself
	DownloadPath string `json:"downloadPath,omitempty"`
	// HomePath is a path to a "home" resource, either a url or d.web path
	HomePath string `json:"homePath,omitempty"`
	// Identifier is for *other* data catalog specifications. Identifier should not be used
	// or relied on to be unique, because this package does not enforce any of these rules.
	Identifier string `json:"identifier,omitempty"`
	// String of Keywords
	Keywords []string `json:"keywords,omitempty"`
	// Kind is required, must be qri:md:[version]
	Kind Kind `json:"kind"`
	// Languages this dataset is written in
	Language []string `json:"language,omitempty"`
	// License will automatically parse to & from a string value if provided as a raw string
	License *License `json:"license,omitempty"`
	// path to readmePath
	ReadmePath string `json:"readmePath,omitempty"`
	// Title of this dataset
	Title string `json:"title,omitempty"`
	// Theme
	Theme []string `json:"theme,omitempty"`
	// Version is the semantic version for this dataset
	Version string `json:"version,omitempty"`
}

// IsEmpty checks to see if dataset has any fields other than the internal path
func (md *Metadata) IsEmpty() bool {
	return md.Title == "" &&
		md.Description == "" &&
		md.AccessPath == "" &&
		md.AccrualPeriodicity == "" &&
		md.Citations == nil &&
		md.Contributors == nil &&
		md.Description == "" &&
		md.DownloadPath == "" &&
		md.HomePath == "" &&
		md.Identifier == "" &&
		md.Keywords == nil &&
		md.Language == nil &&
		md.ReadmePath == "" &&
		md.Title == "" &&
		md.Theme == nil &&
		md.Version == ""
}

// Path gives the internal path reference for this dataset
func (md *Metadata) Path() datastore.Key {
	return md.path
}

// NewMetadataRef creates a Metadata pointer with the internal
// path property specified, and no other fields.
func NewMetadataRef(path datastore.Key) *Metadata {
	return &Metadata{path: path}
}

// Meta gives access to additional metadata not covered by dataset metadata
func (md *Metadata) Meta() map[string]interface{} {
	if md.meta == nil {
		md.meta = map[string]interface{}{}
	}
	return md.meta
}

// Assign collapses all properties of a group of metadata structs onto one.
// this is directly inspired by Javascript's Object.assign
func (md *Metadata) Assign(metas ...*Metadata) {
	for _, m := range metas {
		if m == nil {
			continue
		}

		if m.path.String() != "" {
			md.path = m.path
		}
		if m.Title != "" {
			md.Title = m.Title
		}
		if m.AccessPath != "" {
			md.AccessPath = m.AccessPath
		}
		if m.DownloadPath != "" {
			md.DownloadPath = m.DownloadPath
		}
		if m.ReadmePath != "" {
			md.ReadmePath = m.ReadmePath
		}
		if m.AccrualPeriodicity != "" {
			md.AccrualPeriodicity = m.AccrualPeriodicity
		}
		if m.Citations != nil {
			md.Citations = m.Citations
		}
		if m.Description != "" {
			md.Description = m.Description
		}
		if m.HomePath != "" {
			md.HomePath = m.HomePath
		}
		if m.Identifier != "" {
			md.Identifier = m.Identifier
		}
		if m.License != nil {
			md.License = m.License
		}
		if m.Version != "" {
			md.Version = m.Version
		}
		if m.Keywords != nil {
			md.Keywords = m.Keywords
		}
		if m.Contributors != nil {
			md.Contributors = m.Contributors
		}
		if m.Language != nil {
			md.Language = m.Language
		}
		if m.Theme != nil {
			md.Theme = m.Theme
		}
		if m.meta != nil {
			md.meta = m.meta
		}
	}
}

// MarshalJSON uses a map to combine meta & standard fields.
// Marshalling a map[string]interface{} automatically alpha-sorts the keys.
func (md *Metadata) MarshalJSON() ([]byte, error) {
	// if we're dealing with an empty object that has a path specified, marshal to a string instead
	// TODO - check all fielmd
	if md.path.String() != "" && md.IsEmpty() {
		return md.path.MarshalJSON()
	}

	data := md.Meta()

	data["kind"] = KindMetadata

	if md.AccessPath != "" {
		data["accessPath"] = md.AccessPath
	}
	if md.Citations != nil {
		data["citations"] = md.Citations
	}
	if md.Contributors != nil {
		data["contributors"] = md.Contributors
	}
	if md.Description != "" {
		data["description"] = md.Description
	}
	if md.DownloadPath != "" {
		data["downloadPath"] = md.DownloadPath
	}
	if md.HomePath != "" {
		data["homePath"] = md.HomePath
	}
	if md.Identifier != "" {
		data["identifier"] = md.Identifier
	}
	if md.Keywords != nil {
		data["keywords"] = md.Keywords
	}
	if md.Language != nil {
		data["language"] = md.Language
	}
	if md.License != nil {
		data["license"] = md.License
	}
	if md.ReadmePath != "" {
		data["readmePath"] = md.ReadmePath
	}
	if md.Theme != nil {
		data["theme"] = md.Theme
	}
	if md.Title != "" {
		data["title"] = md.Title
	}
	if md.AccrualPeriodicity != "" {
		data["accrualPeriodicity"] = md.AccrualPeriodicity
	}
	if md.Version != "" {
		data["version"] = md.Version
	}

	return json.Marshal(data)
}

// internal struct for json unmarshaling
type _metadata Metadata

// UnmarshalJSON implements json.Unmarshaller
func (md *Metadata) UnmarshalJSON(data []byte) error {
	// first check to see if this is a valid path ref
	var path string
	if err := json.Unmarshal(data, &path); err == nil {
		*md = Metadata{path: datastore.NewKey(path)}
		return nil
	}

	// TODO - I'm guessing what follows could be better
	d := _metadata{}
	if err := json.Unmarshal(data, &d); err != nil {
		return fmt.Errorf("error unmarshling dataset: %s", err.Error())
	}

	meta := map[string]interface{}{}
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("error unmarshaling dataset metadata: %s", err, err)
	}

	for _, f := range []string{
		"accessPath",
		"accrualPeriodicity",
		"author",
		"citations",
		"contributors",
		"data",
		"description",
		"downloadPath",
		"homePath",
		"iconImage",
		"identifier",
		"image",
		"keyword",
		"kind",
		"language",
		"length",
		"license",
		"readmePath",
		"theme",
		"timestamp",
		"title",
		"version",
	} {
		delete(meta, f)
	}

	d.meta = meta
	*md = Metadata(d)
	return nil
}

// User is a placholder for talking about people, groups, organizations
type User struct {
	ID       string `json:"id,omitempty"`
	Fullname string `json:"name,omitempty"`
	Email    string `json:"email,omitempty"`
}

// License represents a legal licensing agreement
type License struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

// private struct for marshaling
type _license License

// MarshalJSON satisfies the json.Marshaller interface
func (l License) MarshalJSON() ([]byte, error) {
	if l.Type != "" && l.URL == "" {
		return []byte(fmt.Sprintf(`"%s"`, l.Type)), nil
	}

	return json.Marshal(_license(l))
}

// UnmarshalJSON satisfies the json.Unmarshaller interface
func (l *License) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*l = License{Type: s}
		return nil
	}

	_l := &_license{}
	if err := json.Unmarshal(data, _l); err != nil {
		return fmt.Errorf("error parsing license from json: %s", err.Error())
	}
	*l = License(*_l)

	return nil
}

// Citation is a place that this dataset drew it's information from
type Citation struct {
	Name  string `json:"name,omitempty"`
	URL   string `json:"url,omitempty"`
	Email string `json:"email,omitempty"`
}

// Theme is pulled from the Project Open Data Schema version 1.1
type Theme struct {
	Description     string `json:"description,omitempty"`
	DisplayName     string `json:"display_name,omitempty"`
	ImageDisplayURL string `json:"image_display_url,omitempty"`
	ID              string `json:"id,omitempty"`
	Name            string `json:"name,omitempty"`
	Title           string `json:"title,omitempty"`
}

// AccuralDuration takes an ISO 8601 periodicity measure & returns a time.Duration.
// invalid periodicities return time.Duration(0)
func AccuralDuration(p string) time.Duration {
	switch p {
	// Decennial
	case "R/P10Y":
		return time.Duration(time.Hour * 24 * 365 * 10)
	// Quadrennial
	case "R/P4Y":
		return time.Duration(time.Hour * 24 * 365 * 4)
	// Annual
	case "R/P1Y":
		return time.Duration(time.Hour * 24 * 365)
	// Bimonthly
	case "R/P2M":
		return time.Duration(time.Hour * 24 * 30 * 10)
	// Semiweekly
	case "R/P3.5D":
		// TODO - inaccurate
		return time.Duration(time.Hour * 24 * 4)
	// Daily
	case "R/P1D":
		return time.Duration(time.Hour * 24)
	// Biweekly
	case "R/P2W":
		return time.Duration(time.Hour * 24 * 14)
	// Semiannual
	case "R/P6M":
		return time.Duration(time.Hour * 24 * 30 * 6)
	// Biennial
	case "R/P2Y":
		return time.Duration(time.Hour * 24 * 365 * 2)
	// Triennial
	case "R/P3Y":
		return time.Duration(time.Hour * 24 * 365 * 3)
	// Three times a week
	case "R/P0.33W":
		return time.Duration((time.Hour * 24 * 7) / 3)
	// Three times a month
	case "R/P0.33M":
		return time.Duration((time.Hour * 24 * 30) / 3)
	// Continuously updated
	case "R/PT1S":
		return time.Second
	// Monthly
	case "R/P1M":
		return time.Duration(time.Hour * 24 * 30)
	// Quarterly
	case "R/P3M":
		return time.Duration((time.Hour * 24 * 365) / 7)
	// Semimonthly
	case "R/P0.5M":
		return time.Duration(time.Hour * 24 * 15)
	// Three times a year
	case "R/P4M":
		return time.Duration((time.Hour * 24 * 365) / 4)
	// Weekly
	case "R/P1W":
		return time.Duration(time.Hour * 24 * 7)
	// Hourly
	case "R/PT1H":
		return time.Hour
	default:
		return time.Duration(0)
	}
}
