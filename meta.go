package dataset

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Meta contains human-readable descriptive metadata that qualifies and
// distinguishes a dataset.
// Well-defined Meta should aid in making datasets Findable by describing a
// dataset in generalizable taxonomies that can aggregate across other dataset
// documents. Because dataset documents are intended to interoperate with many
// other data storage and cataloging systems, meta fields and conventions are
// derived from existing metadata formats whenever possible
type Meta struct {
	// meta holds additional arbitrary metadata not covered by the spec when
	// encoding & decoding json values here will be hoisted into the meta object
	meta map[string]interface{}

	// Url to access the dataset
	AccessURL string `json:"accessURL,omitempty"`
	// The frequency with which dataset changes. Must be an ISO 8601 repeating
	// duration
	AccrualPeriodicity string `json:"accrualPeriodicity,omitempty"`
	// Citations is a slice of assets used to build this dataset
	Citations []*Citation `json:"citations"`
	// Contribute
	Contributors []*User `json:"contributors,omitempty"`
	// Description follows the DCAT sense of the word, it should be around a
	// paragraph of human-readable text
	Description string `json:"description,omitempty"`
	// Url that should / must lead directly to the data itself
	DownloadURL string `json:"downloadURL,omitempty"`
	// HomeURL is a path to a "home" resource
	HomeURL string `json:"homeURL,omitempty"`
	// Identifier is for *other* data catalog specifications. Identifier should
	// not be used or relied on to be unique, because this package does not
	// enforce any of these rules.
	Identifier string `json:"identifier,omitempty"`
	// String of Keywords
	Keywords []string `json:"keywords,omitempty"`
	// Languages this dataset is written in
	Language []string `json:"language,omitempty"`
	// License will automatically parse to & from a string value if provided as a
	// raw string
	License *License `json:"license,omitempty"`
	// path is the location of meta, transient
	// derived
	Path string `json:"path,omitempty"`
	// Kind is required, must be qri:md:[version]
	// derived
	Qri string `json:"qri,omitempty"`
	// path to dataset readme file, not part of the DCAT spec, but a common
	// convention in software dev
	ReadmeURL string `json:"readmeURL,omitempty"`
	// Title of this dataset
	Title string `json:"title,omitempty"`
	// "Category" for
	Theme []string `json:"theme,omitempty"`
	// Version is the version identifier for this dataset
	Version string `json:"version,omitempty"`
}

// DropTransientValues removes values that cannot be recorded when the
// dataset is rendered immutable, usually by storing it in a cafs
func (md *Meta) DropTransientValues() {
	md.Path = ""
}

// DropDerivedValues resets all set-on-save fields to their default values
func (md *Meta) DropDerivedValues() {
	md.Path = ""
	md.Qri = ""
}

// IsEmpty checks to see if dataset has any fields other than the internal path
func (md *Meta) IsEmpty() bool {
	return md.AccessURL == "" &&
		md.AccrualPeriodicity == "" &&
		md.Citations == nil &&
		md.Contributors == nil &&
		md.Description == "" &&
		md.DownloadURL == "" &&
		md.HomeURL == "" &&
		md.Identifier == "" &&
		md.Keywords == nil &&
		md.Language == nil &&
		md.License == nil &&
		md.ReadmeURL == "" &&
		md.Title == "" &&
		md.Theme == nil &&
		md.Version == ""
}

// NewMetaRef creates a Meta pointer with the internal
// path property specified, and no other fields.
func NewMetaRef(path string) *Meta {
	return &Meta{Path: path}
}

// Meta gives access to additional metadata not covered by dataset metadata
func (md *Meta) Meta() map[string]interface{} {
	if md.meta == nil {
		md.meta = map[string]interface{}{}
	}
	shallowCopy := map[string]interface{}{}
	for key, val := range md.meta {
		shallowCopy[key] = val
	}
	return shallowCopy
}

// strVal confirms an interface is a string
func strVal(val interface{}) (s string, err error) {
	var ok bool
	if val == nil {
		return "", nil
	}

	if s, ok = val.(string); !ok {
		err = fmt.Errorf("type must be a string")
	}
	return
}

// strVal confirms an interface is a []string
func strSliceVal(val interface{}) (s []string, err error) {
	var ok bool
	if val == nil {
		return nil, nil
	}

	si, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("type must be a set of strings")
	}

	for i, stri := range si {
		str, e := strVal(stri)
		if e != nil {
			return nil, fmt.Errorf("index %d: %s", i, e.Error())
		}
		s = append(s, str)
	}
	return
}

// Set writes value to key in metadata, erroring if the type is invalid
// input values are expected to be json.Unmarshal types
func (md *Meta) Set(key string, val interface{}) (err error) {

	switch strings.TrimSpace(strings.ToLower(key)) {
	// string meta fields
	case "qri":
		md.Qri, err = strVal(val)
	case "accessurl":
		md.AccessURL, err = strVal(val)
	case "accrualperiodicity":
		md.AccrualPeriodicity, err = strVal(val)
	case "description":
		md.Description, err = strVal(val)
	case "downloadurl":
		md.DownloadURL, err = strVal(val)
	case "homeurl":
		md.HomeURL, err = strVal(val)
	case "identifier":
		md.Identifier, err = strVal(val)
	case "readmeurl":
		md.ReadmeURL, err = strVal(val)
	case "title":
		md.Title, err = strVal(val)
	case "version":
		md.Version, err = strVal(val)

	// []string meta fields
	case "keywords":
		md.Keywords, err = strSliceVal(val)
	case "language":
		md.Language, err = strSliceVal(val)
	case "theme":
		md.Theme, err = strSliceVal(val)

	// "exotic" meta fields
	case "citations":
		if sl, ok := val.([]interface{}); ok {
			md.Citations = make([]*Citation, len(sl))
			for i, ci := range sl {
				c := &Citation{}
				if err = c.Decode(ci); err != nil {
					err = fmt.Errorf("parsing citations index %d: %s", i, err.Error())
					return
				}
				md.Citations[i] = c
			}
		} else {
			err = fmt.Errorf("citation: expected interface slice")
		}
	case "contributors":
		if sl, ok := val.([]interface{}); ok {
			md.Contributors = make([]*User, len(sl))
			for i, ci := range sl {
				c := &User{}
				if err = c.Decode(ci); err != nil {
					err = fmt.Errorf("parsing contributors index %d: %s", i, err.Error())
					return
				}
				md.Contributors[i] = c
			}
		} else {
			err = fmt.Errorf("contributors: expected interface slice")
		}
	case "license":
		md.License = &License{}
		err = md.License.Decode(val)

	// everything else
	default:
		if md.meta == nil {
			md.meta = map[string]interface{}{}
		}
		md.meta[key] = val
	}

	return
}

// SetArbitrary is for implementing the ArbitrarySetter interface defined by base/fill_struct.go
func (md *Meta) SetArbitrary(key string, val interface{}) (err error) {
	if md.meta == nil {
		md.meta = map[string]interface{}{}
	}
	md.meta[key] = val
	return nil
}

// Assign collapses all properties of a group of metadata structs onto one.
// this is directly inspired by Javascript's Object.assign
func (md *Meta) Assign(metas ...*Meta) {
	for _, m := range metas {
		if m == nil {
			continue
		}

		if m.meta != nil {
			md.meta = m.meta
		}

		if m.AccessURL != "" {
			md.AccessURL = m.AccessURL
		}
		if m.AccrualPeriodicity != "" {
			md.AccrualPeriodicity = m.AccrualPeriodicity
		}
		if m.Citations != nil {
			md.Citations = m.Citations
		}
		if m.Contributors != nil {
			md.Contributors = m.Contributors
		}
		if m.Description != "" {
			md.Description = m.Description
		}
		if m.DownloadURL != "" {
			md.DownloadURL = m.DownloadURL
		}
		if m.HomeURL != "" {
			md.HomeURL = m.HomeURL
		}
		if m.Identifier != "" {
			md.Identifier = m.Identifier
		}
		if m.Keywords != nil {
			md.Keywords = m.Keywords
		}
		if m.Language != nil {
			md.Language = m.Language
		}
		if m.License != nil {
			md.License = m.License
		}
		if m.Path != "" {
			md.Path = m.Path
		}
		if m.Qri != "" {
			md.Qri = m.Qri
		}
		if m.ReadmeURL != "" {
			md.ReadmeURL = m.ReadmeURL
		}
		if m.Theme != nil {
			md.Theme = m.Theme
		}
		if m.Title != "" {
			md.Title = m.Title
		}
		if m.Version != "" {
			md.Version = m.Version
		}
	}
}

// MarshalJSON uses a map to combine meta & standard fields.
// Marshalling a map[string]interface{} automatically alpha-sorts the keys.
func (md *Meta) MarshalJSON() ([]byte, error) {
	// if we're dealing with an empty object that has a path specified
	// marshal to a string instead
	if md.Path != "" && md.IsEmpty() {
		return json.Marshal(md.Path)
	}

	return md.MarshalJSONObject()
}

// MarshalJSONObject always marshals to a json Object, even if meta is empty or
// a reference
func (md *Meta) MarshalJSONObject() ([]byte, error) {
	data := md.Meta()

	data["qri"] = KindMeta.String()

	if md.AccessURL != "" {
		data["accessURL"] = md.AccessURL
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
	if md.DownloadURL != "" {
		data["downloadURL"] = md.DownloadURL
	}
	if md.HomeURL != "" {
		data["homeURL"] = md.HomeURL
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
	if md.Path != "" {
		data["path"] = md.Path
	}
	if md.ReadmeURL != "" {
		data["readmeURL"] = md.ReadmeURL
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
type _metadata Meta

// UnmarshalJSON implements json.Unmarshaller
func (md *Meta) UnmarshalJSON(data []byte) error {
	// first check to see if this is a valid path ref
	var path string
	if err := json.Unmarshal(data, &path); err == nil {
		*md = Meta{Path: path}
		return nil
	}

	d := _metadata{}
	if err := json.Unmarshal(data, &d); err != nil {
		return fmt.Errorf("error unmarshling dataset metadata: %s", err.Error())
	}

	meta := map[string]interface{}{}
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("error unmarshaling dataset metadata: %s", err)
	}

	for _, f := range []string{
		"accessURL",
		"accrualPeriodicity",
		"citations",
		"contributors",
		"data",
		"description",
		"downloadURL",
		"homeURL",
		"identifier",
		"image",
		"keyword",
		"path",
		"qri",
		"language",
		"length",
		"license",
		"readmeURL",
		"theme",
		"timestamp",
		"title",
		"version",
	} {
		delete(meta, f)
	}

	if len(meta) > 0 {
		d.meta = meta
	}
	*md = Meta(d)
	return nil
}

// User is a placholder for talking about people, groups, organizations
type User struct {
	ID       string `json:"id,omitempty"`
	Fullname string `json:"name,omitempty"`
	Email    string `json:"email,omitempty"`
}

// Decode reads json.Umarshal-style data into a User
func (u *User) Decode(val interface{}) (err error) {
	msi, ok := val.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected map[string]interface{}")
	}
	if u.ID, err = strVal(msi["id"]); err != nil {
		return
	}
	if u.Fullname, err = strVal(msi["name"]); err != nil {
		return
	}
	if u.Email, err = strVal(msi["email"]); err != nil {
		return
	}
	return
}

// License represents a legal licensing agreement
type License struct {
	Type string `json:"type,omitempty"`
	URL  string `json:"url,omitempty"`
}

// Decode reads json.Umarshal-style data into a License
func (l *License) Decode(val interface{}) (err error) {
	msi, ok := val.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected map[string]interface{}")
	}
	if l.Type, err = strVal(msi["type"]); err != nil {
		return
	}
	if l.URL, err = strVal(msi["url"]); err != nil {
		return
	}

	return
}

// Citation is a place that this dataset drew it's information from
type Citation struct {
	Name  string `json:"name,omitempty"`
	URL   string `json:"url,omitempty"`
	Email string `json:"email,omitempty"`
}

// Decode reads json.Umarshal-style data into a Citation
func (c *Citation) Decode(val interface{}) (err error) {
	msi, ok := val.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected map[string]interface{}")
	}
	if c.Name, err = strVal(msi["name"]); err != nil {
		return
	}
	if c.URL, err = strVal(msi["url"]); err != nil {
		return
	}
	if c.Email, err = strVal(msi["email"]); err != nil {
		return
	}
	return
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

// AccuralDuration takes an ISO 8601 periodicity measure & returns a
// time.Duration invalid periodicities return time.Duration(0)
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
