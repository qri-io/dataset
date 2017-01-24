package dataset

// Source is a place that this datapackage drew it's information from
type Source struct {
	Name  string `json:"name,omitempty"`
	Url   string `json:"url,omitempty"`
	Email string `json:"email,omitempty"`
}
