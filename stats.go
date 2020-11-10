package dataset

type Stats struct {
	Qri   Kind        `json:"qri,omitempty"`
	Stats interface{} `json:"stats,omitempty"`
	Path  string      `json:"path,omitempty"`
}

// NewStatsRef creates an empty struct with it's path set
func NewStatsRef(path string) *Stats {
	return &Stats{Path: path}
}

// type Stat struct {
// 	Type   string `json:"type,omitempty"`
// 	Fields map[string]interface{}
// }

// DropDerivedValues resets all set-on-save fields to their default values
func (st *Stats) DropDerivedValues() {
	st.Qri = ""
	st.Path = ""
}
