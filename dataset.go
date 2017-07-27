package dataset

// Current version of the specification
const version = "0.0.1"

// Dataset combines Metadata & Resource to form a "full" description
type Dataset struct {
	Metadata
	Resource
}
