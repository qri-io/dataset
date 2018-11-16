// Package subset provides methods for extracting defined abbreviations of a
// dataset document. Datasets can theoretically be any size, subset lets us
// take pieces of a dataset, and use names to quickly identify what kind of sizes
// we can expect while being clear on what info each subset is forgoing.
//
// The full cascade of subsets from smallest to largest is as follows:
// * hash - the content-addressed dataset identifier
// * reference - a dataset name + human-friendly name
// * preview - a short description of a dataset indended for listing datasets
// * summary - a subsection of a dataset, including a bounded subset of body, meta, viz, script
// * head - all dataset content except the body
// * document - the full dataset document
// * history - the full dataset document and all previous verions of a dataset
//
// subset currently provides methods for creating previews and summaries, and heads
//
// This package is currently a working proof-of-concept, with a more thorough
// version coming after we ratify an RFC on dataset abbreviation
package subset

import (
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
)

// LoadPreview loads a dataset preview for a given hash path
func LoadPreview(s cafs.Filestore, path string) (*dataset.DatasetPod, error) {
	// TODO - this is overfetching. Refine.
	ds, err := dsfs.LoadDataset(s, datastore.NewKey(path))
	if err != nil {
		return nil, err
	}
	return Preview(ds.Encode()), nil
}

// Preview creates a new preview from a given dataset
// dataset preivews contain the entire contents of commit, with selected fields from meta & structure
// preview is intended to be used when listing dataset, containing important details
// previews also contain all information necessary to verify the commit signature
func Preview(ds *dataset.DatasetPod) *dataset.DatasetPod {
	return &dataset.DatasetPod{
		Path:         ds.Path,
		Name:         ds.Name,
		Peername:     ds.Peername,
		Commit:       ds.Commit,
		Meta:         previewMeta(ds.Meta),
		Structure:    previewStructure(ds.Structure),
		PreviousPath: ds.PreviousPath,
	}
}

func previewMeta(md *dataset.Meta) *dataset.Meta {
	if md == nil {
		return nil
	}

	return &dataset.Meta{
		Title:       md.Title,
		Description: md.Description,
		Theme:       md.Theme,
		Keywords:    md.Keywords,
	}
}

func previewStructure(st *dataset.StructurePod) *dataset.StructurePod {
	if st == nil {
		return nil
	}
	return &dataset.StructurePod{
		Format:   st.Format,
		Length:   st.Length,
		ErrCount: st.ErrCount,
		Entries:  st.Entries,
		Checksum: st.Checksum,
	}
}
