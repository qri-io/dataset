// Package subset provides methods for extracting defined abbreviations of a
// dataset document. Datasets can theoretically be any size, subset lets us
// take pieces of a dataset, and use names to quickly identify what kind of sizes
// we can expect while being clear on what info each subset is forgoing.
//
// The full cascade of subsets from smallest to largest is as follows:
// * hash - the content-addressed dataset identifier
// * reference - a dataset name + human-friendly name
// * preview - a fixed size description of a dataset, with fields from meta & structure. indended for listing datasets with important details
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
func Preview(ds *dataset.DatasetPod) *dataset.DatasetPod {
	return &dataset.DatasetPod{
		Path:         ds.Path,
		Name:         ds.Name,
		Peername:     ds.Peername,
		Commit:       previewCommit(ds.Commit),
		Meta:         previewMeta(ds.Meta),
		Structure:    previewStructure(ds.Structure),
		PreviousPath: ds.PreviousPath,
	}
}

func previewCommit(cm *dataset.CommitPod) *dataset.CommitPod {
	// TODO - consider removing longer fields like signature
	if cm == nil {
		return nil
	}

	return &dataset.CommitPod{
		Timestamp: cm.Timestamp,
		Title:     cm.Title,
		Author:    cm.Author,
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
	}
}
