package dsgraph

var walkParallelism = 4

// NodeType specifies different types of qri nodes
type NodeType string

var (
	// NtDataset is a holistic reference to a dataset,
	// aka the base hash of a dataset
	NtDataset = NodeType("dataset")
	// NtMetadata is the dataset.json file in a dataset
	NtMetadata = NodeType("metadata")
	// NtCommit is the commit.json file in a dataset
	NtCommit = NodeType("commit")
	// NtData is a dataset's raw data
	NtData = NodeType("data")
	// NtQuery is the query.json in a dataset
	NtQuery = NodeType("query")
	// NtAbstQuery is the abstract_query.json in a dataset
	NtAbstQuery = NodeType("abst_query")
	// NtStructure is the structure.json in a dataset
	NtStructure = NodeType("structure")
	// NtAbstStructure is the abstract_structure.json in a dataset
	NtAbstStructure = NodeType("abst_structure")
	// NtNamespace is the namespace of a single qri repository
	NtNamespace = NodeType("namespace")
)

// Node is a typed reference to a path
type Node struct {
	Type  NodeType
	Path  string
	Links []Link
}

// Equal checks for field-level equality with another Node
func (n Node) Equal(b *Node) bool {
	return n.Type == b.Type && n.Path == b.Path
}

// AddLinks is a no-duplicates method for adding one or more links to a node
func (n *Node) AddLinks(links ...Link) {
ADDITIONS:
	for _, link := range links {
		for _, l := range n.Links {
			if link.To.Path == "" || link.Equal(l) {
				continue ADDITIONS
			}
		}
		n.Links = append(n.Links, link)
	}
}

// TODO - still considering if links need to be typed or not
// type LinkType string

// var (
// 	LtPrevious      = LinkType("previous")
// 	LtResource      = LinkType("resource")
// 	LtDsData        = LinkType("dataset_data")
// 	LtDsCommit      = LinkType("dataset_commit")
// 	LtAbstStructure = LinkType("abst_structure")
// 	LtQuery         = LinkType("query")
// 	LtAbstQuery     = LinkType("abst_query")
// 	LtNamespaceTip  = LinkType("namespace_tip")
// )

// Link is a typed, directional connection from one
// node to another
type Link struct {
	// Type     LinkType
	From, To *Node
}

// Equal checks for field-level equality with another Link
func (a Link) Equal(b Link) bool {
	return a.From.Equal(b.From) && a.To.Equal(b.To)
}

// FilterNodeTypes returns a slice of node pointers from a graph that match
// the provided NodeType's
func FilterNodeTypes(graph *Node, nodetypes ...NodeType) (nodes []*Node) {
	Walk(graph, 0, func(n *Node) error {
		if n != nil {
			for _, nt := range nodetypes {
				if n.Type == nt {
					nodes = append(nodes, n)
					break
				}
			}
		}
		return nil
	})
	return
}

// Walk visits node and all descendants with a provided visit function
func Walk(node *Node, depth int, visit func(n *Node) error) error {
	if err := visit(node); err != nil {
		return err
	}
	for _, l := range node.Links {
		if err := Walk(l.To, depth+1, visit); err != nil {
			return err
		}
	}
	return nil
}
