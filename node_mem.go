package partizen

type NodeMem struct {
	ItemLocs ItemLocs

	Partitions *Partitions
}

func (n *NodeMem) GetItemLocs() ItemLocs {
	return n.ItemLocs
}

func (n *NodeMem) GetPartitions() *Partitions {
	return n.Partitions
}
