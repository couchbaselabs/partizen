package partizen

type NodeMem struct {
	ItemLocs ItemLocs
}

func (n *NodeMem) GetItemLocs() ItemLocs {
	return n.ItemLocs
}
