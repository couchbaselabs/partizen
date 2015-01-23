package partizen

type NodeMem struct {
	KeySeqLocs KeySeqLocs
}

func (n *NodeMem) GetKeySeqLocs() KeySeqLocs {
	return n.KeySeqLocs
}
