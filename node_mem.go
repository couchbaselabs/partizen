package partizen

import (
	"bytes"
	"sort"
)

func (n *NodeMem) LocateNodePartition(partitionId PartitionId) (
	found bool, nodePartitionIdx int) {
	nodePartitionIdx = sort.Search(len(n.NodePartitions),
		func(i int) bool {
			return n.NodePartitions[i].PartitionId >= partitionId
		})
	if nodePartitionIdx >= len(n.NodePartitions) {
		return false, nodePartitionIdx
	}
	// TODO: Optimize away this extra comparison.
	nodePartition := &n.NodePartitions[nodePartitionIdx]
	if nodePartition.PartitionId != partitionId {
		return false, nodePartitionIdx
	}
	return true, nodePartitionIdx
}

func (n *NodeMem) LocateKeySeqIdx(nodePartitionIdx int, key Key) (
	found bool, nodePartitionKeyIdx int, keySeqIdx KeySeqIdx) {
	if nodePartitionIdx >= len(n.NodePartitions) {
		panic("nodePartitionIdx >= len(n.NodePartitions)")
	}
	np := &n.NodePartitions[nodePartitionIdx]
	nodePartitionKeyIdx = sort.Search(len(np.KeyIdxs),
		func(k int) bool {
			keySeqIdx := &n.KeySeqIdxs[int(np.KeyIdxs[k])]
			return bytes.Compare(keySeqIdx.Key, key) >= 0
		})
	if nodePartitionKeyIdx >= len(np.KeyIdxs) {
		return false, nodePartitionKeyIdx, keySeqIdx
	}
	// TODO: Optimize away this extra comparison.
	keySeqIdx = n.KeySeqIdxs[int(np.KeyIdxs[nodePartitionKeyIdx])]
	if bytes.Compare(keySeqIdx.Key, key) != 0 {
		return false, nodePartitionKeyIdx, keySeqIdx
	}
	return true, nodePartitionKeyIdx, keySeqIdx
}

func (n *NodeMem) ChildLoc(childLocIdx int) *Loc {
	if childLocIdx < 0 {
		panic("childLocIdx < 0")
	}
	if childLocIdx >= len(n.ChildLocs) {
		return nil
	}
	return &n.ChildLocs[childLocIdx]
}

func (n *NodeMem) IsLeaf(defaultVal bool) bool {
	if len(n.ChildLocs) > 0 {
		return n.ChildLocs[0].Type == LocTypeVal
	}
	return defaultVal
}

func (n *NodeMem) InsertChildLoc(partitionId PartitionId,
	nodePartitionIdx, nodePartitionKeyIdx int, key Key, seq Seq, loc Loc) Node {
	npa := make([]NodePartition, len(n.NodePartitions) + 1)
	copy(npa[:nodePartitionIdx], n.NodePartitions[:nodePartitionIdx])
	npa[nodePartitionIdx] = NodePartition{
		PartitionId: partitionId,
		KeyIdxs:     []uint16{666},
	}
	copy(npa[nodePartitionIdx+1:], n.NodePartitions[nodePartitionIdx:])

	rv := &NodeMem{
		ChildLocs:      n.ChildLocs,
		KeySeqIdxs:     n.KeySeqIdxs,
		NodePartitions: npa,
	}
	return rv
}

func (n *NodeMem) UpdateChildLoc(partitionId PartitionId,
	nodePartitionIdx, nodePartitionKeyIdx int, seq Seq, loc Loc) Node {
	return nil
}

func makeNodeMem(locType uint8, partitionId PartitionId, key Key, seq Seq, val Val) (
	Node, error) {
	return &NodeMem{ // TODO: Memory mgmt.
		ChildLocs: []Loc{
			Loc{
				Offset: 0,
				Size:   uint32(len(val)),
				Type:   locType,
				buf:    val,
			}},
		KeySeqIdxs: []KeySeqIdx{
			KeySeqIdx{
				Key: key,
				Seq: seq,
				Idx: 0,
			}},
		NodePartitions: []NodePartition{
			NodePartition{
				PartitionId: partitionId,
				TotKeys:     1,
				TotVals:     1,
				TotKeyBytes: uint64(len(key)),
				TotValBytes: uint64(len(val)),
				KeyIdxs:     []uint16{0},
			}},
	}, nil
}
