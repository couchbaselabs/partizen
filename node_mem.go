package partizen

import (
	"bytes"
	"sort"
)

type NodeMem struct {
	// NumChildLocs      uint8
	// NumKeySeqs        uint8
	// NumNodePartitions uint16

	// ChildLocs are kept separate from Partitions because multiple
	// NodePartition.KeySeq's may be sharing or indexing to the same
	// ChildLoc entries.
	//
	// TODO: Consider ordering ChildLocs by ChildLoc.Offset?
	ChildLocs []Loc // See MAX_CHILD_LOCS_PER_NODE.

	// Sorted by KeySeqIdx.Key.
	KeySeqIdxs []KeySeqIdx

	// Sorted by NodePartition.PartitionId.
	NodePartitions []NodePartition // See MAX_NODE_PARTITIONS_PER_NODE.

	// -----------------------------------

	KeySeqLocs KeySeqLocs
}

// A NodePartition is a variable-sized struct that holds keys of
// direct descendants of a Partition for a Node.
type NodePartition struct {
	PartitionId PartitionId
	TotKeys     uint64
	TotVals     uint64
	TotKeyBytes uint64
	TotValBytes uint64
	KeyIdxs     []uint16 // Indexes into the Node.KeySeqIdxs array.
}

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

func (n *NodeMem) NumChildren() int {
	return len(n.ChildLocs)
}

func (n *NodeMem) IsLeaf() bool {
	if len(n.ChildLocs) > 0 {
		return n.ChildLocs[0].Type == LocTypeVal
	}
	return false
}

func (n *NodeMem) GetKeySeqLocs() KeySeqLocs {
	return n.KeySeqLocs
}

func (n *NodeMem) InsertChildLoc(partitionId PartitionId,
	nodePartitionIdx, nodePartitionKeyIdx int,
	key Key, seq Seq, loc Loc) Node {
	npa := make([]NodePartition, len(n.NodePartitions)+1)
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

func makeNodeMem(locType uint8, partitionId PartitionId,
	key Key, seq Seq, val Val) (
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
