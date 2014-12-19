package partizen

import (
	"bytes"
	"fmt"
	"sort"
)

func (r *RootLoc) NodeGet(n Node, partitionId PartitionId, key Key, withValue bool) (
	seq Seq, val Val, err error) {
	if n == nil {
		return 0, nil, nil
	}
	found, nodePartitionIdx := n.LocateNodePartition(partitionId)
	if !found {
		return 0, nil, nil
	}
	found, _, keySeqIdx := n.LocateKeySeqIdx(nodePartitionIdx, key)
	if !found {
		return 0, nil, nil
	}
	cl := n.ChildLoc(int(keySeqIdx.Idx))
	if cl == nil {
		return 0, nil, fmt.Errorf("missing child node: %d", keySeqIdx.Idx)
	}
	if cl.Type == LocTypeNode {
		return r.NodeGet(cl.node, partitionId, key, withValue)
	}
	if cl.Type == LocTypeVal {
		return keySeqIdx.Seq, cl.buf, nil // TODO: Buffer mgmt.
	}
	return 0, nil, fmt.Errorf("unexpected child node type: %d", cl.Type)
}

func (r *RootLoc) NodeSet(n Node, partitionId PartitionId, key Key, seq Seq, val Val) (
	Node, error) {
	if n == nil {
		return makeNodeMem(LocTypeVal, partitionId, key, seq, val)
	}
	return nil, fmt.Errorf("todo")
}

// ----------------------------------------------------------------------

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
	found bool, nodePartitionKeyIdx int, keySeqIdx *KeySeqIdx) {
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
		return false, nodePartitionKeyIdx, nil
	}
	// TODO: Optimize away this extra comparison.
	keySeqIdx = &n.KeySeqIdxs[int(np.KeyIdxs[nodePartitionKeyIdx])]
	if bytes.Compare(keySeqIdx.Key, key) != 0 {
		return false, nodePartitionKeyIdx, nil
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
