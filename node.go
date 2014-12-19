package partizen

import (
	"bytes"
	"fmt"
	"sort"
)

func (n *Node) Get(r *RootLoc, partitionId PartitionId, key Key, withValue bool) (
	seq Seq, val Val, err error) {
	if n == nil {
		return 0, nil, nil
	}
	resNode, _, _, keySeqIdx, childLocIdx, err := n.GetIdxs(r, partitionId, key)
	if err != nil {
		return 0, nil,  err
	}
	if resNode == nil {
		return 0, nil, nil
	}
	cl := &resNode.ChildLocs[childLocIdx]
	if cl.Type == LocTypeNode {
		return cl.node.Get(r, partitionId, key, withValue)
	}
	if cl.Type == LocTypeVal {
		return resNode.KeySeqIdxs[keySeqIdx].Seq, cl.buf, nil // TODO: Buffer mgmt.
	}
	return 0, nil, fmt.Errorf("unexpected child node type: %d", cl.Type)
}

func (n *Node) GetIdxs(r *RootLoc, partitionId PartitionId, key Key) (
	resNode *Node,
	nodePartitionIdx int,
	nodePartitionKeyIdx int,
	keySeqIdx int,
	childLocIdx int,
	err error) {
	nodePartitionIdx = sort.Search(len(n.NodePartitions),
		func(i int) bool {
			return n.NodePartitions[i].PartitionId >= partitionId
		})
	if nodePartitionIdx >= len(n.NodePartitions) {
		return nil, 0, 0, 0, 0, nil
	}
	p := &n.NodePartitions[nodePartitionIdx]
	nodePartitionKeyIdx = sort.Search(len(p.KeyIdxs),
		func(k int) bool {
			return bytes.Compare(n.KeySeqIdxs[int(p.KeyIdxs[k])].Key, key) >= 0
		})
	if nodePartitionKeyIdx >= len(p.KeyIdxs) {
		return nil, 0, 0, 0, 0, nil
	}
	keySeqIdx = int(p.KeyIdxs[nodePartitionKeyIdx])
	if keySeqIdx >= len(n.KeySeqIdxs) {
		return nil, 0, 0, 0, 0, fmt.Errorf("unexpected keyIdx >= len(n.KeySeqIdxs)")
	}
	childLocIdx = int(n.KeySeqIdxs[keySeqIdx].Idx)
	if childLocIdx >= len(n.ChildLocs) {
		return nil, 0, 0, 0, 0, fmt.Errorf("unexpected childLocIdx >= len(n.ChildLocs)")
	}
	return n, nodePartitionIdx, nodePartitionKeyIdx, keySeqIdx, childLocIdx, nil
}

func (n *Node) Set(r *RootLoc, partitionId PartitionId,
	key Key, seq Seq, val Val) (*Node, error) {
	if n == nil {
		return makeValNode(partitionId, key, seq, val)
	}

	_, _, _, _, _, err := n.GetIdxs(r, partitionId, key)
	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("todo")
}

func makeValNode(partitionId PartitionId, key Key, seq Seq, val Val) (
	*Node, error) {
	return &Node{ // TODO: Memory mgmt.
		ChildLocs: []Loc{
			Loc{
				Offset: 0,
				Size:   uint32(len(val)),
				Type:   LocTypeVal,
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
