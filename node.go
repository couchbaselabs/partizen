package partizen

import (
	"bytes"
	"fmt"
	"sort"
)

func (n *Node) Get(r *RootLoc, partitionId PartitionId,
	key Key,
	withValue bool, // When withValue is false, value will be nil.
	fastSample bool) ( // Return result only if fast / in memory (no disk hit).
	seq Seq, val Val, err error) {
	if n == nil {
		return 0, nil, nil
	}
	i := sort.Search(len(n.NodePartitions),
		func(i int) bool {
			return n.NodePartitions[i].PartitionId >= partitionId
		})
	if i >= len(n.NodePartitions) {
		return 0, nil, nil
	}
	p := &n.NodePartitions[i]
	k := sort.Search(len(p.KeyIdxs),
		func(k int) bool {
			return bytes.Compare(n.KeySeqIdxs[int(p.KeyIdxs[k])].Key, key) >= 0
		})
	if k >= len(p.KeyIdxs) {
		return 0, nil, nil
	}
	ki := int(p.KeyIdxs[k])
	if ki >= len(n.KeySeqIdxs) {
		return 0, nil, fmt.Errorf("unexpected ki >= len(n.KeySeqIdxs)")
	}
	ksi := &n.KeySeqIdxs[ki]
	if int(ksi.Idx) >= len(n.ChildLocs) {
		return 0, nil, fmt.Errorf("unexpected ksi.Idx >= len(n.ChildLocs)")
	}
	cl := &n.ChildLocs[int(ksi.Idx)]
	if cl.Type == LocTypeNode {
		return cl.node.Get(r, partitionId, key, withValue, fastSample)
	}
	if cl.Type == LocTypeVal {
		return ksi.Seq, cl.buf, nil // TODO: Buffer mgmt.
	}
	return 0, nil, fmt.Errorf("unexpected child node type: %d", cl.Type)
}

func (n *Node) Set(r *RootLoc, partitionId PartitionId,
	key Key, seq Seq, val Val) (*Node, error) {
	if n == nil {
		return makeValNode(partitionId, key, seq, val)
	}

	return nil, fmt.Errorf("todo")
}

func makeValNode(partitionId PartitionId, key Key, seq Seq, val Val) (*Node, error) {
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
