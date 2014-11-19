package partizen

import (
	"bytes"
	"fmt"
	"sort"
)

func (n *Node) Get(partitionID PartitionID,
	key Key,
	withValue bool, // When withValue is false, value will be nil.
	fastSample bool) ( // Return result only if fast / in memory (no disk hit).
	seq Seq, val Val, err error) {
	if n == nil {
		return 0, nil, nil
	}
	i := sort.Search(int(n.NumPartitions),
		func(i int) bool { return n.PartitionIdxs[i].PartitionID >= partitionID })
	if i >= int(n.NumPartitions) {
		return 0, nil, nil
	}
	if i >= len(n.Partitions) {
		fmt.Errorf("unexpected i >= len(n.Partitions)")
	}
	p := &n.Partitions[i]
	k := sort.Search(int(p.NumKeySeqs),
		func(k int) bool { return bytes.Compare(p.KeySeqs[k].Key, key) >= 0 })
	if k >= int(p.NumKeySeqs) {
		return 0, nil, nil
	}
	ks := p.KeySeqs[k]
	if int(ks.ChildLocsIdx) >= int(n.NumChildLocs) {
		return 0, nil, fmt.Errorf("unexpected ks.ChildLocsIdx >= n.NumChildLocs")
	}
	if int(ks.ChildLocsIdx) >= len(n.ChildLocs) {
		return 0, nil, fmt.Errorf("unexpected ks.ChildLocsIdx >= len(n.ChildLocs)")
	}
	c := &n.ChildLocs[ks.ChildLocsIdx]
	if c.Type == LocTypeNode {
		return c.node.Get(partitionID, key, withValue, fastSample)
	}
	if c.Type == LocTypeVal {
		return ks.Seq, c.buf, nil // TODO: Buffer mgmt.
	}
	return 0, nil, fmt.Errorf("unexpected child node type")
}

func (n *Node) Set(partitionID PartitionID, key Key, seq Seq, val Val) (*Node, error) {
	if n == nil {
		return &Node{
			NumChildLocs:  1,
			NumPartitions: 1,
			ChildLocs: append([]Loc(nil),
				Loc{Type: LocTypeVal, CheckSum: 0,
					Size: uint32(len(val)), Offset: 0, buf: val}),
			PartitionIdxs: append([]NodePartitionIdx(nil),
				NodePartitionIdx{
					PartitionID: partitionID,
					Offset:      0,
				}),
			Partitions: append([]NodePartition(nil),
				NodePartition{
					TotKeys:     1,
					TotVals:     1,
					TotKeyBytes: uint64(len(key)),
					TotValBytes: uint64(len(val)),
					NumKeySeqs:  1,
					KeySeqs: append([]KeySeq(nil),
						KeySeq{
							KeyLen:       uint16(len(key)),
							Seq:          seq,
							ChildLocsIdx: 0,
							Key:          key,
						}),
				}),
		}, nil
	}

	return nil, fmt.Errorf("todo")
}
