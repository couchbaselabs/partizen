package partizen

import (
	"fmt"
)

func (n *Node) Get(partitionID PartitionID,
	key Key,
	withValue bool, // When withValue is false, value will be nil.
	fastSample bool) ( // Return result only if fast / in memory (no disk hit).
	seq Seq, val Val, err error) {
	if n == nil {
		return 0, nil, nil
	}
	return 0, nil, fmt.Errorf("todo")
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
