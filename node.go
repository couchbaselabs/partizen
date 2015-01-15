package partizen

import (
	"fmt"
)

func (r *RootLoc) NodeGet(n Node, partitionId PartitionId,
	key Key, withValue bool) (
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
		panic("NodeGet: n.ChildLoc() returned nil")
	}
	if cl.Type == LocTypeNode {
		return r.NodeGet(cl.node, partitionId, key, withValue)
	}
	if cl.Type == LocTypeVal {
		return keySeqIdx.Seq, cl.buf, nil // TODO: Buffer mgmt.
	}
	return 0, nil, fmt.Errorf("unexpected child node type: %d", cl.Type)
}

func (r *RootLoc) NodeSet(n Node, partitionId PartitionId,
	key Key, seq Seq, val Val) (
	Node, error) {
	if n == nil {
		return makeNodeMem(LocTypeVal, partitionId, key, seq, val)
	}
	found, nodePartitionIdx := n.LocateNodePartition(partitionId)
	if !found {
		if n.IsLeaf(true) {
			return n.InsertChildLoc(partitionId, nodePartitionIdx, 0,
				key, seq, Loc{
					Size: uint32(len(val)),
					Type: LocTypeVal,
					buf:  val,
				}), nil
		}
	}
	found, nodePartitionKeyIdx, keySeqIdx :=
		n.LocateKeySeqIdx(nodePartitionIdx, key)
	if !found {
		return n.InsertChildLoc(partitionId,
			nodePartitionIdx, nodePartitionKeyIdx,
			key, seq, Loc{
				Size: uint32(len(val)),
				Type: LocTypeVal,
				buf:  val,
			}), nil
	}
	cl := n.ChildLoc(int(keySeqIdx.Idx))
	if cl == nil {
		panic("NodeSet: n.ChildLoc() returned nil")
	}
	if cl.Type == LocTypeNode {
		return r.NodeSet(cl.node, partitionId, key, seq, val)
	}
	if cl.Type == LocTypeVal {
		return n.UpdateChildLoc(partitionId,
			nodePartitionIdx, nodePartitionKeyIdx,
			seq, Loc{
				Size: uint32(len(val)),
				Type: LocTypeVal,
				buf:  val,
			}), nil
	}
	return nil, fmt.Errorf("todo")
}
