package partizen

import (
	"bytes"
	"fmt"
	"io"
	"sort"
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
		if n.IsLeaf() {
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

// --------------------------------------------

func locateKeySeqLoc(ksl *KeySeqLoc, key Key, r io.ReaderAt) (
	*KeySeqLoc, error) {
	for ksl != nil {
		if ksl.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&ksl.Loc, r)
			if err != nil {
				return nil, err
			}
			if node == nil {
				return nil, nil
			}
			ksls := node.GetKeySeqLocs()
			if ksls == nil {
				return nil, nil
			}
			n := ksls.Len()
			c := 0
			i := sort.Search(n, func(i int) bool {
				c = bytes.Compare(ksls.Key(i), key)
				return c >= 0
			})
			if i >= n || c > 0 {
				return nil, nil
			}
			ksl = ksls.KeySeqLoc(i)
		} else if ksl.Loc.Type == LocTypeVal {
			if bytes.Equal(ksl.Key, key) {
				return ksl, nil
			}
			return nil, nil
		} else {
			return nil, fmt.Errorf("locateKeySeqLoc")
		}
	}
	return nil, nil
}
