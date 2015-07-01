package partizen

import (
	"bytes"
	"fmt"
	"io"
	"sort"
)

type Node struct {
	ksLocs     KeySeqLocs
	partitions *Partitions
}

func (n *Node) GetKeySeqLocs() KeySeqLocs {
	return n.ksLocs
}

func (n *Node) GetPartitions() *Partitions {
	return n.partitions
}

// ----------------------------------------------------

func (loc *Loc) Read(bufManager BufManager, r io.ReaderAt) (
	*Loc, error) {
	// TODO: Need locking for swizzling.

	if loc.Type == LocTypeNode {
		if loc.node == nil {
			return nil, fmt.Errorf("node: Read loc type node TODO")
		}
		return loc, nil
	}

	if loc.Type == LocTypeVal {
		if loc.leafValBufRef == nil || loc.leafValBufRef.IsNil() {
			return nil, fmt.Errorf("node: Read loc type val TODO")
		}
		return loc, nil
	}

	return nil, fmt.Errorf("node: Read unreadable type, loc: %v", loc)
}

func ReadLocNode(loc *Loc, bufManager BufManager, r io.ReaderAt) (
	*Node, error) {
	loc, err := loc.Read(bufManager, r)
	if err != nil {
		return nil, err
	}

	if loc.node == nil {
		return nil, fmt.Errorf("node: ReadLocNode, no node")
	}

	return loc.node, nil
}

// ----------------------------------------------------

func locateKeySeqLoc(il *KeySeqLoc, key Key, compareFunc CompareFunc,
	bufManager BufManager, r io.ReaderAt) (*KeySeqLoc, error) {
	for il != nil {
		if il.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&il.Loc, bufManager, r)
			if err != nil {
				return nil, err
			}
			if node == nil {
				return nil, nil
			}
			ils := node.GetKeySeqLocs()
			if ils == nil {
				return nil, nil
			}
			n := ils.Len()
			if n <= 0 {
				return nil, nil
			}
			i := sort.Search(n, func(i int) bool {
				return compareFunc(ils.Key(i), key) >= 0
			})
			if i >= n || compareFunc(ils.Key(i), key) != 0 {
				return nil, nil
			}
			il = ils.KeySeqLoc(i)
		} else if il.Loc.Type == LocTypeVal {
			if bytes.Equal(il.Key, key) {
				return il, nil
			}
			return nil, nil
		} else {
			return nil, fmt.Errorf("locateKeySeqLoc")
		}
	}
	return nil, nil
}

func locateMinMax(il *KeySeqLoc, wantMax bool,
	bufManager BufManager, r io.ReaderAt) (*KeySeqLoc, error) {
	for il != nil {
		if il.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&il.Loc, bufManager, r)
			if err != nil {
				return nil, err
			}
			if node == nil {
				return nil, nil
			}
			ils := node.GetKeySeqLocs()
			if ils == nil {
				return nil, nil
			}
			n := ils.Len()
			if n <= 0 {
				return nil, nil
			}
			if wantMax {
				il = ils.KeySeqLoc(n - 1)
			} else {
				il = ils.KeySeqLoc(0)
			}
		} else if il.Loc.Type == LocTypeVal {
			return il, nil
		} else {
			return nil, fmt.Errorf("locateMinMax")
		}
	}
	return nil, nil
}
