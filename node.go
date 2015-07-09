package partizen

import (
	"bytes"
	"fmt"
	"io"
	"sort"
)

type Node struct {
	childLocs  ChildLocs
	partitions *Partitions
}

func (n *Node) GetChildLocs() ChildLocs {
	return n.childLocs
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
		if loc.itemBufRef == nil || loc.itemBufRef.IsNil() {
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

func locateChildLoc(il *ChildLoc, key Key, compareFunc CompareFunc,
	bufManager BufManager, r io.ReaderAt) (*ChildLoc, error) {
	for il != nil {
		if il.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&il.Loc, bufManager, r)
			if err != nil {
				return nil, err
			}
			if node == nil {
				return nil, nil
			}
			ils := node.GetChildLocs()
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
			il = ils.ChildLoc(i)
		} else if il.Loc.Type == LocTypeVal {
			if bytes.Equal(il.Key, key) {
				return il, nil
			}
			return nil, nil
		} else {
			return nil, fmt.Errorf("locateChildLoc")
		}
	}
	return nil, nil
}

func locateMinMax(il *ChildLoc, wantMax bool,
	bufManager BufManager, r io.ReaderAt) (*ChildLoc, error) {
	for il != nil {
		if il.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&il.Loc, bufManager, r)
			if err != nil {
				return nil, err
			}
			if node == nil {
				return nil, nil
			}
			ils := node.GetChildLocs()
			if ils == nil {
				return nil, nil
			}
			n := ils.Len()
			if n <= 0 {
				return nil, nil
			}
			if wantMax {
				il = ils.ChildLoc(n - 1)
			} else {
				il = ils.ChildLoc(0)
			}
		} else if il.Loc.Type == LocTypeVal {
			return il, nil
		} else {
			return nil, fmt.Errorf("locateMinMax")
		}
	}
	return nil, nil
}
