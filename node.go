package partizen

import (
	"bytes"
	"fmt"
	"io"
	"sort"
)

type node struct {
	itemLocs   ItemLocs
	partitions *Partitions
}

func (n *node) GetItemLocs() ItemLocs {
	return n.itemLocs
}

func (n *node) GetPartitions() *Partitions {
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
	*node, error) {
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

func locateItemLoc(il *ItemLoc, key Key, compareFunc CompareFunc,
	bufManager BufManager, r io.ReaderAt) (*ItemLoc, error) {
	for il != nil {
		if il.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&il.Loc, bufManager, r)
			if err != nil {
				return nil, err
			}
			if node == nil {
				return nil, nil
			}
			ils := node.GetItemLocs()
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
			il = ils.ItemLoc(i)
		} else if il.Loc.Type == LocTypeVal {
			if bytes.Equal(il.Key, key) {
				return il, nil
			}
			return nil, nil
		} else {
			return nil, fmt.Errorf("locateItemLoc")
		}
	}
	return nil, nil
}

func locateMinMax(il *ItemLoc, wantMax bool,
	bufManager BufManager, r io.ReaderAt) (*ItemLoc, error) {
	for il != nil {
		if il.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&il.Loc, bufManager, r)
			if err != nil {
				return nil, err
			}
			if node == nil {
				return nil, nil
			}
			ils := node.GetItemLocs()
			if ils == nil {
				return nil, nil
			}
			n := ils.Len()
			if n <= 0 {
				return nil, nil
			}
			if wantMax {
				il = ils.ItemLoc(n - 1)
			} else {
				il = ils.ItemLoc(0)
			}
		} else if il.Loc.Type == LocTypeVal {
			return il, nil
		} else {
			return nil, fmt.Errorf("locateMinMax")
		}
	}
	return nil, nil
}
