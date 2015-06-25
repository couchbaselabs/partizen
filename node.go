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
		if loc.bufRef.IsNil() {
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

func locateItemLoc(ksl *ItemLoc, key Key,
	bufManager BufManager, r io.ReaderAt) (*ItemLoc, error) {
	for ksl != nil {
		if ksl.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&ksl.Loc, bufManager, r)
			if err != nil {
				return nil, err
			}
			if node == nil {
				return nil, nil
			}
			ksls := node.GetItemLocs()
			if ksls == nil {
				return nil, nil
			}
			n := ksls.Len()
			if n <= 0 {
				return nil, nil
			}
			i := sort.Search(n, func(i int) bool {
				return bytes.Compare(ksls.Key(i), key) >= 0
			})
			if i >= n || !bytes.Equal(ksls.Key(i), key) {
				return nil, nil
			}
			ksl = ksls.ItemLoc(i)
		} else if ksl.Loc.Type == LocTypeVal {
			if bytes.Equal(ksl.Key, key) {
				return ksl, nil
			}
			return nil, nil
		} else {
			return nil, fmt.Errorf("locateItemLoc")
		}
	}
	return nil, nil
}

func locateMinMax(ksl *ItemLoc, wantMax bool,
	bufManager BufManager, r io.ReaderAt) (*ItemLoc, error) {
	for ksl != nil {
		if ksl.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&ksl.Loc, bufManager, r)
			if err != nil {
				return nil, err
			}
			if node == nil {
				return nil, nil
			}
			ksls := node.GetItemLocs()
			if ksls == nil {
				return nil, nil
			}
			n := ksls.Len()
			if n <= 0 {
				return nil, nil
			}
			if wantMax {
				ksl = ksls.ItemLoc(n - 1)
			} else {
				ksl = ksls.ItemLoc(0)
			}
		} else if ksl.Loc.Type == LocTypeVal {
			return ksl, nil
		} else {
			return nil, fmt.Errorf("locateMinMax")
		}
	}
	return nil, nil
}
