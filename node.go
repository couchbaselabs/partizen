package partizen

import (
	"bytes"
	"fmt"
	"io"
	"sort"
)

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
			i := sort.Search(n, func(i int) bool {
				return bytes.Compare(ksls.Key(i), key) >= 0
			})
			if i >= n || !bytes.Equal(ksls.Key(i), key) {
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

func locateMinMax(ksl *KeySeqLoc, locateMax bool, r io.ReaderAt) (
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
			if n <= 0 {
				return nil, nil
			}
			if locateMax {
				ksl = ksls.KeySeqLoc(n - 1)
			} else {
				ksl = ksls.KeySeqLoc(0)
			}
		} else if ksl.Loc.Type == LocTypeVal {
			return ksl, nil
		} else {
			return nil, fmt.Errorf("locateMinMax")
		}
	}
	return nil, nil
}
