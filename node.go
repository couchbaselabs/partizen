package partizen

import (
	"bytes"
	"errors"
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

var ErrCursorClosed = errors.New("cursor closed sentinel")

type CursorResult struct {
	err error
	ksl *KeySeqLoc
}

func (r *CollRoot) startCursor(key Key, ascending bool,
	partitionIds []PartitionId, readerAt io.ReaderAt,
	closeCh chan struct{}) (resultsCh chan CursorResult, err error) {
	if partitionIds != nil {
		return nil, fmt.Errorf("partitionsIds unimplemented")
	}

	resultsCh = make(chan CursorResult) // TODO: Channel buffer.

	r.store.m.Lock()
	kslr, ksl := r.RootKeySeqLocRef.AddRef()
	r.store.m.Unlock()

	var visit func(ksl *KeySeqLoc) error
	visit = func(ksl *KeySeqLoc) error {
		if ksl == nil {
			return nil
		}
		if ksl.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&ksl.Loc, readerAt)
			if err != nil {
				return err
			}
			if node == nil {
				return nil
			}
			ksls := node.GetKeySeqLocs()
			if ksls == nil {
				return nil
			}
			n := ksls.Len()
			if n <= 0 {
				return nil
			}
			i := sort.Search(n, func(i int) bool {
				return bytes.Compare(ksls.Key(i), key) >= 0
			})
			if !ascending && bytes.Compare(ksls.Key(i), key) > 0 {
				i = i - 1
			}
			for i >= 0 && i < n {
				err := visit(ksls.KeySeqLoc(i))
				if err != nil {
					return err
				}
				if ascending {
					i++
				} else {
					i--
				}
			}
			return nil
		}

		if ksl.Loc.Type == LocTypeVal {
			select {
			case <-closeCh:
				return ErrCursorClosed
			case resultsCh <- CursorResult{err: nil, ksl: ksl}:
				// TODO: Mem mgmt.
			}
			return nil
		}

		return fmt.Errorf("startCursor.visit:",
			" unexpected Loc.Type, ksl: %#v", ksl)
	}

	go func() {
		err := visit(ksl)

		if kslr != nil {
			r.store.m.Lock()
			kslr.DecRef()
			r.store.m.Unlock()
		}

		if err != nil && err != ErrCursorClosed {
			resultsCh <- CursorResult{err: err}
		}
		close(resultsCh)
	}()

	return resultsCh, nil
}
