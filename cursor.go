package partizen

import (
	"bytes"
	"fmt"
	"io"
	"sort"
)

type CursorImpl struct {
	bufManager BufManager
	closeCh    chan struct{}
	resultsCh  chan CursorResult
}

type CursorResult struct {
	err error
	ksl *ItemLoc
}

// --------------------------------------------

func (c *CursorImpl) Close() error {
	close(c.closeCh)
	return nil
}

func (c *CursorImpl) Next() (
	PartitionId, Key, Seq, Val, error) {
	partitionId, key, seq, bufRef, err := c.NextBufRef()
	if err != nil || bufRef == nil {
		return 0, nil, 0, nil, err
	}

	val := FromBufRef(nil, bufRef, c.bufManager)

	bufRef.DecRef(c.bufManager)

	return partitionId, key, seq, val, nil
}

func (c *CursorImpl) NextBufRef() (
	PartitionId, Key, Seq, BufRef, error) {
	r, ok := <-c.resultsCh
	if !ok {
		return 0, nil, 0, nil, nil // TODO: PartitionId.
	}

	partitionId := PartitionId(0) // TODO: PartitionId.

	return partitionId, r.ksl.Key, r.ksl.Seq,
		r.ksl.Loc.BufRef(c.bufManager), r.err
}

// --------------------------------------------

func (r *collection) startCursor(key Key, ascending bool,
	partitionIds []PartitionId, readerAt io.ReaderAt,
	closeCh chan struct{}) (resultsCh chan CursorResult, err error) {
	if partitionIds != nil {
		return nil, fmt.Errorf("partitionsIds unimplemented")
	}

	resultsCh = make(chan CursorResult) // TODO: Channel buffer.

	kslr, ksl := r.rootAddRef()

	var visit func(ksl *ItemLoc) error
	visit = func(ksl *ItemLoc) error {
		if ksl == nil {
			return nil
		}
		if ksl.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&ksl.Loc,
				r.store.bufManager, readerAt)
			if err != nil {
				return err
			}
			if node == nil {
				return nil
			}
			ksls := node.GetItemLocs()
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
			if !ascending &&
				(i >= n || bytes.Compare(ksls.Key(i), key) > 0) {
				i = i - 1
			}
			for i >= 0 && i < n {
				err := visit(ksls.ItemLoc(i))
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

		r.rootDecRef(kslr)

		if err != nil && err != ErrCursorClosed {
			resultsCh <- CursorResult{err: err}
		}

		close(resultsCh)
	}()

	return resultsCh, nil
}
