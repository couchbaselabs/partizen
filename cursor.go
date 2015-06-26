package partizen

import (
	"fmt"
	"io"
	"sort"
)

type cursorImpl struct {
	bufManager BufManager
	readerAt   io.ReaderAt
	closeCh    chan struct{}
	resultsCh  chan cursorResult
}

type cursorResult struct {
	err     error
	itemLoc *ItemLoc
}

// --------------------------------------------

func (c *cursorImpl) Close() error {
	close(c.closeCh)
	return nil
}

func (c *cursorImpl) Next() (
	PartitionId, Key, Seq, Val, error) {
	partitionId, key, seq, bufRef, err := c.NextBufRef()
	if err != nil || bufRef == nil {
		return 0, nil, 0, nil, err
	}

	val := FromBufRef(nil, bufRef, c.bufManager)

	bufRef.DecRef(c.bufManager)

	return partitionId, key, seq, val, nil
}

func (c *cursorImpl) NextBufRef() (
	PartitionId, Key, Seq, BufRef, error) {
	r, ok := <-c.resultsCh
	if !ok {
		return 0, nil, 0, nil, nil // TODO: zero/nil PartitionId.
	}

	loc, err := r.itemLoc.Loc.Read(c.bufManager, c.readerAt)
	if err != nil {
		return 0, nil, 0, nil, err
	}

	return loc.leafPartitionId, r.itemLoc.Key, r.itemLoc.Seq,
		loc.LeafValBufRef(c.bufManager), r.err
}

// --------------------------------------------

func (r *collection) startCursor(key Key, ascending bool,
	partitionIds []PartitionId, readerAt io.ReaderAt,
	closeCh chan struct{}, maxReadAhead int) (
	resultsCh chan cursorResult, err error) {
	if partitionIds != nil {
		return nil, fmt.Errorf("partitionsIds unimplemented")
	}

	resultsCh = make(chan cursorResult, maxReadAhead)

	itemLocRef, itemLoc := r.rootAddRef()

	var visit func(itemLoc *ItemLoc) error
	visit = func(itemLoc *ItemLoc) error {
		if itemLoc == nil {
			return nil
		}
		if itemLoc.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&itemLoc.Loc,
				r.store.bufManager, readerAt)
			if err != nil {
				return err
			}
			if node == nil {
				return nil
			}
			itemLocs := node.GetItemLocs()
			if itemLocs == nil {
				return nil
			}
			n := itemLocs.Len()
			if n <= 0 {
				return nil
			}
			i := sort.Search(n, func(i int) bool {
				return r.compareFunc(itemLocs.Key(i), key) >= 0
			})
			if !ascending &&
				(i >= n || r.compareFunc(itemLocs.Key(i), key) > 0) {
				i = i - 1
			}
			for i >= 0 && i < n {
				err := visit(itemLocs.ItemLoc(i))
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

		if itemLoc.Loc.Type == LocTypeVal {
			select {
			case <-closeCh:
				return ErrCursorClosed
			case resultsCh <- cursorResult{err: nil, itemLoc: itemLoc}:
				// TODO: Mem mgmt.
			}
			return nil
		}

		return fmt.Errorf("startCursor.visit:",
			" unexpected Loc.Type, itemLoc: %#v", itemLoc)
	}

	go func() {
		err := visit(itemLoc)

		r.rootDecRef(itemLocRef)

		if err != nil && err != ErrCursorClosed {
			resultsCh <- cursorResult{err: err}
		}

		close(resultsCh)
	}()

	return resultsCh, nil
}
