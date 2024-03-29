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
	withValue  bool
}

type cursorResult struct {
	err      error
	childLoc *ChildLoc
}

// --------------------------------------------

func (c *cursorImpl) Close() error {
	close(c.closeCh)
	return nil
}

func (c *cursorImpl) Next() (
	PartitionId, Key, Seq, Val, error) {
	itemBufRef, err := c.NextItemBufRef()
	if err != nil || itemBufRef == nil {
		return 0, nil, 0, nil, err
	}

	partitionId := itemBufRef.PartitionId(c.bufManager)

	key := FromItemBufRef(nil, true, itemBufRef, c.bufManager)

	seq := itemBufRef.Seq(c.bufManager)

	val := FromItemBufRef(nil, false, itemBufRef, c.bufManager)

	itemBufRef.DecRef(c.bufManager)

	return partitionId, key, seq, val, nil
}

func (c *cursorImpl) NextItemBufRef() (ItemBufRef, error) {
	r, ok := <-c.resultsCh
	if !ok || r.err != nil {
		return nil, r.err // TODO: zero/nil PartitionId.
	}

	loc, err := r.childLoc.Loc.Read(c.bufManager, c.readerAt)
	if err != nil {
		return nil, err
	}

	var itemBufRef ItemBufRef
	if c.withValue {
		itemBufRef = loc.ItemBufRef(c.bufManager)
	}

	return itemBufRef, nil
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

	rootILR, rootIL := r.rootAddRef()

	var visit func(il *ChildLoc) error
	visit = func(il *ChildLoc) error {
		if il == nil {
			return nil
		}
		if il.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&il.Loc,
				r.store.bufManager, readerAt)
			if err != nil {
				return err
			}
			if node == nil {
				return nil
			}
			ils := node.GetChildLocs()
			if ils == nil {
				return nil
			}
			n := ils.Len()
			if n <= 0 {
				return nil
			}
			i := sort.Search(n, func(i int) bool {
				return r.compareFunc(ils.Key(i), key) >= 0
			})
			if !ascending &&
				(i >= n || r.compareFunc(ils.Key(i), key) > 0) {
				i = i - 1
			}
			for i >= 0 && i < n {
				err := visit(ils.ChildLoc(i))
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

		if il.Loc.Type == LocTypeVal {
			select {
			case <-closeCh:
				return ErrCursorClosed
			case resultsCh <- cursorResult{err: nil, childLoc: il}:
				// TODO: Mem mgmt.
			}
			return nil
		}

		return fmt.Errorf("startCursor.visit:",
			" unexpected Loc.Type, il: %#v", il)
	}

	go func() {
		err := visit(rootIL)

		r.rootDecRef(rootILR)

		if err != nil && err != ErrCursorClosed {
			resultsCh <- cursorResult{err: err}
		}

		close(resultsCh)
	}()

	return resultsCh, nil
}
