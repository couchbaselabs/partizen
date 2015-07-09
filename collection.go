package partizen

import (
	"fmt"
	"io"
)

func (c *collection) Close() error {
	c.store.m.Lock()
	c.decRefUnlocked()
	c.store.m.Unlock()

	return nil
}

// --------------------------------------------

// Must be invoked while caller has store.m locked.
func (c *collection) addRefUnlocked() *collection {
	c.refs++

	return c
}

// Must be invoked while caller has store.m locked.
func (c *collection) decRefUnlocked() {
	c.refs--
	if c.refs <= 0 {
		c.root.decRef(c.store.bufManager)
		c.root = nil
		c.readOnly = true
	}
}

// --------------------------------------------

func (c *collection) rootAddRef() (*ChildLocRef, *ChildLoc) {
	c.store.m.Lock()
	ilr, il := c.root.addRef()
	c.store.m.Unlock()

	return ilr, il
}

func (c *collection) rootDecRef(ilr *ChildLocRef) {
	c.store.m.Lock()
	ilr.decRef(c.store.bufManager)
	c.store.m.Unlock()
}

// --------------------------------------------

func (c *collection) Get(key Key, matchSeq Seq, withValue bool) (
	seq Seq, val Val, err error) {
	itemBufRef, err := c.GetItemBufRef(key, matchSeq, withValue)
	if err != nil || itemBufRef == nil {
		return seq, nil, err
	}

	seq = itemBufRef.Seq(c.store.bufManager)

	if withValue {
		val = FromItemBufRef(nil, false, itemBufRef, c.store.bufManager)
	}

	itemBufRef.DecRef(c.store.bufManager)

	return seq, val, nil
}

func (c *collection) GetItemBufRef(key Key, matchSeq Seq, withValue bool) (
	ItemBufRef, error) {
	bufManager := c.store.bufManager

	var hitSeq Seq
	var hitType uint8
	var hitItemBufRef ItemBufRef

	ilr, il := c.rootAddRef()

	hit, err := locateChildLoc(il, key, c.compareFunc,
		bufManager, io.ReaderAt(nil))
	if err == nil && hit != nil {
		hitSeq, hitType = hit.Seq, hit.Loc.Type
		if withValue {
			hitItemBufRef = hit.Loc.ItemBufRef(bufManager)
		}
	}

	c.rootDecRef(ilr)

	if err != nil {
		if hitItemBufRef != nil {
			hitItemBufRef.DecRef(bufManager)
		}
		return nil, err
	}

	if matchSeq != NO_MATCH_SEQ {
		if hit != nil && matchSeq != hitSeq {
			if hitItemBufRef != nil {
				hitItemBufRef.DecRef(bufManager)
			}
			return nil, ErrMatchSeq
		}
		if hit == nil && matchSeq != CREATE_MATCH_SEQ {
			if hitItemBufRef != nil {
				hitItemBufRef.DecRef(bufManager)
			}
			return nil, ErrMatchSeq
		}
	}

	if hit != nil && hitType != LocTypeVal {
		if hitItemBufRef != nil {
			hitItemBufRef.DecRef(bufManager)
		}
		return nil, fmt.Errorf("collection.Get: bad type: %#v", hitType)
	}

	return hitItemBufRef, nil
}

func (c *collection) Set(partitionId PartitionId, key Key,
	matchSeq Seq, newSeq Seq, val Val) error {
	itemBufRef, err :=
		NewItemBufRef(c.store.bufManager, partitionId, key, newSeq, val)
	if err != nil {
		return err
	}

	err = c.SetItemBufRef(matchSeq, itemBufRef)

	itemBufRef.DecRef(c.store.bufManager)

	return err
}

func (c *collection) SetItemBufRef(matchSeq Seq,
	itemBufRef ItemBufRef) error {
	if itemBufRef == nil || itemBufRef.IsNil() {
		return ErrAlloc
	}

	return c.mutate([]Mutation{Mutation{
		ItemBufRef: itemBufRef,
		Op:         MUTATION_OP_UPDATE,
		MatchSeq:   matchSeq,
	}}, c.store.bufManager)
}

func (c *collection) Del(key Key, matchSeq Seq, newSeq Seq) error {
	bufManager := c.store.bufManager

	itemBufRef := bufManager.AllocItem(len(key), 0)
	if itemBufRef == nil || itemBufRef.IsNil() {
		return ErrAlloc
	}

	ItemBufRefAccess(itemBufRef, true, true, bufManager,
		0, len(key), CopyToBufRef, key)

	itemBufRef.SetSeq(bufManager, newSeq)

	err := c.mutate([]Mutation{Mutation{
		ItemBufRef: itemBufRef,
		Op:         MUTATION_OP_DELETE,
		MatchSeq:   matchSeq,
	}}, c.store.bufManager)

	itemBufRef.DecRef(bufManager)

	return err
}

func (c *collection) Batch(mutations []Mutation) error {
	return c.mutate(mutations, c.store.bufManager)
}

func (c *collection) Min(withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	return c.minMax(false, withValue)
}

func (c *collection) Max(withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	return c.minMax(true, withValue)
}

func (c *collection) MaxSeq(partitionId PartitionId) (
	seq Seq, err error) {
	return 0, fmt.Errorf("unimplemented")
}

func (c *collection) PartitionIds(outPartitionIds PartitionIds) (
	PartitionIds, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (c *collection) Scan(key Key, ascending bool,
	partitionIds []PartitionId, // Use nil for all partitions.
	withValue bool, maxReadAhead int) (Cursor, error) {
	closeCh := make(chan struct{})

	readerAt := io.ReaderAt(nil)

	resultsCh, err := c.startCursor(key, ascending,
		partitionIds, readerAt, closeCh, maxReadAhead)
	if err != nil {
		close(closeCh)
		return nil, err
	}

	return &cursorImpl{
		bufManager: c.store.bufManager,
		readerAt:   readerAt,
		closeCh:    closeCh,
		resultsCh:  resultsCh,
		withValue:  withValue,
	}, nil
}

func (c *collection) Snapshot() (Collection, error) {
	c.store.m.Lock()
	x := *c // Shallow copy.
	x.root.addRef()
	x.refs = 1
	x.readOnly = true
	c.store.m.Unlock()
	return &x, nil
}

func (c *collection) Diff(partitionId PartitionId, seq Seq,
	exactToSeq bool) (Cursor, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (c *collection) Rollback(partitionId PartitionId, seq Seq,
	exactToSeq bool) (Seq, error) {
	return 0, fmt.Errorf("unimplemented")
}

// --------------------------------------------

func (c *collection) mutate(
	mutations []Mutation, bufManager BufManager) error {
	if c.readOnly {
		return ErrReadOnly
	}

	var cbErr error

	cb := func(existing *ChildLoc, isVal bool, mutation *Mutation) bool {
		if !isVal ||
			mutation.MatchSeq == NO_MATCH_SEQ ||
			(existing == nil && mutation.MatchSeq == CREATE_MATCH_SEQ) ||
			(existing != nil && mutation.MatchSeq == existing.Seq) {
			return true
		}

		// TODO: Should optionally keep going vs all-or-none semantics.
		// TODO: Should track which mutations had error.
		cbErr = ErrMatchSeq
		return false
	}

	reclaimables := &PtrChildLocsArrayHolder{} // TODO: sync.Pool'able.

	ilr, il := c.rootAddRef()

	il2, err := rootProcessMutations(il, mutations, cb,
		int(c.minFanOut), int(c.maxFanOut), reclaimables,
		bufManager, io.ReaderAt(nil))
	if err != nil {
		c.rootDecRef(ilr)
		return err
	}
	if cbErr != nil {
		c.rootDecRef(ilr)
		return cbErr
	}

	c.store.m.Lock()
	if ilr != c.root {
		err = ErrConcurrentMutation
	} else if ilr != nil && ilr.next != nil {
		err = ErrConcurrentMutationChain
	} else {
		err = nil

		c.root = &ChildLocRef{il: il2, refs: 1}

		// If the previous root was in-use, hook it up with a
		// ref-count on the new root to prevent the new root's nodes
		// from being reclaimed until the previous is done.
		if ilr != nil && ilr.refs > 2 {
			ilr.next, _ = c.root.addRef()
		}
	}
	c.store.m.Unlock()

	if err == nil {
		c.rootDecRef(ilr) // Because c.root was replaced.
	}

	c.rootDecRef(ilr) // Matches above c.rootAddRef().
	return err
}

// --------------------------------------------

func (c *collection) minMax(locateMax bool, withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	itemBufRef, err := c.minMaxItemBufRef(locateMax)
	if err != nil || itemBufRef == nil {
		return 0, nil, 0, nil, err
	}

	partitionId = itemBufRef.PartitionId(c.store.bufManager)

	key = FromItemBufRef(nil, true, itemBufRef, c.store.bufManager)

	seq = itemBufRef.Seq(c.store.bufManager)

	if withValue {
		val = FromItemBufRef(nil, false, itemBufRef, c.store.bufManager)
	}

	itemBufRef.DecRef(c.store.bufManager)

	return partitionId, key, seq, val, nil
}

func (c *collection) minMaxItemBufRef(wantMax bool) (
	ItemBufRef, error) {
	bufManager := c.store.bufManager

	ilr, il := c.rootAddRef()
	if ilr == nil || il == nil {
		c.rootDecRef(ilr)
		return nil, nil
	}

	ilMM, err := locateMinMax(il, wantMax, bufManager, io.ReaderAt(nil))
	if err != nil {
		c.rootDecRef(ilr)
		return nil, err
	}
	if ilMM == nil {
		c.rootDecRef(ilr)
		return nil, err
	}
	if ilMM.Loc.Type != LocTypeVal {
		c.rootDecRef(ilr)
		return nil, fmt.Errorf("collection.minMaxItemBufRef:"+
			" unexpected type, ilMM: %#v", ilMM)
	}

	itemBufRef := ilMM.Loc.ItemBufRef(bufManager)

	c.rootDecRef(ilr)
	return itemBufRef, nil
}
