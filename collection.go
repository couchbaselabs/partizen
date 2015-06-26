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

func (c *collection) rootAddRef() (*ItemLocRef, *ItemLoc) {
	c.store.m.Lock()
	ilr, il := c.root.addRef()
	c.store.m.Unlock()

	return ilr, il
}

func (c *collection) rootDecRef(ilr *ItemLocRef) {
	c.store.m.Lock()
	ilr.decRef(c.store.bufManager)
	c.store.m.Unlock()
}

// --------------------------------------------

func (c *collection) Get(key Key, matchSeq Seq, withValue bool) (
	seq Seq, val Val, err error) {
	seq, bufRef, err := c.GetBufRef(key, matchSeq, withValue)
	if err != nil || bufRef == nil {
		return seq, nil, err
	}

	if withValue {
		val = FromBufRef(nil, bufRef, c.store.bufManager)
	}

	bufRef.DecRef(c.store.bufManager)

	return seq, val, nil
}

func (c *collection) GetBufRef(key Key, matchSeq Seq, withValue bool) (
	seq Seq, val BufRef, err error) {
	bufManager := c.store.bufManager

	var hitSeq Seq
	var hitType uint8
	var hitBufRef BufRef

	ilr, il := c.rootAddRef()

	hit, err := locateItemLoc(il, key, c.compareFunc,
		bufManager, io.ReaderAt(nil))
	if err == nil && hit != nil {
		hitSeq, hitType = hit.Seq, hit.Loc.Type
		if withValue {
			hitBufRef = hit.Loc.BufRef(bufManager)
		}
	}

	c.rootDecRef(ilr)

	if err != nil {
		if hitBufRef != nil {
			hitBufRef.DecRef(bufManager)
		}
		return 0, nil, err
	}

	if matchSeq != NO_MATCH_SEQ {
		if hit != nil && matchSeq != hitSeq {
			if hitBufRef != nil {
				hitBufRef.DecRef(bufManager)
			}
			return 0, nil, ErrMatchSeq
		}
		if hit == nil && matchSeq != CREATE_MATCH_SEQ {
			if hitBufRef != nil {
				hitBufRef.DecRef(bufManager)
			}
			return 0, nil, ErrMatchSeq
		}
	}

	if hit != nil && hitType != LocTypeVal {
		if hitBufRef != nil {
			hitBufRef.DecRef(bufManager)
		}
		return 0, nil, fmt.Errorf("collection.Get: bad type: %#v", hitType)
	}

	return hitSeq, hitBufRef, nil
}

func (c *collection) Set(partitionId PartitionId, key Key,
	matchSeq Seq, newSeq Seq, val Val) error {
	bufManager := c.store.bufManager

	valBufRef := bufManager.Alloc(len(val), CopyToBufRef, val)
	if valBufRef == nil || valBufRef.IsNil() {
		return ErrAlloc
	}

	err := c.SetBufRef(partitionId, key, matchSeq, newSeq, valBufRef)

	valBufRef.DecRef(bufManager)

	return err
}

func (c *collection) SetBufRef(partitionId PartitionId, key Key,
	matchSeq Seq, newSeq Seq, valBufRef BufRef) error {
	if valBufRef == nil || valBufRef.IsNil() {
		return ErrAlloc
	}

	return c.mutate([]Mutation{Mutation{
		PartitionId: partitionId,
		Key:         key,
		Seq:         newSeq,
		ValBufRef:   valBufRef,
		Op:          MUTATION_OP_UPDATE,
		MatchSeq:    matchSeq,
	}}, c.store.bufManager)
}

func (c *collection) Del(key Key, matchSeq Seq, newSeq Seq) error {
	return c.mutate([]Mutation{Mutation{
		Key:      key,
		Seq:      newSeq,
		Op:       MUTATION_OP_DELETE,
		MatchSeq: matchSeq,
	}}, c.store.bufManager)
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

	cb := func(existing *ItemLoc, isVal bool, mutation *Mutation) bool {
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

	reclaimables := &PtrItemLocsArrayHolder{} // TODO: sync.Pool'able.

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

		c.root = &ItemLocRef{R: il2, refs: 1}

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
	var bufRef BufRef

	partitionId, key, seq, bufRef, err =
		c.minMaxBufRef(locateMax, withValue)
	if err != nil || bufRef == nil {
		return 0, nil, 0, nil, err
	}

	if withValue {
		val = FromBufRef(nil, bufRef, c.store.bufManager)
	}

	bufRef.DecRef(c.store.bufManager)

	return partitionId, key, seq, val, nil
}

func (c *collection) minMaxBufRef(wantMax bool, withValue bool) (
	partitionId PartitionId, key Key, seq Seq, bufRef BufRef, err error) {
	bufManager := c.store.bufManager

	ilr, il := c.rootAddRef()
	if ilr == nil || il == nil {
		c.rootDecRef(ilr)
		return 0, nil, 0, nil, nil
	}

	il, err = locateMinMax(il, wantMax, bufManager, io.ReaderAt(nil))
	if err != nil {
		c.rootDecRef(ilr)
		return 0, nil, 0, nil, err
	}
	if il == nil {
		c.rootDecRef(ilr)
		return 0, nil, 0, nil, err
	}
	if il.Loc.Type != LocTypeVal {
		c.rootDecRef(ilr)
		return 0, nil, 0, nil,
			fmt.Errorf("collection.minMax: unexpected type, il: %#v", il)
	}

	if withValue {
		bufRef = il.Loc.BufRef(bufManager)
	}

	c.rootDecRef(ilr)
	return 0, il.Key, il.Seq, bufRef, nil
}
