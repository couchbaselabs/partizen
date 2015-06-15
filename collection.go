package partizen

import (
	"fmt"
	"io"
)

func (r *collection) Close() error {
	r.store.m.Lock()
	r.decRefUnlocked()
	r.store.m.Unlock()
	return nil
}

// Must be invoked while caller has store.m locked.
func (r *collection) addRefUnlocked() *collection {
	r.refs++
	return r
}

// Must be invoked while caller has store.m locked.
func (r *collection) decRefUnlocked() {
	r.refs--
	if r.refs <= 0 {
		r.Root.decRef()
		r.Root = nil
		r.readOnly = true
	}
}

func (r *collection) rootAddRef() (*ItemLocRef, *ItemLoc) {
	r.store.m.Lock()
	kslr, ksl := r.Root.addRef()
	r.store.m.Unlock()
	return kslr, ksl
}

func (r *collection) rootDecRef(kslr *ItemLocRef) {
	r.store.m.Lock()
	kslr.decRef()
	r.store.m.Unlock()
}

// --------------------------------------------

func (r *collection) Get(key Key, matchSeq Seq, withValue bool) (
	seq Seq, val Val, err error) {
	seq, bufRef, err := r.GetBufRef(key, matchSeq, withValue)
	if err != nil || bufRef == nil {
		return seq, nil, err
	}

	if withValue {
		val = FromBufRef(nil, bufRef, r.store.bufManager)
	}

	bufRef.DecRef(r.store.bufManager)

	return seq, val, nil
}

func (r *collection) GetBufRef(key Key, matchSeq Seq, withValue bool) (
	seq Seq, val BufRef, err error) {
	var hitSeq Seq
	var hitType uint8
	var hitBufRef BufRef

	kslr, ksl := r.rootAddRef()
	hit, err := locateItemLoc(ksl, key,
		r.store.bufManager, io.ReaderAt(nil))
	if err == nil && hit != nil {
		hitSeq, hitType = hit.Seq, hit.Loc.Type
		if withValue {
			hitBufRef = hit.Loc.BufRef(r.store.bufManager)
		}
	}
	r.rootDecRef(kslr)

	if err != nil {
		if hitBufRef != nil {
			hitBufRef.DecRef(r.store.bufManager)
		}
		return 0, nil, err
	}

	if matchSeq != NO_MATCH_SEQ {
		if hit != nil && matchSeq != hitSeq {
			if hitBufRef != nil {
				hitBufRef.DecRef(r.store.bufManager)
			}
			return 0, nil, ErrMatchSeq
		}
		if hit == nil && matchSeq != CREATE_MATCH_SEQ {
			if hitBufRef != nil {
				hitBufRef.DecRef(r.store.bufManager)
			}
			return 0, nil, ErrMatchSeq
		}
	}

	if hit != nil && hitType != LocTypeVal {
		if hitBufRef != nil {
			hitBufRef.DecRef(r.store.bufManager)
		}
		return 0, nil, fmt.Errorf("collection.Get: bad type: %#v", hitType)
	}

	return hitSeq, hitBufRef, nil
}

func (r *collection) Set(partitionId PartitionId, key Key,
	matchSeq Seq, newSeq Seq, val Val) error {
	valBufRef := r.store.bufManager.Alloc(len(val), CopyToBufRef, val)
	if valBufRef == nil || valBufRef.IsNil() {
		return ErrAlloc
	}

	err := r.SetBufRef(partitionId, key, matchSeq, newSeq, valBufRef)

	valBufRef.DecRef(r.store.bufManager)

	return err
}

func (r *collection) SetBufRef(partitionId PartitionId, key Key,
	matchSeq Seq, newSeq Seq, valBufRef BufRef) error {
	if valBufRef == nil || valBufRef.IsNil() {
		return ErrAlloc
	}

	return r.mutate([]Mutation{Mutation{
		PartitionId: partitionId,
		Key:         key,
		Seq:         newSeq,
		ValBufRef:   valBufRef,
		Op:          MUTATION_OP_UPDATE,
		MatchSeq:    matchSeq,
	}}, r.store.bufManager)
}

func (r *collection) Del(key Key, matchSeq Seq, newSeq Seq) error {
	return r.mutate([]Mutation{Mutation{
		Key:      key,
		Seq:      newSeq,
		Op:       MUTATION_OP_DELETE,
		MatchSeq: matchSeq,
	}}, r.store.bufManager)
}

func (r *collection) Batch(mutations []Mutation) error {
	return r.mutate(mutations, r.store.bufManager)
}

func (r *collection) Min(withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	return r.minMax(false, withValue)
}

func (r *collection) Max(withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	return r.minMax(true, withValue)
}

func (r *collection) Scan(key Key, ascending bool,
	partitionIds []PartitionId, // Use nil for all partitions.
	withValue bool, maxReadAhead int) (Cursor, error) {
	closeCh := make(chan struct{})

	readerAt := io.ReaderAt(nil)

	resultsCh, err := r.startCursor(key, ascending,
		partitionIds, readerAt, closeCh, maxReadAhead)
	if err != nil {
		close(closeCh)
		return nil, err
	}

	return &CursorImpl{
		bufManager: r.store.bufManager,
		readerAt:   readerAt,
		closeCh:    closeCh,
		resultsCh:  resultsCh,
	}, nil
}

func (r *collection) Snapshot() (Collection, error) {
	r.store.m.Lock()
	x := *r // Shallow copy.
	x.Root.addRef()
	x.refs = 1
	x.readOnly = true
	r.store.m.Unlock()
	return &x, nil
}

func (r *collection) Diff(partitionId PartitionId, seq Seq,
	exactToSeq bool) (Cursor, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (r *collection) Rollback(partitionId PartitionId, seq Seq,
	exactToSeq bool) (Seq, error) {
	return 0, fmt.Errorf("unimplemented")
}

// --------------------------------------------

func (r *collection) mutate(
	mutations []Mutation, bufManager BufManager) error {
	if r.readOnly {
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

	kslr, ksl := r.rootAddRef()

	ksl2, err := rootProcessMutations(ksl, mutations, cb,
		int(r.minFanOut), int(r.maxFanOut), bufManager, io.ReaderAt(nil))
	if err != nil {
		r.rootDecRef(kslr)
		return err
	}
	if cbErr != nil {
		r.rootDecRef(kslr)
		return cbErr
	}

	r.store.m.Lock()
	if kslr != r.Root {
		err = ErrConcurrentMutation
	} else if kslr != nil && kslr.next != nil {
		err = ErrConcurrentMutationChain
	} else {
		r.Root = &ItemLocRef{R: ksl2, refs: 1}
		if kslr != nil {
			kslr.next, _ = r.Root.addRef()
		}
	}
	r.store.m.Unlock()

	r.rootDecRef(kslr)
	return err
}

// --------------------------------------------

func (r *collection) minMax(locateMax bool, withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	var bufRef BufRef

	partitionId, key, seq, bufRef, err =
		r.minMaxBufRef(locateMax, withValue)
	if err != nil || bufRef == nil {
		return 0, nil, 0, nil, err
	}

	if withValue {
		val = FromBufRef(nil, bufRef, r.store.bufManager)
	}

	bufRef.DecRef(r.store.bufManager)

	return partitionId, key, seq, val, nil
}

func (r *collection) minMaxBufRef(locateMax bool, withValue bool) (
	partitionId PartitionId, key Key, seq Seq, bufRef BufRef, err error) {
	kslr, ksl := r.rootAddRef()
	if kslr == nil || ksl == nil {
		r.rootDecRef(kslr)
		return 0, nil, 0, nil, nil
	}

	ksl, err = locateMinMax(ksl, locateMax,
		r.store.bufManager, io.ReaderAt(nil))
	if err != nil {
		r.rootDecRef(kslr)
		return 0, nil, 0, nil, err
	}
	if ksl == nil {
		r.rootDecRef(kslr)
		return 0, nil, 0, nil, err
	}
	if ksl.Loc.Type != LocTypeVal {
		r.rootDecRef(kslr)
		return 0, nil, 0, nil,
			fmt.Errorf("collection.minMax: unexpected type, ksl: %#v", ksl)
	}

	if withValue {
		bufRef = ksl.Loc.BufRef(r.store.bufManager)
	}

	r.rootDecRef(kslr)
	return 0, ksl.Key, ksl.Seq, bufRef, nil
}
