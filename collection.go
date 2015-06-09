package partizen

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
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

func (r *collection) Get(partitionId PartitionId, key Key, matchSeq Seq,
	withValue bool) (seq Seq, val Val, err error) {
	seq, bufRef, err := r.GetBufRef(partitionId, key, matchSeq, withValue)
	if err != nil || bufRef == nil {
		return seq, nil, err
	}

	if withValue {
		val = BufRefBytes(nil, bufRef, r.store.bufManager)
	}

	bufRef.DecRef(r.store.bufManager)

	return seq, val, nil
}

func (r *collection) GetBufRef(partitionId PartitionId, key Key, matchSeq Seq,
	withValue bool) (seq Seq, val BufRef, err error) {
	if partitionId != 0 {
		return 0, nil, fmt.Errorf("partition unimplemented")
	}

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

	return hitSeq, hitBufRef, nil // TODO: What if partitionId doesn't match?
}

func (r *collection) Set(partitionId PartitionId, key Key, matchSeq Seq,
	newSeq Seq, val Val) (err error) {
	return r.mutate([]Mutation{Mutation{
		PartitionId: partitionId,
		Key:         key,
		Seq:         newSeq,
		Val:         val,
		Op:          MUTATION_OP_UPDATE,
		MatchSeq:    matchSeq,
	}}, r.store.bufManager)
}

func (r *collection) Del(partitionId PartitionId, key Key, matchSeq Seq,
	newSeq Seq) error {
	return r.mutate([]Mutation{Mutation{
		PartitionId: partitionId,
		Key:         key,
		Seq:         newSeq,
		Op:          MUTATION_OP_DELETE,
		MatchSeq:    matchSeq,
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
	withValue bool) (Cursor, error) {
	closeCh := make(chan struct{})

	resultsCh, err := r.startCursor(key, ascending, partitionIds,
		io.ReaderAt(nil), closeCh)
	if err != nil {
		close(closeCh)
		return nil, err
	}

	return &CursorImpl{
		bufManager: r.store.bufManager,
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

func (r *collection) minMax(locateMax bool, withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	var bufRef BufRef

	partitionId, key, seq, bufRef, err =
		r.minMaxBufRef(locateMax, withValue)
	if err != nil || bufRef == nil {
		return 0, nil, 0, nil, err
	}

	if withValue {
		val = BufRefBytes(nil, bufRef, r.store.bufManager)
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

// ----------------------------------------

type CursorImpl struct {
	bufManager BufManager
	closeCh    chan struct{}
	resultsCh  chan CursorResult
}

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

	val := BufRefBytes(nil, bufRef, c.bufManager)

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
		r.ksl.Loc.BufRef(c.bufManager), r.err // TOOD: Mem mgmt.
}

// --------------------------------------------

var ErrCursorClosed = errors.New("cursor closed sentinel")

type CursorResult struct {
	err error
	ksl *ItemLoc
}

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
