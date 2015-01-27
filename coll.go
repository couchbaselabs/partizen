package partizen

import (
	"fmt"
	"io"
)

func (r *CollRoot) Close() error {
	r.store.m.Lock()
	r.RootKeySeqLocRef.DecRef()
	r.RootKeySeqLocRef = nil
	r.store.m.Unlock()
	return nil
}

func (r *CollRoot) Get(partitionId PartitionId, key Key, matchSeq Seq,
	withValue bool) (seq Seq, val Val, err error) {
	if partitionId != 0 {
		return 0, nil, fmt.Errorf("partition unimplemented")
	}

	r.store.m.Lock()
	kslr, ksl := r.RootKeySeqLocRef.AddRef()
	r.store.m.Unlock()
	if kslr == nil || ksl == nil {
		return 0, nil, nil
	}

	ksl, err = locateKeySeqLoc(ksl, key, io.ReaderAt(nil))
	if err != nil {
		return 0, nil, err
	}
	if matchSeq != NO_MATCH_SEQ && (ksl == nil || ksl.Seq != matchSeq) {
		return 0, nil, ErrMatchSeq
	}
	if ksl == nil {
		return 0, nil, err
	}
	if ksl.Loc.Type != LocTypeVal {
		return 0, nil,
			fmt.Errorf("CollRoot.Get: unexpected type, ksl: %#v", ksl)
	}

	seq, val = ksl.Seq, ksl.Loc.buf // TODO: Mem mgmt, need to copy buf?

	r.store.m.Lock()
	kslr.DecRef()
	r.store.m.Unlock()

	return seq, val, nil
}

func (r *CollRoot) Set(partitionId PartitionId, key Key, matchSeq Seq,
	newSeq Seq, val Val) (err error) {
	return r.mutate([]Mutation{Mutation{
		Key:      key,
		Seq:      newSeq,
		Val:      val,
		Op:       MUTATION_OP_UPDATE,
		MatchSeq: matchSeq,
	}})
}

func (r *CollRoot) Del(partitionId PartitionId, key Key, matchSeq Seq,
	newSeq Seq) error {
	return r.mutate([]Mutation{Mutation{
		Key:      key,
		Seq:      newSeq,
		Op:       MUTATION_OP_DELETE,
		MatchSeq: matchSeq,
	}})
}

func (r *CollRoot) Batch(mutations []Mutation) error {
	return r.mutate(mutations)
}

func (r *CollRoot) Min(withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	return r.minMax(false, withValue)
}

func (r *CollRoot) Max(withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	return r.minMax(true, withValue)
}

func (r *CollRoot) Scan(key Key,
	ascending bool,
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
		closeCh:   closeCh,
		resultsCh: resultsCh,
	}, nil
}

func (r *CollRoot) Snapshot() (Collection, error) {
	r.store.m.Lock()
	r.RootKeySeqLocRef.AddRef()
	x := *r // Shallow copy.
	r.store.m.Unlock()

	x.readOnly = true
	return &x, nil
}

func (r *CollRoot) Diff(partitionId PartitionId, seq Seq,
	exactToSeq bool) (
	Cursor, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (r *CollRoot) Rollback(partitionId PartitionId, seq Seq,
	exactToSeq bool) error {
	return fmt.Errorf("unimplemented")
}

// --------------------------------------------

func (r *CollRoot) mutate(mutations []Mutation) (err error) {
	if r.readOnly {
		return ErrReadOnly
	}

	var cbErr error
	cb := func(existing *KeySeqLoc, isVal bool, mutation *Mutation) bool {
		if !isVal ||
			mutation.MatchSeq == NO_MATCH_SEQ ||
			(existing == nil && mutation.MatchSeq == CREATE_MATCH_SEQ) ||
			(existing != nil && mutation.MatchSeq == existing.Seq) {
			return true
		}
		cbErr = ErrMatchSeq
		return false
	}

	r.store.m.Lock()
	r.store.startChanges()
	kslr, ksl := r.RootKeySeqLocRef.AddRef()
	r.store.m.Unlock()

	ksl2, err := rootProcessMutations(ksl, mutations, cb,
		int(r.minFanOut), int(r.maxFanOut), io.ReaderAt(nil))
	if err != nil {
		return err
	}
	if cbErr != nil {
		return cbErr
	}

	r.store.m.Lock()
	if r.RootKeySeqLocRef == kslr {
		next := &KeySeqLocRef{R: ksl2, refs: 2}
		if kslr != nil {
			kslr.next = next
		}
		r.RootKeySeqLocRef = next
		if kslr != nil {
			kslr.DecRef()
		}
	} else {
		err = fmt.Errorf("CollRoot.mutate: concurrent modification")
	}
	r.store.m.Unlock()

	return err
}

func (r *CollRoot) minMax(locateMax bool, withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	r.store.m.Lock()
	kslr, ksl := r.RootKeySeqLocRef.AddRef()
	r.store.m.Unlock()
	if kslr == nil || ksl == nil {
		return 0, nil, 0, nil, nil
	}

	ksl, err = locateMinMax(ksl, locateMax, io.ReaderAt(nil))
	if err != nil {
		return 0, nil, 0, nil, err
	}
	if ksl == nil {
		return 0, nil, 0, nil, err
	}
	if ksl.Loc.Type != LocTypeVal {
		return 0, nil, 0, nil,
			fmt.Errorf("CollRoot.minMax: unexpected type, ksl: %#v", ksl)
	}

	key, seq, val = ksl.Key, ksl.Seq, ksl.Loc.buf // TOOD: Mem mgmt.

	r.store.m.Lock()
	kslr.DecRef()
	r.store.m.Unlock()

	return 0, key, seq, val, nil
}

// ----------------------------------------

type CursorImpl struct {
	closeCh   chan struct{}
	resultsCh chan CursorResult
}

func (c *CursorImpl) Close() error {
	close(c.closeCh)
	return nil
}

func (c *CursorImpl) Next() (PartitionId, Key, Seq, Val, error) {
	r, ok := <-c.resultsCh
	if !ok {
		return 0, nil, 0, nil, nil // TODO: PartitionId.
	}
	return 0, r.ksl.Key, r.ksl.Seq, r.ksl.Loc.buf, r.err // TODO: Mem mgmt.
}
