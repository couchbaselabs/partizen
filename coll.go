package partizen

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
)

func (r *CollRoot) Close() error {
	r.store.m.Lock()
	r.decRefUnlocked()
	r.store.m.Unlock()
	return nil
}

// Must be invoked while caller has store.m locked.
func (r *CollRoot) addRefUnlocked() *CollRoot {
	r.refs++
	return r
}

// Must be invoked while caller has store.m locked.
func (r *CollRoot) decRefUnlocked() {
	r.refs--
	if r.refs <= 0 {
		r.RootKeySeqLocRef.decRef()
		r.RootKeySeqLocRef = nil
		r.readOnly = true
	}
}

func (r *CollRoot) rootAddRef() (*KeySeqLocRef, *KeySeqLoc) {
	r.store.m.Lock()
	kslr, ksl := r.RootKeySeqLocRef.addRef()
	r.store.m.Unlock()
	return kslr, ksl
}

func (r *CollRoot) rootDecRef(kslr *KeySeqLocRef) {
	r.store.m.Lock()
	kslr.decRef()
	r.store.m.Unlock()
}

func (r *CollRoot) Get(partitionId PartitionId, key Key, matchSeq Seq,
	withValue bool) (seq Seq, val Val, err error) {
	if partitionId != 0 {
		return 0, nil, fmt.Errorf("partition unimplemented")
	}

	kslr, ksl := r.rootAddRef()
	defer r.rootDecRef(kslr)

	if kslr == nil || ksl == nil {
		return 0, nil, nil
	}

	ksl, err = locateKeySeqLoc(ksl, key, io.ReaderAt(nil))
	if err != nil {
		return 0, nil, err
	}
	if matchSeq != NO_MATCH_SEQ {
		if ksl != nil && matchSeq != ksl.Seq {
			return 0, nil, ErrMatchSeq
		}
		if ksl == nil && matchSeq != CREATE_MATCH_SEQ {
			return 0, nil, ErrMatchSeq
		}
	}
	if ksl == nil {
		return 0, nil, err
	}
	if ksl.Loc.Type != LocTypeVal {
		return 0, nil,
			fmt.Errorf("CollRoot.Get: unexpected type, ksl: %#v", ksl)
	}

	return ksl.Seq, ksl.Loc.buf, nil // TODO: Mem mgmt, need to copy buf?
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
	x := *r // Shallow copy.
	x.RootKeySeqLocRef.addRef()
	x.refs = 1
	x.readOnly = true
	r.store.m.Unlock()
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

	kslr, ksl := r.rootAddRef()
	defer r.rootDecRef(kslr)

	ksl2, err := rootProcessMutations(ksl, mutations, cb,
		int(r.minFanOut), int(r.maxFanOut), io.ReaderAt(nil))
	if err != nil {
		return err
	}
	if cbErr != nil {
		return cbErr
	}

	r.store.m.Lock()
	if kslr != r.RootKeySeqLocRef {
		err = ErrConcurrentModification
	} else if kslr != nil && kslr.next != nil {
		err = fmt.Errorf("CollRoot.mutate: concurrent modification next chain")
	} else {
		r.RootKeySeqLocRef = &KeySeqLocRef{R: ksl2, refs: 1}
		if kslr != nil {
			kslr.next, _ = r.RootKeySeqLocRef.addRef()
		}
	}
	r.store.m.Unlock()

	return err
}

func (r *CollRoot) minMax(locateMax bool, withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	kslr, ksl := r.rootAddRef()
	defer r.rootDecRef(kslr)

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

	return 0, ksl.Key, ksl.Seq, ksl.Loc.buf, nil // TOOD: Mem mgmt.
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

// --------------------------------------------

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

	kslr, ksl := r.rootAddRef()

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
			if !ascending &&
				(i >= n || bytes.Compare(ksls.Key(i), key) > 0) {
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

		r.rootDecRef(kslr)

		if err != nil && err != ErrCursorClosed {
			resultsCh <- CursorResult{err: err}
		}
		close(resultsCh)
	}()

	return resultsCh, nil
}
