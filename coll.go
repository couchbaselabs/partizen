package partizen

import (
	"fmt"
	"io"
)

func (r *CollRoot) Get(partitionId PartitionId, key Key, withValue bool) (
	seq Seq, val Val, err error) {
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
	if err != nil || ksl == nil {
		return 0, nil, err
	}
	if ksl.Loc.Type != LocTypeVal {
		return 0, nil, fmt.Errorf("unexpected type, ksl: %#v", ksl)
	}

	seq, val = ksl.Seq, ksl.Loc.buf

	r.store.m.Lock()
	kslr.DecRef()
	r.store.m.Unlock()

	return seq, val, nil
}

func (r *CollRoot) Set(partitionId PartitionId, key Key, seq Seq, val Val) (
	err error) {
	return r.mutate(MUTATION_OP_UPDATE, partitionId, key, seq, val)
}

func (r *CollRoot) Merge(partitionId PartitionId, key Key, seq Seq,
	mergeFunc MergeFunc) error {
	return fmt.Errorf("unimplemented")
}

func (r *CollRoot) Del(partitionId PartitionId, key Key, seq Seq) error {
	return r.mutate(MUTATION_OP_DELETE, partitionId, key, seq, nil)
}

func (r *CollRoot) Min(withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	return 0, nil, 0, nil, fmt.Errorf("unimplemented")
}

func (r *CollRoot) Max(withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	return 0, nil, 0, nil, fmt.Errorf("unimplemented")
}

func (r *CollRoot) Scan(key Key,
	reverse bool,
	partitionIds []PartitionId, // Use nil for all partitions.
	withValue bool) (Cursor, error) {
	return nil, fmt.Errorf("unimplemented")
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

func (r *CollRoot) mutate(op MutationOp, partitionId PartitionId,
	key Key, seq Seq, val Val) (err error) {
	if partitionId != 0 {
		return fmt.Errorf("partition unimplemented")
	}

	r.store.m.Lock()
	r.store.startChanges()
	kslr, ksl := r.RootKeySeqLocRef.AddRef()
	r.store.m.Unlock()

	ksl2, err := rootKeySeqLocProcessMutations(ksl, []Mutation{
		Mutation{Key: key, Seq: seq, Val: val, Op: op},
	}, int(r.minFanOut), int(r.maxFanOut), io.ReaderAt(nil))
	if err != nil {
		return err
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
		err = fmt.Errorf("concurrent modification")
	}
	r.store.m.Unlock()

	return err
}
