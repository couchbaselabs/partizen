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

	r.m.Lock()
	rksl := r.RootKeySeqLoc // TODO: Ref count.
	r.m.Unlock()

	ksl, err := locateKeySeqLoc(rksl, key, io.ReaderAt(nil))
	if err != nil || ksl == nil {
		return 0, nil, err
	}
	if ksl.Loc.Type != LocTypeVal {
		return 0, nil, fmt.Errorf("unexpected type, ksl: %#v", ksl)
	}
	return ksl.Seq, ksl.Loc.buf, nil
}

func (r *CollRoot) Set(partitionId PartitionId, key Key, seq Seq, val Val) (
	err error) {
	if partitionId != 0 {
		return fmt.Errorf("partition unimplemented")
	}

	r.store.startChanges()

	r.m.Lock()
	rksl := r.RootKeySeqLoc // TODO: Ref count.
	r.m.Unlock()

	rksl2, err := rootKeySeqLocProcessMutations(rksl, []Mutation{
		Mutation{
			Key: key,
			Seq: seq,
			Val: val,
			Op:  MUTATION_OP_UPDATE,
		},
	}, int(r.minFanOut), int(r.maxFanOut), io.ReaderAt(nil))
	if err != nil {
		return err
	}

	r.m.Lock()
	if r.RootKeySeqLoc == rksl {
		r.RootKeySeqLoc = rksl2
	} else {
		err = fmt.Errorf("concurrent modification")
	}
	r.m.Unlock()

	return err
}

func (r *CollRoot) Merge(partitionId PartitionId, key Key, seq Seq,
	mergeFunc MergeFunc) error {
	return fmt.Errorf("unimplemented")
}

func (r *CollRoot) Del(partitionId PartitionId, key Key, seq Seq) error {
	return fmt.Errorf("unimplemented")
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
