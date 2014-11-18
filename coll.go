package partizen

import (
	"fmt"
)

func (r *RootLoc) Get(partitionID PartitionID, key Key) (
	seq Seq, val Val, err error) {
	return r.GetEx(partitionID, key, true, false)
}

func (r *RootLoc) GetEx(partitionID PartitionID,
	key Key,
	withValue bool, // When withValue is false, value will be nil.
	fastSample bool) ( // Return result only if fast / in memory (no disk hit).
	seq Seq, val Val, err error) {
	r.store.m.Lock()
	defer r.store.m.Unlock()

	return r.node.Get(partitionID, key, withValue, fastSample)
}

func (r *RootLoc) Set(partitionID PartitionID, key Key, seq Seq, val Val) error {
	r.store.m.Lock()
	defer r.store.m.Unlock()

	r.store.changes.startChanges(r.store.footer)

	n, err := r.node.Set(partitionID, key, seq, val)
	if err != nil {
		return err
	}
	r.node = n
	return nil
}

func (r *RootLoc) Merge(partitionID PartitionID, key Key, seq Seq,
	mergeFunc MergeFunc) error {
	return fmt.Errorf("unimplemented")
}

func (r *RootLoc) Del(partitionID PartitionID, key Key, seq Seq) error {
	return fmt.Errorf("unimplemented")
}

func (r *RootLoc) Min(withValue bool) (
	partitionID PartitionID, key Key, seq Seq, val Val, err error) {
	return 0, nil, 0, nil, fmt.Errorf("unimplemented")
}

func (r *RootLoc) Max(withValue bool) (
	partitionID PartitionID, key Key, seq Seq, val Val, err error) {
	return 0, nil, 0, nil, fmt.Errorf("unimplemented")
}

func (r *RootLoc) Scan(fromKeyInclusive Key,
	toKeyExclusive Key,
	reverse bool, // When reverse flag is true, fromKey should be greater than toKey.
	partitions []PartitionID, // Scan only these partitions; nil for all partitions.
	withValue bool, // When withValue is false, nil value is passed to visitorFunc.
	fastSample bool, // Return subset of range that's fast / in memory (no disk hit).
	visitorFunc VisitorFunc) error {
	return fmt.Errorf("unimplemented")
}

func (r *RootLoc) Diff(partitionID PartitionID,
	fromSeqExclusive Seq, // Should be a Seq at some past commit point.
	withValue bool, // When withValue is false, nil value is passed to visitorFunc.
	visitorFunc VisitorFunc) error {
	return fmt.Errorf("unimplemented")
}

func (r *RootLoc) Rollback(partitionID PartitionID, seq Seq,
	exactToSeq bool) error {
	return fmt.Errorf("unimplemented")
}
