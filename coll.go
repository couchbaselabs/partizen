package partizen

import (
	"fmt"
)

func (r *RootLoc) Get(partitionId PartitionId, key Key, withValue bool) (
	seq Seq, val Val, err error) {
	r.m.Lock()
	node := r.node
	r.m.Unlock()

	return r.NodeGet(node, partitionId, key, withValue)
}

func (r *RootLoc) Set(partitionId PartitionId, key Key, seq Seq, val Val) (err error) {
	r.store.startChanges()

	r.m.Lock()
	node := r.node
	r.m.Unlock()

	n, err := r.NodeSet(node, partitionId, key, seq, val)
	if err != nil {
		return err
	}

	r.m.Lock()
	if r.node == node {
		r.ClearLoc(LocTypeNode)
		r.node = n
	} else {
		err = fmt.Errorf("concurrent modification")
	}
	r.m.Unlock()

	return err
}

func (r *RootLoc) Merge(partitionId PartitionId, key Key, seq Seq,
	mergeFunc MergeFunc) error {
	return fmt.Errorf("unimplemented")
}

func (r *RootLoc) Del(partitionId PartitionId, key Key, seq Seq) error {
	return fmt.Errorf("unimplemented")
}

func (r *RootLoc) Min(withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	return 0, nil, 0, nil, fmt.Errorf("unimplemented")
}

func (r *RootLoc) Max(withValue bool) (
	partitionId PartitionId, key Key, seq Seq, val Val, err error) {
	return 0, nil, 0, nil, fmt.Errorf("unimplemented")
}

func (r *RootLoc) Scan(fromKeyInclusive Key,
	toKeyExclusive Key,
	reverse bool, // When reverse flag is true, fromKey should be greater than toKey.
	partitions []PartitionId, // Scan only these partitions; nil for all partitions.
	withValue bool, // When withValue is false, nil value is passed to visitorFunc.
	visitorFunc VisitorFunc) error {
	return fmt.Errorf("unimplemented")
}

func (r *RootLoc) Diff(partitionId PartitionId,
	fromSeqExclusive Seq, // Should be a Seq at some past commit point.
	withValue bool, // When withValue is false, nil value is passed to visitorFunc.
	visitorFunc VisitorFunc) error {
	return fmt.Errorf("unimplemented")
}

func (r *RootLoc) Rollback(partitionId PartitionId, seq Seq,
	exactToSeq bool) error {
	return fmt.Errorf("unimplemented")
}
