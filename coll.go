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

func (r *RootLoc) Set(partitionId PartitionId, key Key, seq Seq, val Val) (
	err error) {
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
		r.Clear()
		r.Type = LocTypeNode
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

func (r *RootLoc) Scan(key Key,
	reverse bool,
	partitionIds []PartitionId, // Use nil for all partitions.
	withValue bool) (Cursor, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (r *RootLoc) Diff(partitionId PartitionId, seq Seq,
	exactToSeq bool) (
	Cursor, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (r *RootLoc) Rollback(partitionId PartitionId, seq Seq,
	exactToSeq bool) error {
	return fmt.Errorf("unimplemented")
}
