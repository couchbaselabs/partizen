package partizen

import (
	"fmt"
)

func (c *CollDef) Get(partitionID PartitionID, key Key) (
	seq Seq, val Val, err error) {
	return c.GetEx(partitionID, key, true, false)
}

func (c *CollDef) GetEx(partitionID PartitionID,
	key Key,
	withValue bool, // When withValue is false, value will be nil.
	fastSample bool) ( // Return result only if fast / in memory (no disk hit).
	seq Seq, val Val, err error) {
	return 0, nil, fmt.Errorf("unimplemented")
}

func (c *CollDef) Set(partitionID PartitionID, key Key, seq Seq,
	val Val) error {
	return fmt.Errorf("unimplemented")
}

func (c *CollDef) Merge(partitionID PartitionID, key Key, seq Seq,
	mergeFunc MergeFunc) error {
	return fmt.Errorf("unimplemented")
}

func (c *CollDef) Del(partitionID PartitionID, key Key, seq Seq) error {
	return fmt.Errorf("unimplemented")
}

func (c *CollDef) Min(withValue bool) (
	partitionID PartitionID, key Key, seq Seq, val Val, err error) {
	return 0, nil, 0, nil, fmt.Errorf("unimplemented")
}

func (c *CollDef) Max(withValue bool) (
	partitionID PartitionID, key Key, seq Seq, val Val, err error) {
	return 0, nil, 0, nil, fmt.Errorf("unimplemented")
}

func (c *CollDef) Scan(fromKeyInclusive Key,
	toKeyExclusive Key,
	reverse bool, // When reverse flag is true, fromKey should be greater than toKey.
	partitions []PartitionID, // Scan only these partitions; nil for all partitions.
	withValue bool, // When withValue is false, nil value is passed to visitorFunc.
	fastSample bool, // Return subset of range that's fast / in memory (no disk hit).
	visitorFunc VisitorFunc) error {
	return fmt.Errorf("unimplemented")
}

func (c *CollDef) Diff(partitionID PartitionID,
	fromSeqExclusive Seq, // Should be a Seq at some past commit point.
	withValue bool, // When withValue is false, nil value is passed to visitorFunc.
	visitorFunc VisitorFunc) error {
	return fmt.Errorf("unimplemented")
}

func (c *CollDef) Rollback(partitionID PartitionID, seq Seq,
	exactToSeq bool) error {
	return fmt.Errorf("unimplemented")
}
