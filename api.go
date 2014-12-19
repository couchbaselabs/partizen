package partizen

import (
	"io"
	"os"
)

type Key []byte
type Val []byte
type PartitionId uint16
type Seq uint64

func StoreOpen(storeFile StoreFile, storeOptions *StoreOptions) (Store, error) {
	return storeOpen(storeFile, storeOptions)
}

type StoreFile interface {
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
}

type Store interface {
	CollectionNames() ([]string, error)
	GetCollection(collName string) (Collection, error)
	AddCollection(collName string, compareFuncName string) (Collection, error)
	RemoveCollection(collName string) error

	HasChanges() bool
	CommitChanges(*ChangeStats) error
	AbortChanges(*ChangeStats) error

	Snapshot() (Store, error)
	SnapshotPreviousCommit(numCommitsBack int) (Store, error)

	CopyTo(StoreFile, keepCommitsTo interface{}) error

	Stats(dest *StoreStats) error
}

type Collection interface {
	Get(partitionId PartitionId, key Key, withValue bool) (
		seq Seq, val Val, err error)

	// Set takes a seq number that should be monotonically increasing.
	Set(partitionId PartitionId, key Key, seq Seq, val Val) error

	// Merge takes a seq number that should be monotonically increasing.
	Merge(partitionId PartitionId, key Key, seq Seq, mergeFunc MergeFunc) error

	// Del takes a seq number that should be monotonically increasing.
	Del(partitionId PartitionId, key Key, seq Seq) error

	Min(withValue bool) (
		partitionId PartitionId, key Key, seq Seq, val Val, err error)
	Max(withValue bool) (
		partitionId PartitionId, key Key, seq Seq, val Val, err error)

	// Scan provides range results in [fromKeyInclusive...toKeyExclusive) sequence,
	// even when the reverse flag is true.
	Scan(fromKeyInclusive Key,
		toKeyExclusive Key,
		reverse bool, // When reverse flag is true, fromKey should be greater than toKey.
		partitionIds []PartitionId, // Scan only these partitions; nil for all partitions.
		withValue bool, // When withValue is false, nil value is passed to visitorFunc.
		visitorFunc VisitorFunc) error

	Diff(partitionId PartitionId,
		fromSeqExclusive Seq, // Should be a Seq at some past commit point.
		withValue bool, // When withValue is false, nil value is passed to visitorFunc.
		visitorFunc VisitorFunc) error

	// Rollback rewindws a partition back to at mox a previous seq
	// number.  If the rollback operation can't hit the exact seq
	// number but must go further back into the past, then if
	// exactToSeq is true, the rollback will error; if exactToSeq is
	// false then the rollback may be further into the past than the
	// seq number.
	Rollback(partitionId PartitionId, seq Seq, exactToSeq bool) error
}

type StoreOptions struct {
	CompareFuncs map[string]CompareFunc // Keyed by compareFuncName.

	DefaultPageSize  uint16 // Ex: 4096.
	DefaultMinDegree uint16
	DefaultMaxDegree uint16 // Usually (2*DefaultMinDegree)+1.

	BufManager BufManager
}

type VisitorFunc func(partitionId PartitionId, key Key, seq Seq, val Val) bool

type MergeFunc func(base, a, b []byte) ([]byte, error)

// A CompareFunc should return 0 if a == b, -1 if a < b,
// and +1 if a > b.  For example: bytes.Compare()
type CompareFunc func(a, b []byte) int

type ChangeStats struct {
	// TODO.
}

type StoreStats struct {
	// TODO.
}

type BufManager interface {
	Alloc(size int) []byte
	Len(buf []byte) int
	AddRef(buf []byte)
	DecRef(buf []byte) bool
	Visit(buf []byte, from, to int,
		partVisitor func(partBuf []byte), partFrom, partTo int)
}
