package partizen

import (
	"io"
	"os"
)

type Key []byte
type Val []byte
type PartitionID uint16
type Seq uint64

type StoreFile interface {
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
}

func Open(sf StoreFile, options StoreOptions) (Store, error) {
	return nil, nil
}

type StoreOptions struct {
	CompareFuncs map[string]CompareFunc // Keyed by compareFuncName.

	BufAlloc   func(size int) []byte
	BufAddRef  func(buf []byte)
	BufDecRef  func(buf []byte)
	BufGetNext func(buf []byte) (bufNext []byte)
	BufSetNext func(buf, bufNext []byte)
}

// A CompareFunc should return 0 if a == b, -1 if a < b, and +1 if a >
// b. For example: bytes.Compare()
type CompareFunc func(a, b []byte) int

type ChangeStats struct{}

type StoreStats struct{}

type Store interface {
	CommitChanges() (ChangeStats, error)
	AbortChanges() (ChangeStats, error)

	CopyTo(StoreFile, keepCommitsTo interface{}) error

	Snapshot() (Store, error)

	SnapshotPreviousCommit(numCommitsBack int) (Store, error)

	CollectionNames() ([]string, error)
	GetCollection(collName string) (Collection, error)
	AddCollection(collName string, compareFuncName string) (Collection, error)
	RemoveCollection(collName string) error

	Stats(dest *StoreStats) error
}

type VisitorFunc func(partition PartitionID, key []byte, seq Seq, value []byte) bool

type MergeFunc func(base, a, b []byte) ([]byte, error)

type Collection interface {
	CompareFunc() CompareFunc

	Get(partition PartitionID,
		key []byte,
		withValue bool, // When withValue is false, value will be nil.
		fastSample bool) ( // Return result only if fast / in memory (no disk hit).
		seq Seq, value []byte, err error)

	// Set takes a seq number that should be monotonically increasing.
	Set(partition PartitionID, key []byte, seq Seq, value []byte) error

	// Merge takes a seq number that should be monotonically increasing.
	Merge(partition PartitionID, key []byte, seq Seq, mergeFunc MergeFunc) error

	// Del takes a seq number that should be monotonically increasing.
	Del(partition PartitionID, key []byte, seq Seq) error

	Min(withValue bool) (
		partition PartitionID, key []byte, seq Seq, value []byte, err error)
	Max(withValue bool) (
		partition PartitionID, key []byte, seq Seq, value []byte, err error)

	// Scan provides range results in [fromKeyInclusive...toKeyExclusive) sequence,
	// even when the reverse flag is true.
	Scan(fromKeyInclusive []byte,
		toKeyExclusive []byte,
		reverse bool, // When reverse flag is true, fromKey should be greater than toKey.
		partitions []PartitionID, // Focus on subset of partitions; nil for all partitions.
		withValue bool, // When withValue is false, value will be nil.
		fastSample bool, // Return subset of range that's fast / in memory (no disk hit).
		visitorFunc VisitorFunc) error

	Diff(partition PartitionID,
		fromSeqExclusive Seq,
		withValue bool,
		visitorFunc VisitorFunc) error

	// Rollback rewindws a partition back to at mox a previous seq
	// number.  If the rollback operation can't hit the exact seq
	// number but must go further back into the past, then
	// if exact is true, the rollback will error; if exact is false
	// then the rollback may be further into the past than the
	// seq number.
	Rollback(partition PartitionID, seq Seq, exact bool) error
}
