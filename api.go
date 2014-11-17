package partizen

import (
	"io"
	"os"
)

type Key []byte
type Val []byte
type PartitionID uint16
type Seq uint64

func Open(storeFile StoreFile, storeOptions StoreOptions) (Store, error) {
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
	Get(partition PartitionID,
		key Key,
		withValue bool, // When withValue is false, value will be nil.
		fastSample bool) ( // Return result only if fast / in memory (no disk hit).
		seq Seq, val Val, err error)

	// Set takes a seq number that should be monotonically increasing.
	Set(partition PartitionID, key Key, seq Seq, val Val) error

	// Merge takes a seq number that should be monotonically increasing.
	Merge(partition PartitionID, key Key, seq Seq, mergeFunc MergeFunc) error

	// Del takes a seq number that should be monotonically increasing.
	Del(partition PartitionID, key Key, seq Seq) error

	Min(withValue bool) (
		partition PartitionID, key Key, seq Seq, val Val, err error)
	Max(withValue bool) (
		partition PartitionID, key Key, seq Seq, val Val, err error)

	// Scan provides range results in [fromKeyInclusive...toKeyExclusive) sequence,
	// even when the reverse flag is true.
	Scan(fromKeyInclusive Key,
		toKeyExclusive Key,
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

type StoreOptions struct {
	CompareFuncs map[string]CompareFunc // Keyed by compareFuncName.

	BufAlloc func(size int) []byte
	BufLen   func(buf []byte) int
	BufVisit func(buf []byte, from, to int,
		partVisitor func(partBuf []byte), partFrom, partTo int)
	BufAddRef func(buf []byte)
	BufDecRef func(buf []byte)
}

type VisitorFunc func(partition PartitionID, key Key, seq Seq, val Val) bool

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
