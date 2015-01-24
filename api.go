package partizen

import (
	"errors"
	"io"
	"os"
)

type Key []byte
type Val []byte
type PartitionId uint16
type Seq uint64

const NO_MATCH_SEQ = Seq(0xffffffffffffffff)

var ErrMatchSeq = errors.New("non-matching seq")

func StoreOpen(storeFile StoreFile, storeOptions *StoreOptions) (
	Store, error) {
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
}

// TODO: Should have separation of insert versus update versus upsert?

type Collection interface {
	Get(partitionId PartitionId, key Key, matchSeq Seq,
		withValue bool) (seq Seq, val Val, err error)

	// Set takes a newSeq that should be monotonically increasing.
	// The newSeq represents the new mutation, not the seq of the
	// existing item, if it exists, that's to-be-overwritten.  Use
	// matchSeq of NO_MATCH_SEQ if you don't want a seq match.
	Set(partitionId PartitionId, key Key, matchSeq Seq,
		newSeq Seq, val Val) error

	// Merge takes a newSeq that should be monotonically increasing.
	// The newSeq represents the new mutation, not the seq of the
	// existing item, if it exists, that's to-be-overwritten.  Use
	// matchSeq of NO_MATCH_SEQ if you don't want a seq match.
	Merge(partitionId PartitionId, key Key, matchSeq Seq,
		newSeq Seq, val Val, mergeFunc MergeFunc) error

	// Del takes a newSeq that should be monotonically increasing.
	// The newSeq represents the new mutation, not the seq of the
	// existing item, if it exists, that's to-be-overwritten.  Use
	// matchSeq of NO_MATCH_SEQ if you don't want a seq match.
	Del(partitionId PartitionId, key Key, matchSeq Seq,
		newSeq Seq) error

	Min(withValue bool) (
		partitionId PartitionId, key Key, seq Seq, val Val, err error)
	Max(withValue bool) (
		partitionId PartitionId, key Key, seq Seq, val Val, err error)

	Scan(key Key,
		reverse bool,
		partitionIds []PartitionId, // Use nil for all partitions.
		withValue bool) (Cursor, error)

	// The seq should be the Seq that was at or before some past
	// commit point.
	Diff(partitionId PartitionId, seq Seq, exactToSeq bool) (
		Cursor, error)

	// Rollback rewinds a partition back to at mox a previous seq
	// number.  If the rollback operation can't hit the exact seq
	// number but must go further back into the past, then if
	// exactToSeq is true, the rollback will error; if exactToSeq is
	// false then the rollback may be further into the past than the
	// seq number.
	Rollback(partitionId PartitionId, seq Seq, exactToSeq bool) error
}

type Cursor interface {
	// TODO.
}

type StoreOptions struct {
	CompareFuncs map[string]CompareFunc // Keyed by compareFuncName.

	DefaultPageSize  uint16 // Ex: 4096.
	DefaultMinFanOut uint16
	DefaultMaxFanOut uint16 // Usually (2*DefaultMinFanOut)+1.

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
		partVisitor func(partBuf []byte, partFrom, partTo int))
}
