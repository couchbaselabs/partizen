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

	// TODO: Store-level read-only snapshots.
}

// NO_MATCH_SEQ can be used for Collection.Set()'s matchSeq to specify
// that the caller doesn't care about the existing item's Seq.
const NO_MATCH_SEQ = Seq(0xffffffffffffffff)

// CREATE_MATCH_SEQ can be used for Collection.Set()'s matchSeq to
// specify that the caller explicitly wants a creation instead of an
// update of an existing item.
const CREATE_MATCH_SEQ = Seq(0xfffffffffffffffe)

var ErrMatchSeq = errors.New("non-matching seq")
var ErrReadOnly = errors.New("read-only")

type Collection interface {
	Close() error

	Get(partitionId PartitionId, key Key, matchSeq Seq,
		withValue bool) (seq Seq, val Val, err error)

	// Set takes a newSeq that should be monotonically increasing.
	// The newSeq represents the new mutation's seq.  Use matchSeq of
	// NO_MATCH_SEQ if you don't care about the existing item, if any.
	// Use CREATE_MATCH_SEQ for matchSeq to ensure a creation.
	Set(partitionId PartitionId, key Key, matchSeq Seq,
		newSeq Seq, val Val) error

	// Del takes a newSeq that should be monotonically increasing.
	// The newSeq represents the new mutation, not the seq of the
	// existing item, if it exists, that's to-be-overwritten.  Use
	// matchSeq of NO_MATCH_SEQ if you don't want a seq match.
	Del(partitionId PartitionId, key Key, matchSeq Seq,
		newSeq Seq) error

	Batch([]Mutation) error

	Min(withValue bool) (
		partitionId PartitionId, key Key, seq Seq, val Val, err error)
	Max(withValue bool) (
		partitionId PartitionId, key Key, seq Seq, val Val, err error)

	// Scan returns a Cursor positioned "before" the first result, so
	// the caller should use Cursor.Next() for the first result.
	Scan(key Key,
		ascending bool,
		partitionIds []PartitionId, // Use nil for all partitions.
		withValue bool) (Cursor, error)

	// Snapshot returns a read-only snapshot of the Collection.
	Snapshot() (Collection, error)

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
	Close() error

	// Next returns a nil Key if the Cursor is done.
	Next() (PartitionId, Key, Seq, Val, error)
}

// A Mutation represents a mutation request on a key.
type Mutation struct {
	Key Key
	Seq Seq
	Val Val
	Op  MutationOp

	// A MatchSeq of NO_MATCH_SEQ is allowed.
	MatchSeq Seq
}

type MutationOp uint8

const (
	MUTATION_OP_NONE   MutationOp = 0
	MUTATION_OP_UPDATE MutationOp = 1
	MUTATION_OP_DELETE MutationOp = 2

	// FUTURE MutationOp's might include merging, visiting, etc.
)

type StoreOptions struct {
	CompareFuncs map[string]CompareFunc // Keyed by compareFuncName.

	DefaultPageSize  uint16 // Ex: 4096.
	DefaultMinFanOut uint16
	DefaultMaxFanOut uint16 // Usually (2*DefaultMinFanOut)+1.

	BufManager BufManager
}

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
