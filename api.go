package partizen

import (
	"errors"
	"io"
	"os"
)

type Key []byte
type Val []byte
type Seq uint64
type PartitionId uint16
type PartitionIds []PartitionId

func (a PartitionIds) Len() int {
	return len(a)
}

func (a PartitionIds) Less(i, j int) bool {
	return a[i] < a[j]
}

func (a PartitionIds) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// ----------------------------------------

// A Store is a set of collections.
type Store interface {
	Close() error

	CollectionNames([]string) ([]string, error)

	// Caller must Close() the returned Collection.
	GetCollection(collName string) (Collection, error)

	// Caller must Close() the returned Collection.
	AddCollection(collName string, compareFuncName string) (Collection, error)

	// Behavior is undefined for any closed collections that the
	// caller still has references for.
	RemoveCollection(collName string) error

	// TODO: Commit changes.
	// TODO: Store-level read-only snapshots.
}

// A Collection is an ordered set of key-value entries.
type Collection interface {
	Close() error

	Get(partitionId PartitionId, key Key, matchSeq Seq,
		withValue bool) (seq Seq, val Val, err error)

	GetBufRef(partitionId PartitionId, key Key, matchSeq Seq,
		withValue bool) (seq Seq, val BufRef, err error)

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
	// Caller should Close() the returned read-only Collection.
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
	Rollback(partitionId PartitionId, seq Seq, exactToSeq bool) (
		rollbackedToSeq Seq, err error)
}

// A Cursor starts with a position "before" the first result, so the
// caller should use Cursor.Next() for the first result.
type Cursor interface {
	Close() error

	// Next returns a nil Key if the Cursor is done.
	Next() (PartitionId, Key, Seq, Val, error)
}

// A Mutation represents a mutation request on a key.
type Mutation struct {
	PartitionId PartitionId
	Key         Key
	Seq         Seq
	Val         Val
	Op          MutationOp

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

// ------------------------------------------------------------

// StoreOpen is used to create a store and reopen a previous store.
func StoreOpen(storeFile StoreFile, storeOptions *StoreOptions) (
	Store, error) {
	return storeOpen(storeFile, storeOptions)
}

// StoreFile represents the persistence operations needed by a store.
type StoreFile interface {
	io.ReaderAt
	io.WriterAt
	Stat() (os.FileInfo, error)
	Truncate(size int64) error
}

// StoreOptions represent options when (re-)opening a store.
type StoreOptions struct {
	// Optional, may be nil.  A compareFuncName of "" means use
	// bytes.Compare.  CompareFuncs must be immutable and should be
	// the same (or superset of functions) when re-opening a store.
	CompareFuncs map[string]CompareFunc // Keyed by compareFuncName.

	DefaultPageSize  uint16 // In bytes.  Ex: 4096.
	DefaultMinFanOut uint16 // Tree node fan-out.
	DefaultMaxFanOut uint16 // Tree node fan-in.  Ex: (2*DefaultMinFanOut)+1.

	// Optional, may be nil.
	BufManager BufManager
}

// A CompareFunc should return 0 if a == b, -1 if a < b,
// and +1 if a > b.  For example: bytes.Compare()
type CompareFunc func(a, b []byte) int

// ------------------------------------------------------------

var ErrReadOnly = errors.New("read-only")
var ErrNoCompareFunc = errors.New("no compare func")
var ErrUnknownCollection = errors.New("unknown collection")
var ErrCollectionExists = errors.New("collection exists")
var ErrMatchSeq = errors.New("non-matching seq")
var ErrConcurrentMutation = errors.New("concurrent mutation")
var ErrConcurrentMutationChain = errors.New("concurrent mutation chain")

// ------------------------------------------------------------

// NO_MATCH_SEQ can be used for Collection.Set()'s matchSeq to specify
// that the caller doesn't care about the existing item's Seq.
const NO_MATCH_SEQ = Seq(0xffffffffffffffff)

// CREATE_MATCH_SEQ can be used for Collection.Set()'s matchSeq to
// specify that the caller explicitly wants a creation instead of an
// update of an existing item.
const CREATE_MATCH_SEQ = Seq(0xfffffffffffffffe)

// ------------------------------------------------------------

// A BufManager represents the functionality needed for memory
// management.
type BufManager interface {
	// Alloc might return nil if no memory is available.  The optional
	// partUpdater callback, if non-nil, is invoked via
	// BufRef.Update().
	Alloc(size int,
		partUpdater func(partBuf []byte, partFrom, partTo int) bool) BufRef
}

// A BufRef represents a reference to memory that's managed by a
// BufManager.  This extra level of indirection via a flyweight
// pattern allows a BufManager to optionally avoid GC scan traversals
// and other optimizations like chunking.
type BufRef interface {
	IsNil() bool

	Len(bm BufManager) int

	AddRef(bm BufManager)
	DecRef(bm BufManager)

	// A buffer might be implemented as one or more chunks, which can
	// be mutated by Update().  The callback can return false to stop
	// the callbacks.
	Update(bm BufManager, from, to int,
		partUpdater func(partBuf []byte, partFrom, partTo int) bool) BufRef

	// A buffer might be implemented as one or more chunks, which can
	// be visited in read-only fashion by Visit().  The callback can
	// return false to stop the callbacks.
	Visit(bm BufManager, from, to int,
		partVisitor func(partBuf []byte, partFrom, partTo int) bool) BufRef
}
