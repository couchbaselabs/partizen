package partizen

import (
	"errors"
	"io"
	"os"
)

type Key []byte
type Seq uint64
type Val []byte

const MAX_KEY_LEN = 1024 * 1024       // In bytes.
const MAX_VAL_LEN = 1024 * 1024 * 100 // In bytes.

// ----------------------------------------

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
	// Close allows the system to free/recycle resources.  Further
	// operations on a closed store or its child resources (e.g.,
	// collections) may have undefined effects.
	Close() error

	// CollectionNames returns the list of collections, appending to
	// the given rv slice (which may be nil or may be pre-allocated
	// with capacity).
	CollectionNames(rv []string) ([]string, error)

	// Caller must Close() the returned collection.
	GetCollection(collName string) (Collection, error)

	// Caller must Close() the returned collection.
	AddCollection(collName string, compareFuncName string) (Collection, error)

	// Behavior is undefined for any closed collections that the
	// caller still has references for.
	RemoveCollection(collName string) error

	// Returns the BufManager being used by this store, allowing
	// applications to have more control of memory buffer (BufRef)
	// management.
	BufManager() BufManager

	// TODO: Commit changes.
	// TODO: Store-level read-only snapshots.
}

// A Collection is an ordered set of key-value entries.
type Collection interface {
	// Further operations on a closed collection may have undefined
	// effects.
	Close() error

	// Get returns the item information, if it exists.
	Get(key Key, matchSeq Seq, withValue bool) (
		seq Seq, val Val, err error)

	// GetBufRef is a lower-level Get() API, where the caller
	// participates in memory management and must DecRef() the
	// returned itemBufRef.
	GetItemBufRef(key Key, matchSeq Seq, withValue bool) (
		itemBufRef ItemBufRef, err error)

	// Set takes a newSeq that should be monotonically increasing.
	// The newSeq represents the new mutation's seq.  Use matchSeq of
	// NO_MATCH_SEQ if you don't care about the existing item, if any.
	// Use CREATE_MATCH_SEQ for matchSeq to ensure a creation.
	Set(partitionId PartitionId, key Key, matchSeq Seq,
		newSeq Seq, val Val) error

	// SetItemBufRef is a lower-level Set() API, where the itemBufRef
	// should be immutable and will be ref-count incremented for a
	// potentially long-lived period of time.  The same itemBufRef may
	// be returned from a later call to GetItemBufRef().
	SetItemBufRef(matchSeq Seq, itemBufRef ItemBufRef) error

	// Del takes a newSeq that should be monotonically increasing.
	// The newSeq represents the new mutation, not the seq of the
	// existing item, if it exists, that's to-be-overwritten.  Use
	// matchSeq of NO_MATCH_SEQ if you don't want a seq match.
	Del(key Key, matchSeq Seq, newSeq Seq) error

	// Batch atomically updates the Collection from one or more
	// mutations.
	Batch([]Mutation) error

	// Min returns the smallest key item.
	Min(withValue bool) (
		partitionId PartitionId, key Key, seq Seq, val Val, err error)

	// Max returns the largest key item.
	Max(withValue bool) (
		partitionId PartitionId, key Key, seq Seq, val Val, err error)

	// MaxSeq returns the max seq for a partition.
	MaxSeq(partitionId PartitionId) (seq Seq, err error)

	// PartitionIds returns the partition id's of a collection.  If an
	// optional non-nil outPartitionIds is provided, it will be
	// append()'ed to and returned.
	PartitionIds(outPartitionIds PartitionIds) (PartitionIds, error)

	// Scan returns a Cursor positioned "before" the first result, so
	// the caller should use Cursor.Next() for the first result.
	Scan(key Key,
		ascending bool,
		partitionIds []PartitionId, // Use nil for all partitions.
		withValue bool,
		maxReadAhead int) (Cursor, error)

	// Snapshot returns a read-only snapshot of the collection.
	// Caller should Close() the returned read-only collection.
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
// caller should use Cursor.Next() for the first result (or error).
type Cursor interface {
	Close() error

	// Next returns a nil Key if the Cursor is done.
	Next() (PartitionId, Key, Seq, Val, error)

	// NextItemBufRef is lower-level than the Next() API, where the
	// caller participates in memory management and must DecRef() the
	// returned ItemBufRef.
	NextItemBufRef() (ItemBufRef, error)
}

// A Mutation represents a mutation request on a key.
type Mutation struct {
	ItemBufRef ItemBufRef
	Op         MutationOp

	// A MatchSeq of NO_MATCH_SEQ is allowed.
	MatchSeq Seq
}

var NilMutation Mutation

type MutationOp uint8

const (
	MUTATION_OP_NONE   MutationOp = 0
	MUTATION_OP_UPDATE MutationOp = 1
	MUTATION_OP_DELETE MutationOp = 2

	// FUTURE MutationOp's might include merging, visiting, etc.
)

func NewMutation(bm BufManager, op MutationOp,
	partitionId PartitionId, key []byte, seq Seq, val []byte,
	matchSeq Seq) (Mutation, error) {
	itemBufRef, err := NewItemBufRef(bm, partitionId, key, seq, val)
	if err != nil {
		return NilMutation, err
	}

	return Mutation{
		ItemBufRef: itemBufRef,
		Op:         op,
		MatchSeq:   matchSeq,
	}, nil
}

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

var ErrAlloc = errors.New("alloc failed")
var ErrCollectionExists = errors.New("collection exists")
var ErrCollectionUnknown = errors.New("unknown collection")
var ErrConcurrentMutation = errors.New("concurrent mutation")
var ErrConcurrentMutationRootNext = errors.New("concurrent mutation root next")
var ErrConcurrentMutationRootReclaimables = errors.New("concurrent mutation root reclaimables")
var ErrCursorClosed = errors.New("cursor closed sentinel")
var ErrMatchSeq = errors.New("non-matching seq")
var ErrNoCompareFunc = errors.New("no compare func")
var ErrReadOnly = errors.New("read-only")

// ------------------------------------------------------------

// NO_MATCH_SEQ can be used for Collection.Set()'s matchSeq to specify
// that the caller doesn't care about the existing item's Seq.
const NO_MATCH_SEQ = Seq(0xffffffffffffffff)

// CREATE_MATCH_SEQ can be used for Collection.Set()'s matchSeq to
// specify that the caller explicitly wants a creation instead of an
// update of an existing item.
const CREATE_MATCH_SEQ = Seq(0xfffffffffffffffe)
