package partizen

import (
	"fmt"
	"io"
	"sync"
)

const MAX_UINT64 = uint64(0xffffffffffffffff)

// A store implements the Store interface.
type store struct {
	// These fields are immutable.
	storeFile    StoreFile
	storeOptions StoreOptions
	bufManager   BufManager
	readOnly     bool
	header       *Header

	// These fields are mutable, protected by the m lock.
	m      sync.Mutex
	footer *Footer
}

// A Header is stored at the head (0th position) of the storage file,
// where we follow the given field ordering.
type Header struct {
	Magic0 uint64 // See HEADER_MAGIC0.
	Magic1 uint64 // See HEADER_MAGIC1.

	// UUID helps determine if storage copies are possibly related.
	UUID uint64

	// Version bytes are '\0' terminated semver; see HEADER_VERSION.
	Version [64]byte

	// Extra space in header, for future proofing.
	ExtrasLen uint16
	ExtrasVal []byte
}

const HEADER_MAGIC0 = 0xea45113d
const HEADER_MAGIC1 = 0xc03c1b04
const HEADER_VERSION = "0.0.0" // Follows semver conventions.

// A Footer is the last record appended to storage file whenever
// there's a successful Store.Commit().
type Footer struct {
	StoreDefLoc StoreDefLoc // Location of StoreDef.

	// The len(Footer.Collections) equals len(StoreDef.CollectionDefs)
	// and are 1-to-1 position matched with the
	// StoreDef.CollectionDefs.
	Collections []*collection
}

// A StoreDefLoc represents a persisted location of a StoreDef.
type StoreDefLoc struct {
	Loc
	storeDef *StoreDef // If nil, runtime representation not loaded yet.
}

// A StoreDef represents a store definition, holding "slow-changing"
// configuration metadata.  We keep slow changing metadata separate
// from the footer for efficiency, but use JSON encoding of the
// persisted StoreDef for diagnosability.
type StoreDef struct {
	CollectionDefs []*CollectionDef
}

// ----------------------------------------

// A CollectionDef represents a collection definition and is persisted
// as JSON for diagnosability.
type CollectionDef struct {
	Name            string
	CompareFuncName string
	MinFanOut       uint16
	MaxFanOut       uint16 // Usually (2*MinFanOut)+1.
}

// A collection implements the Collection interface and holds one
// ref-count on the root KeySeqLocRef.
type collection struct {
	root *KeySeqLocRef // Mutatable, access covered by store.m lock.
	refs int32         // Mutatable, access covered by store.m lock.

	// The following fields are immutable.
	store       *store // Pointer to parent store.
	name        string
	compareFunc CompareFunc
	minFanOut   uint16
	maxFanOut   uint16
	readOnly    bool
}

// ----------------------------------------

// A KeySeqLocRef represents a ref-counted reference to a KeySeqLoc.
type KeySeqLocRef struct {
	il *KeySeqLoc // Immutable.

	refs int32 // Mutator must have store.m locked.

	// We might own a reference count on a chained KeySeqLocRef.
	next *KeySeqLocRef // Mutator must have store.m locked.

	reclaimables ReclaimableKeySeqLocs
}

// AddRef must be invoked by caller with collection.store.m locked.
func (r *KeySeqLocRef) addRef() (*KeySeqLocRef, *KeySeqLoc) {
	if r == nil {
		return nil, nil
	}

	if r.refs <= 0 {
		panic("KeySeqLocRef.refs addRef saw underflow")
	}

	r.refs++

	return r, r.il
}

// DecRef must be invoked by caller with collection.store.m locked.
func (r *KeySeqLocRef) decRef(bufManager BufManager) *KeySeqLocRef {
	if r == nil {
		return nil
	}

	if r.refs <= 0 {
		panic("KeySeqLocRef.refs defRef saw underflow")
	}

	r.refs--
	if r.refs <= 0 {
		r.next.decRef(bufManager)
		r.next = nil

		if r.reclaimables != nil {
			r.reclaimables.Reset(bufManager)
		}

		return nil
	}

	return r
}

// ReclaimableKeySeqLocs is used to track KeySeqLocs that were
// obsoleted/replaced by mutations and so can be reclaimed and
// recycled after root ref-counts reach zero.
type ReclaimableKeySeqLocs PtrKeySeqLocsAppendable

// ----------------------------------------

// A KeySeqLoc represents a Key, Seq and Loc association.
type KeySeqLoc struct {
	Key Key // The minimum Key.
	Seq Seq // The maximum Seq.
	Loc Loc
}

var NilKeySeqLoc KeySeqLoc

func (iloc *KeySeqLoc) GetPartitions(
	bufManager BufManager, r io.ReaderAt) (
	*Partitions, error) {
	loc, err := iloc.Loc.Read(bufManager, r)
	if err != nil {
		return nil, err
	}

	if loc.Type == LocTypeNode {
		return loc.node.GetPartitions(), nil
	}

	if loc.Type != LocTypeVal {
		return nil, fmt.Errorf("defs: GetPartitions type, iloc: %v", iloc)
	}

	if loc.leafValBufRef == nil || loc.leafValBufRef.IsNil() {
		return nil, fmt.Errorf("defs: GetPartitions leafValBufRef nil")
	}

	return &Partitions{
		PartitionIds: []PartitionId{loc.leafPartitionId},
		KeyKeySeqLocs: [][]KeyKeySeqLoc{[]KeyKeySeqLoc{KeyKeySeqLoc{
			Key:       iloc.Key,
			KeySeqLoc: iloc,
		}}},
	}, nil
}

// ----------------------------------------

// A Loc represents the location of a byte range persisted or
// soon-to-be-persisted to storage.  Field sizes are carefully chosed
// to add up to 128 bits.
type Loc struct {
	// Offset is relative to start of file.  Offset of 0 means the
	// pointed-to bytes buf are not persisted yet.
	Offset   uint64
	Size     uint32
	Type     uint8
	Flags    uint8
	CheckSum uint16 // An optional checksum of the bytes buf.

	// Transient; only used when Type is LocTypeVal.  If nil,
	// in-memory representation hasn't been loaded yet.  When non-nil,
	// it might be dirty (not yet persisted, Offset == 0).
	//
	// TODO: Need lock to protect swizzling?
	leafValBufRef BufRef

	// Transient; only used when Type is LocTypeVal and when
	// leafValBufRef is non-nil.
	//
	// TODO: Need lock to protect swizzling?
	leafPartitionId PartitionId

	// Transient; only used when Type is LocTypeNode.  If nil,
	// in-memory representation hasn't been loaded yet.  When non-nil,
	// it might be dirty (not yet persisted, Offset == 0).
	//
	// TODO: Need lock to protect swizzling?
	node *Node
}

const (
	// Allowed values for Loc.Type field...
	LocTypeZero     uint8 = 0x00
	LocTypeNode     uint8 = 0x01 // For partizen tree interior node.
	LocTypeVal      uint8 = 0x02 // For partizen tree leaf val.
	LocTypeStoreDef uint8 = 0x03
)

// Returns the Loc's leafValBufRef, adding an additional ref-count
// that must be DecRef()'ed by the caller.
func (loc *Loc) LeafValBufRef(bm BufManager) BufRef {
	if loc.leafValBufRef == nil || loc.leafValBufRef.IsNil() {
		return nil
	}

	loc.leafValBufRef.AddRef(bm)

	return loc.leafValBufRef
}

// ----------------------------------------

type KeySeqLocs interface {
	Len() int
	Key(idx int) Key
	Seq(idx int) Seq
	Loc(idx int) *Loc
	KeySeqLoc(idx int) *KeySeqLoc
}

type KeySeqLocsAppendable interface {
	KeySeqLocs

	Append(KeySeqLoc) KeySeqLocsAppendable
}

type PtrKeySeqLocsAppendable interface {
	Len() int
	KeySeqLoc(idx int) *KeySeqLoc
	Append(*KeySeqLoc)
	Reset(bufManager BufManager)
}

// ----------------------------------------

// KeySeqLocsArray implements the KeySeqLocs and KeySeqLocsAppendable
// interfaces.
type KeySeqLocsArray []KeySeqLoc

func (a KeySeqLocsArray) Len() int {
	return len(a)
}

func (a KeySeqLocsArray) Key(idx int) Key {
	return a[idx].Key
}

func (a KeySeqLocsArray) Seq(idx int) Seq {
	return a[idx].Seq
}

func (a KeySeqLocsArray) Loc(idx int) *Loc {
	return &a[idx].Loc
}

func (a KeySeqLocsArray) KeySeqLoc(idx int) *KeySeqLoc {
	return &a[idx]
}

func (a KeySeqLocsArray) Append(x KeySeqLoc) KeySeqLocsAppendable {
	return append(a, x)
}

// ----------------------------------------

// PtrKeySeqLocsArray implements the KeySeqLocs and KeySeqLocsAppendable
// interfaces.
type PtrKeySeqLocsArray []*KeySeqLoc

func (a PtrKeySeqLocsArray) Len() int {
	return len(a)
}

func (a PtrKeySeqLocsArray) Key(idx int) Key {
	return a[idx].Key
}

func (a PtrKeySeqLocsArray) Seq(idx int) Seq {
	return a[idx].Seq
}

func (a PtrKeySeqLocsArray) Loc(idx int) *Loc {
	return &a[idx].Loc
}

func (a PtrKeySeqLocsArray) KeySeqLoc(idx int) *KeySeqLoc {
	return a[idx]
}

func (a PtrKeySeqLocsArray) Append(x KeySeqLoc) KeySeqLocsAppendable {
	return append(a, &x)
}

// ----------------------------------------

// PtrKeySeqLocsArrayHolder implements PtrKeySeqLocsAppendable interface.
type PtrKeySeqLocsArrayHolder struct {
	a PtrKeySeqLocsArray
}

func (a *PtrKeySeqLocsArrayHolder) Len() int {
	if a == nil || a.a == nil {
		return 0
	}

	return a.a.Len()
}

func (a *PtrKeySeqLocsArrayHolder) KeySeqLoc(idx int) *KeySeqLoc {
	if a == nil || a.a == nil {
		return nil
	}

	return a.a.KeySeqLoc(idx)
}

func (a *PtrKeySeqLocsArrayHolder) Append(il *KeySeqLoc) {
	if a == nil {
		return
	}

	a.a = append(a.a, il)
}

func (a *PtrKeySeqLocsArrayHolder) Reset(bufManager BufManager) {
	if a == nil || a.a == nil {
		return
	}

	for i, il := range a.a {
		il.Loc.leafValBufRef.DecRef(bufManager)
		il.Loc.leafValBufRef = nil
		il.Loc.leafPartitionId = 0
		il.Loc.node = nil

		a.a[i] = nil
	}
}

// ----------------------------------------

// Partitions represent a mapping of PartitionId's to KeyKeySeqLoc's.
type Partitions struct {
	PartitionIds PartitionIds

	// 1-to-1 paired with entries from PartitionIds, so
	// len(KeyKeySeqLocs) == len(PartitionIds).
	KeyKeySeqLocs [][]KeyKeySeqLoc
}

// KeyKeySeqLoc represents a Key associated with a KeySeqLoc.  We
// can't just use KeySeqLoc's Key, because a KeySeqLoc's Key
// represents the minimum key for a KeySeqLoc.
type KeyKeySeqLoc struct {
	Key       Key // Key can be >= KeySeqLoc.Key due to partition scope.
	KeySeqLoc *KeySeqLoc
}

// ----------------------------------------

// MutationCallback returns true if the mutation should proceed, or
// false if the mutation should be skipped.  In either case, the
// processing of the batch of mutations will continue.  The existing
// may be nil in case there's no previous item.  The isVal is true if
// the existing is for a leaf level KeySeqLoc (e.g., LocTypeVal).
type MutationCallback func(existing *KeySeqLoc, isVal bool,
	mutation *Mutation) bool

var NilMutation Mutation
