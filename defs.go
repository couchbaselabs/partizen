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
// ref-count on the root ChildLocRef.
type collection struct {
	root *ChildLocRef // Mutatable, access covered by store.m lock.
	refs int32        // Mutatable, access covered by store.m lock.

	// The following fields are immutable.
	store       *store // Pointer to parent store.
	name        string
	compareFunc CompareFunc
	minFanOut   uint16
	maxFanOut   uint16
	readOnly    bool
}

// ----------------------------------------

// A ChildLocRef represents a ref-counted reference to a ChildLoc.
type ChildLocRef struct {
	il *ChildLoc // Immutable.

	refs int32 // Mutator must have store.m locked.

	// We might own a reference count on a chained ChildLocRef.
	next *ChildLocRef // Mutator must have store.m locked.

	reclaimables ReclaimableChildLocs
}

// AddRef must be invoked by caller with collection.store.m locked.
func (r *ChildLocRef) addRef() (*ChildLocRef, *ChildLoc) {
	if r == nil {
		return nil, nil
	}

	if r.refs <= 0 {
		panic("ChildLocRef.refs addRef saw underflow")
	}

	r.refs++

	return r, r.il
}

// DecRef must be invoked by caller with collection.store.m locked.
func (r *ChildLocRef) decRef(bufManager BufManager) *ChildLocRef {
	if r == nil {
		return nil
	}

	if r.refs <= 0 {
		panic("ChildLocRef.refs defRef saw underflow")
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

// ReclaimableChildLocs is used to track ChildLocs that were
// obsoleted/replaced by mutations and so can be reclaimed and
// recycled after root ref-counts reach zero.
type ReclaimableChildLocs PtrChildLocsAppendable

// ----------------------------------------

// A ChildLoc represents a Key, Seq and Loc association.
type ChildLoc struct {
	Key Key // The minimum Key.
	Seq Seq // The maximum Seq.
	Loc Loc
}

var NilChildLoc ChildLoc

func (ksl *ChildLoc) GetPartitions(
	bufManager BufManager, r io.ReaderAt) (
	*Partitions, error) {
	loc, err := ksl.Loc.Read(bufManager, r)
	if err != nil {
		return nil, err
	}

	if loc.Type == LocTypeNode {
		return loc.node.GetPartitions(), nil
	}

	if loc.Type != LocTypeVal {
		return nil, fmt.Errorf("defs: GetPartitions type, ksl: %v", ksl)
	}

	if loc.itemBufRef == nil || loc.itemBufRef.IsNil() {
		return nil, fmt.Errorf("defs: GetPartitions itemBufRef nil")
	}

	partitionId := loc.itemBufRef.PartitionId(bufManager)

	return &Partitions{
		PartitionIds: []PartitionId{partitionId},
		KeyChildLocs: [][]KeyChildLoc{[]KeyChildLoc{KeyChildLoc{
			Key:      ksl.Key,
			ChildLoc: ksl,
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
	itemBufRef ItemBufRef

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

// ItemBufRef returns the Loc's itemBufRef, adding an additional
// ref-count that must be DecRef()'ed by the caller.
func (loc *Loc) ItemBufRef(bm BufManager) ItemBufRef {
	if loc.itemBufRef == nil || loc.itemBufRef.IsNil() {
		return nil
	}

	loc.itemBufRef.AddRef(bm)

	return loc.itemBufRef
}

// ----------------------------------------

type ChildLocs interface {
	Len() int
	Key(idx int) Key
	Seq(idx int) Seq
	Loc(idx int) *Loc
	ChildLoc(idx int) *ChildLoc
}

type ChildLocsAppendable interface {
	ChildLocs

	Append(ChildLoc) ChildLocsAppendable
}

type PtrChildLocsAppendable interface {
	Len() int
	ChildLoc(idx int) *ChildLoc
	Append(*ChildLoc)
	Reset(bufManager BufManager)
}

// ----------------------------------------

// ChildLocsArray implements the ChildLocs and ChildLocsAppendable
// interfaces.
type ChildLocsArray []ChildLoc

func (a ChildLocsArray) Len() int {
	return len(a)
}

func (a ChildLocsArray) Key(idx int) Key {
	return a[idx].Key
}

func (a ChildLocsArray) Seq(idx int) Seq {
	return a[idx].Seq
}

func (a ChildLocsArray) Loc(idx int) *Loc {
	return &a[idx].Loc
}

func (a ChildLocsArray) ChildLoc(idx int) *ChildLoc {
	return &a[idx]
}

func (a ChildLocsArray) Append(x ChildLoc) ChildLocsAppendable {
	return append(a, x)
}

// ----------------------------------------

// PtrChildLocsArray implements the ChildLocs and ChildLocsAppendable
// interfaces.
type PtrChildLocsArray []*ChildLoc

func (a PtrChildLocsArray) Len() int {
	return len(a)
}

func (a PtrChildLocsArray) Key(idx int) Key {
	return a[idx].Key
}

func (a PtrChildLocsArray) Seq(idx int) Seq {
	return a[idx].Seq
}

func (a PtrChildLocsArray) Loc(idx int) *Loc {
	return &a[idx].Loc
}

func (a PtrChildLocsArray) ChildLoc(idx int) *ChildLoc {
	return a[idx]
}

func (a PtrChildLocsArray) Append(x ChildLoc) ChildLocsAppendable {
	return append(a, &x)
}

// ----------------------------------------

// PtrChildLocsArrayHolder implements PtrChildLocsAppendable interface.
type PtrChildLocsArrayHolder struct {
	a PtrChildLocsArray
}

func (a *PtrChildLocsArrayHolder) Len() int {
	if a == nil || a.a == nil {
		return 0
	}

	return a.a.Len()
}

func (a *PtrChildLocsArrayHolder) ChildLoc(idx int) *ChildLoc {
	if a == nil || a.a == nil {
		return nil
	}

	return a.a.ChildLoc(idx)
}

func (a *PtrChildLocsArrayHolder) Append(il *ChildLoc) {
	if a == nil {
		return
	}

	a.a = append(a.a, il)
}

func (a *PtrChildLocsArrayHolder) Reset(bufManager BufManager) {
	if a == nil || a.a == nil {
		return
	}

	for i, il := range a.a {
		if il.Loc.itemBufRef != nil { // TODO: recycle il.
			il.Loc.itemBufRef.DecRef(bufManager)
			il.Loc.itemBufRef = nil
		}

		if il.Loc.node != nil { // TODO: recycle node.
			il.Loc.node = nil
		}

		a.a[i] = nil
	}

	a.a = a.a[0:0]
}

// ----------------------------------------

// Partitions represent a mapping of PartitionId's to KeyChildLoc's.
type Partitions struct {
	PartitionIds PartitionIds

	// 1-to-1 paired with entries from PartitionIds, so
	// len(KeyChildLocs) == len(PartitionIds).
	KeyChildLocs [][]KeyChildLoc
}

// KeyChildLoc represents a Key associated with a ChildLoc.  We
// can't just use ChildLoc's Key, because a ChildLoc's Key
// represents the minimum key for a ChildLoc.
type KeyChildLoc struct {
	Key      Key // Key can be >= ChildLoc.Key due to partition scope.
	ChildLoc *ChildLoc
}

// ----------------------------------------

// MutationCallback returns true if the mutation should proceed, or
// false if the mutation should be skipped.  In either case, the
// processing of the batch of mutations will continue.  The existing
// may be nil in case there's no previous item.  The isVal is true if
// the existing is for a leaf level ChildLoc (e.g., LocTypeVal).
type MutationCallback func(existing *ChildLoc, isVal bool,
	mutation *Mutation) bool
