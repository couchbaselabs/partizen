package partizen

import (
	"sync"
)

// A Header is stored at the head (or 0th byte) of the log file.
type Header struct {
	Magic0    uint64
	Magic1    uint64
	UUID      uint64
	Version   [64]byte
	PageSize  uint16
	ExtrasLen uint16
	ExtrasVal []byte
}

// A Footer is the last record appended to the log file whenever
// there's a successful Store.Commit().
type Footer struct {
	StoreDefLoc StoreDefLoc // Location of StoreDef.
	WALTailLoc  WALItemLoc  // Last item of write-ahead-log.

	// Locations of partizen btree root Nodes, 1 per Collection.
	// len(Footer.CollRoots) equals len(StoreDef.CollDefs).
	CollRoots []*CollRoot
}

type StoreDefLoc struct {
	Loc
	storeDef *StoreDef // If nil, runtime representation isn't loaded yet.
}

// A StoreDef defines a partizen Store, holding "slow-changing"
// configuration metadata about a Store.  We keep slow changing
// metadata separate from the Store footer for efficiency, but use
// JSON encoding of the persisted StoreDef for debuggability.
type StoreDef struct {
	CollDefs []*CollDef
}

// A CollDef is persisted as JSON for debuggability.
type CollDef struct {
	Name            string
	CompareFuncName string
	MinFanOut       uint16
	MaxFanOut       uint16 // Usually (2*MinFanOut)+1.
}

// A CollRoot implements the Collection interface.
type CollRoot struct {
	RootKeySeqLoc *KeySeqLoc

	store       *store // Pointer to parent store.
	name        string
	compareFunc CompareFunc
	minFanOut   uint16
	maxFanOut   uint16
	m           sync.Mutex // Protects RootKeySeqLoc.

	// TODO: Need more fields here to track gkvlite-esque ref-counting.
}

type KeySeqLocRef struct {
	refs int64
	ksl  *KeySeqLoc

	// We might own a reference count on another KeySeqLocRef.  When
	// our count drops to 0 and we're free'd, then also release our
	// reference count on the next.
	next *KeySeqLocRef
}

// A Node of a partizen btree has its descendent locations first
// ordered by PartitionID, then secondarily ordered by Key.
type Node interface {
	GetKeySeqLocs() KeySeqLocs

	// --------------------------------

	IsLeaf() bool

	ChildLoc(childLocIdx int) *Loc

	LocateNodePartition(partitionId PartitionId) (
		found bool, nodePartitionIdx int)

	LocateKeySeqIdx(nodePartitionIdx int, key Key) (
		found bool, nodePartitionKeyIdx int, keySeqIdx KeySeqIdx)

	InsertChildLoc(partitionId PartitionId,
		nodePartitionIdx, nodePartitionKeyIdx int, key Key,
		seq Seq, loc Loc) Node

	UpdateChildLoc(partitionId PartitionId,
		nodePartitionIdx, nodePartitionKeyIdx int,
		seq Seq, loc Loc) Node
}

// MAX_CHILD_LOCS_PER_NODE defines the max number for
// Node.NumChildLocs per Node. Although Node.NumChildLocs is a uint8,
// the max fan-out of a Node is 255, not 256, because ChildLoc index
// 0xff is reserved to mark deletions.
const MAX_CHILD_LOCS_PER_NODE = 255        // (2^8)-1.
const MAX_NODE_PARTITIONS_PER_NODE = 65535 // (2^16)-1.

// A KeySeqIdx tracks a single key.
type KeySeqIdx struct {
	Key Key

	// The meaning of this Seq field depends on the ChildLoc's type...
	// If this KeySeqIdx points to a Val (or to a deleted Val), this
	// Seq is for that leaf data item.  If this KeySeqIdx points to a
	// Node, this Seq is the Node's max Seq for a Partition.
	Seq Seq

	// An index into Node.ChildLocs; and, to support Diff(), a
	// ChildLocsIdx of uint8(0xff) means a deleted item.
	Idx uint8
}

// A Loc represents the location of a byte range persisted or
// soon-to-be-persisted to the storage file.  Field sizes are
// carefully chosed to add up to 128 bits.
type Loc struct {
	// Offset is relative to start of file.  Offset of 0 means the
	// pointed-to bytes buf are not persisted yet.
	Offset   uint64
	Size     uint32
	Type     uint8
	Flags    uint8
	CheckSum uint16 // An optional checksum of the bytes buf.

	// Transient; non-nil when the Loc is read into memory or when the
	// bytes of the Loc are prepared for writing.  The len(Loc.buf)
	// should equal Loc.Size.
	buf []byte

	// Transient; only used when Type is LocTypeNode.  If nil, runtime
	// representation hasn't been loaded yet.
	node Node
}

func (l *Loc) Clear() {
	l.Offset = 0
	l.Size = 0
	l.Type = LocTypeUnknown
	l.Flags = 0
	l.CheckSum = 0
	l.buf = nil
	l.node = nil
}

const (
	// Allowed values for Loc.Type field...
	LocTypeUnknown  uint8 = 0x00
	LocTypeNode     uint8 = 0x01
	LocTypeVal      uint8 = 0x02
	LocTypeStoreDef uint8 = 0x03
	LocTypeWALItem  uint8 = 0x04
)

// A KeySeqLoc associates a Key with a (max) Seq and a Loc.  When Loc
// is a node, then Seq will be the max Seq for the entire sub-tree.
type KeySeqLoc struct {
	Key Key
	Seq Seq
	Loc Loc
}

var zeroKeySeqLoc KeySeqLoc

type KeySeqLocs interface {
	Len() int
	Key(idx int) Key
	Seq(idx int) Seq
	Loc(idx int) *Loc
	KeySeqLoc(idx int) *KeySeqLoc
	Append(KeySeqLoc) KeySeqLocs
}

// ----------------------------------------

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

func (a KeySeqLocsArray) Append(x KeySeqLoc) KeySeqLocs {
	return append(a, x)
}

// ----------------------------------------

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

func (a PtrKeySeqLocsArray) Append(x KeySeqLoc) KeySeqLocs {
	return append(a, &x)
}

// ----------------------------------------

// A Mutation represents a mutation request on a key.
type Mutation struct {
	Key Key
	Seq Seq
	Val Val
	Op  MutationOp
}

var zeroMutation Mutation

type MutationOp uint8

const (
	MUTATION_OP_NONE   MutationOp = 0
	MUTATION_OP_UPDATE MutationOp = 1
	MUTATION_OP_DELETE MutationOp = 2

	// FUTURE MutationOp's might include merging, visiting, etc.
)

type WALItemLoc struct {
	Loc
	walItem *WALItem // If nil, runtime representation isn't loaded yet.
}

type WALItem struct {
	Mutation
	Prev WALItemLoc
}
