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
	WALTailLoc  WALEntryLoc // Last entry of write-ahead-log.

	// Locations of partizen btree root Nodes, 1 per Collection.
	// len(Footer.CollRootLocs) equals len(StoreDef.CollDefs).
	CollRootLocs []*RootLoc
}

// A StoreDef defines a partizen Store, holding "slow-changing"
// configuration metadata about a Store.  We keep slow changing
// metadata separate from the Store footer for efficiency, but use
// JSON encoding of the persisted StoreDef for debuggability.
type StoreDef struct {
	CollDefs []*CollDef
}

type StoreDefLoc struct {
	Loc

	storeDef *StoreDef // If nil, runtime representation isn't loaded yet.
}

// A CollDef is persisted as JSON for debuggability.
type CollDef struct {
	Name            string
	CompareFuncName string
	MinDegree       uint16
	MaxDegree       uint16 // Usually (2*MinDegree)+1.
}

// A RootLoc implements the Collection interface.
type RootLoc struct {
	Loc

	store       *store // Pointer to parent store.
	name        string
	compareFunc CompareFunc
	minDegree   uint16
	maxDegree   uint16

	m sync.Mutex // Protects writes to the Loc fields.

	// TODO: Need a separate RootLocRef for gkvlite-esque ref-counting.
}

// A Node of a partizen btree has its descendent locations first
// ordered by PartitionID, then secondarily ordered by Key.
type Node interface {
	NumChildren() int

	IsLeaf() bool

	ChildLoc(childLocIdx int) *Loc

	GetKeyLocs() []*KeyLoc

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
	LocTypeWALEntry uint8 = 0x04
)

// A KeyLoc associates a Key with a Loc.
type KeyLoc struct {
	Key Key
	Loc Loc
}

var zeroKeyLoc KeyLoc

// A Mutation represents a mutation request on a key.
type Mutation struct {
	Key []byte
	Val []byte
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

type WALEntry struct {
	// TODO: some mutation info here.
	Prev WALEntryLoc
}

type WALEntryLoc struct {
	Loc

	walEntry *WALEntry // If nil, runtime representation isn't loaded yet.
}
