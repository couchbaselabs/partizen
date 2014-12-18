package partizen

import (
	"sync"
)

// A Header is stored at the head (or 0th byte) of the log file.
type Header struct {
	Magic0    uint64
	Magic1    uint64
	UUID      uint64
	Version   [48]byte
	PageSize  uint32
	ExtrasLen uint32
	ExtrasVal []byte
}

// A Footer is the last record appended to the log file whenever
// there's a successful Store.Commit().
type Footer struct {
	// Magic0 uint64 // Same as Header.Magic0.
	// Magic1 uint64 // Same as Header.Magic1.
	// UUID   uint64 // Same as Header.UUID.

	// Size in bytes of the entire written Footer and its related
	// atomic tree data, including the Footer's MagicX, UUID & Size
	// fields).  We write Val's before the Footer and copy-on-write
	// tree data after the Footer bytes to try to fill up an entire
	// underlying storage page.
	//
	// TODO: Is that a safe/good idea?
	// Size uint32

	StoreDefLoc StoreDefLoc // Location of StoreDef.
	WALTailLoc  WALEntryLoc // Last entry of write-ahead-log.

	// Locations of partizen btree root Nodes, 1 per Collection.  The
	// length of CollRootLocs equals len(StoreDef.CollDefs).
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

	storeDef *StoreDef // If nil, runtime representation hasn't been loaded.
}

// A CollDef is persisted as JSON for debuggability.
type CollDef struct {
	Name            string
	CompareFuncName string
}

// A RootLoc implements the Collection interface.
type RootLoc struct {
	Loc

	store       *store // Pointer to parent store.
	name        string
	compareFunc CompareFunc

	m sync.Mutex // Protects writes to the Loc fields.
}

// A Node of a partizen btree has its descendent locations first
// ordered by PartitionID, then secondarily ordered by Key.
type Node struct {
	NumChildLocs  uint8
	NumKeys       uint8
	NumPartitions uint16

	// ChildLocs are kept separate from Partitions because multiple
	// NodePartition.KeySeq's may be sharing or indexing to the same
	// ChildLoc entries.
	//
	// TODO: Consider ordering ChildLocs by ChildLoc.Offset?
	ChildLocs []Loc // See MAX_CHILD_LOCS_PER_NODE.

	KeySeqs []Seq

	// The PartitionIdxs and Partitions arrays have length of
	// NumPartitions and are both ordered by PartitionID.  For example
	// PartitionIdxs[4] and Partitions[4] are both about
	// PartitionIdxs[4].PartitionID.
	PartitionIdxs []NodePartitionIdx
	Partitions    []NodePartition
}

// MAX_CHILD_LOCS_PER_NODE defines the max number for
// Node.NumChildLocs per Node. Although Node.NumChildLocs is a uint8,
// the max fan-out of a Node is 255, not 256, because ChildLoc index
// 0xff is reserved to mark deletions.
const MAX_CHILD_LOCS_PER_NODE = 255

// A NodePartitionIdx is a fixed-sized struct to allow fast lookup of
// a PartitionId in a Node (see Node.PartitionIdxs array).
type NodePartitionIdx struct {
	PartitionID PartitionID

	// Offset is the starting byte offset of the corresponding
	// NodePartition entry in the Node.Partitions array, starting from
	// the 0th Node.Partitions[0] byte position.
	Offset uint16
}

// A NodePartition is a variable-sized struct that holds keys of
// direct descendants of a Partition for a Node.
type NodePartition struct {
	TotKeys     uint64 // TotKeys - TotVals equals number of deletions.
	TotVals     uint64
	TotKeyBytes uint64
	TotValBytes uint64

	NumKeySeqs uint8
	KeySeqs    []KeySeq // KeySeqs is ordered by Key.

	// FUTURE: Aggregates might be also maintained here per NodePartition.
}

// A KeySeqIdx is a variable-sized struct that tracks a single key.
type KeySeq struct {
	KeyLen uint16

	// The meaning of this Seq field depends on the ChildLoc's type...
	// If this KeySeqIdx points to a Val (or to a deleted Val), this
	// Seq is for that leaf data item.  If this KeySeqIdx points to a
	// Node, this Seq is the Node's max Seq for a Partition.
	Seq Seq

	// An index into Node.ChildLocs; and, to support ChangesSince(),
	// a ChildLocsIdx of uint8(0xff) means a deleted item.
	ChildLocsIdx uint8

	// The Key goes last as its variable sized.
	Key Key
}

// A Loc represents the location of a byte range persisted or
// soon-to-be-persisted to the storage file.
type Loc struct {
	Type     uint8
	CheckSum uint16
	Size     uint32

	// Offset is relative to start of file.
	// Offset of 0 means not persisted yet.
	Offset uint64

	// Transient; non-nil when the Loc is read into memory
	// or when the bytes of the Loc are prepared for writing.
	buf []byte

	// Only used when Type is LocTypeNode.  If nil, runtime
	// representation hasn't been loaded.
	node *Node
}

func (l *Loc) ClearLoc(t uint8) {
	l.Type = t
	l.CheckSum = 0
	l.Size = 0
	l.Offset = 0
	l.buf = nil
	l.node = nil
}

const (
	// Allowed values for Loc.Type field...
	LocTypeNode     = 0x00
	LocTypeVal      = 0x01
	LocTypeStoreDef = 0x02
)

type WALEntry struct {
	// TODO: some mutation info here.
	Prev WALEntryLoc
}

type WALEntryLoc struct {
	Loc

	walEntry *WALEntry // If nil, runtime representation hasn't been loaded.
}
