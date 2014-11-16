package partizen

// A Header is stored at the head (or 0th byte) of the log file.
type Header struct {
	Magic      uint64
	UUID       uint64
	VersionLen uint32
	VersionVal []byte
	ExtrasLen  uint32
	ExtrasVal  []byte
}

// A Footer is the last record appended to the log file whenever
// there's a successful Store.Commit().
type Footer struct {
	Magic       uint64 // Same as Header.Magic.
	UUID        uint64 // Same as Header.UUID.
	StoreDefLoc Loc    // Location of StoreDef.

	// Locations of partizen btree root Nodes, 1 per Collection.  The
	// length of CollectionRootNodes equals len(StoreDef.Collections).
	CollectionRootNodeLocs []Loc

	WALTail Loc // Write-ahead-log.
}

// A StoreDef defines a partizen Store, holding "slow-changing"
// configuration metadata about a Store.  We keep slow changing
// metadata separate from the Store footer for efficiency, but use
// JSON ecoding of the StoreDef for debuggability.
type StoreDef struct {
	Collections []*CollectionDef
}

// A CollectionDef is stored as JSON for debuggability.
type CollectionDef struct {
	Name            string
	CompareFuncName string
}

// A Node of a partizen btree has its descendent locations first
// ordered by PartitionID, then secondarily ordered by Key.
type Node struct {
	// ChildLocs are not ordered (or, at least roughly ordered by
	// append sequence) and are kept separate from the NodePartitions
	// because multiple NodePartition.KeySeq's may be sharing or
	// indexing to the same ChildLoc's.
	//
	// TODO: Consider ordering ChildLocs by ChildLoc.Offset?
	NumChildLocs uint8
	ChildLocs    []Loc // See MAX_CHILD_LOCS_PER_NODE.

	// The PartitionIdxs and Partitions arrays have length of
	// NumPartitions and are both ordered by PartitionID.  For example
	// PartitionIdxs[4] and Partitions[4] are both about
	// PartitionIdxs[4].PartitionID.
	NumPartitions uint16
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
	// the 0th Node.Partition[0] byte position.
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
	Key    Key

	// The meaning of this Seq field depends on the ChildLoc's type...
	// If this KeySeqIdx points to a Val (or to a deleted Val), this
	// Seq is for that leaf data item.  If this KeySeqIdx points to a
	// Node, this Seq is the Node's max Seq for a Partition.
	Seq Seq

	// An index into Node.ChildLocs; and, to support ChangesSince(),
	// a ChildLocsIdx of uint8(0xff) means a deleted item.
	ChildLocsIdx uint8
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
}

const (
	// Allowed values for Loc.Type field...
	LocTypeStoreDef = 0x00
	LocTypeNode     = 0x01
	LocTypeNodeLeaf = 0x03 // 0x01 | 0x02
	LocTypeVal      = 0x04
)
