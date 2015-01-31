package partizen

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
	RootKeySeqLocRef *KeySeqLocRef // Mutator must have store.m locked.
	refs             int32         // Mutator must have store.m locked.

	// The following fields are immutable.
	store       *store // Pointer to parent store.
	name        string
	compareFunc CompareFunc
	minFanOut   uint16
	maxFanOut   uint16
	readOnly    bool
}

type KeySeqLocRef struct {
	R *KeySeqLoc // Immutable.

	refs int32 // Mutator must have store.m locked.

	// We might own a reference count on another KeySeqLocRef.  When
	// our count hits 0, also release our refcount on the next.
	next *KeySeqLocRef // Mutator must have store.m locked.
}

// AddRef must be invoked by caller with CollRoot.store.m locked.
func (r *KeySeqLocRef) addRef() (*KeySeqLocRef, *KeySeqLoc) {
	if r == nil {
		return nil, nil
	}
	if r.refs <= 0 {
		panic("KeySeqLocRef.refs addRef saw underflow")
	}
	r.refs++
	return r, r.R
}

// DecRef must be invoked by caller with CollRoot.store.m locked.
func (r *KeySeqLocRef) decRef() *KeySeqLocRef {
	if r == nil {
		return nil
	}
	if r.refs <= 0 {
		panic("KeySeqLocRef.refs defRef saw underflow")
	}
	r.refs--
	if r.refs <= 0 {
		r.next.decRef()
		r.next = nil
		return nil
	}
	return r
}

// A Node of a partizen btree has its descendent locations first
// ordered by PartitionID, then secondarily ordered by Key.
type Node interface {
	GetKeySeqLocs() KeySeqLocs
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

// MutationCallback returns true if the mutation should proceed, or
// false if the mutation should be skipped.  In either case, the
// processing of the batch of mutations will continue.  The existing
// may be nil in case there's no previous item.  The isVal is true if
// the existing is for a leaf level KeySeqLoc (e.g., LocTypeVal).
type MutationCallback func(existing *KeySeqLoc, isVal bool,
	mutation *Mutation) bool

var zeroMutation Mutation

type WALItemLoc struct {
	Loc
	walItem *WALItem // If nil, runtime representation isn't loaded yet.
}

type WALItem struct {
	Mutation
	Prev WALItemLoc
}
