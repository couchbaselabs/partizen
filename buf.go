package partizen

import (
	"encoding/binary"
	"sync"

	"github.com/couchbaselabs/go-slab"
)

// TODO: A BufManager implementation that leverages chunks.

// DefaultBufManager is an implementation of BufManager interface
// based on the go-slab memory manager.
type DefaultBufManager struct {
	m     sync.Mutex
	arena *slab.Arena
}

// DefaultBufRef is an implementation of the BufRef interface based on
// the go-slab memory manager.
type DefaultBufRef struct {
	slabLoc slab.Loc
}

// -------------------------------------------------

// NewDefaultBufManager returns a BufManager implementation based on
// the go-slab allocator.
func NewDefaultBufManager(
	startChunkSize int,
	slabSize int,
	growthFactor float64,
	malloc func(size int) []byte) *DefaultBufManager {
	arena := slab.NewArena(startChunkSize, slabSize, growthFactor, malloc)
	if arena == nil {
		return nil
	}

	return &DefaultBufManager{arena: arena}
}

// -------------------------------------------------

func (dbm *DefaultBufManager) Alloc(size int,
	partUpdater func(cbData, partBuf []byte,
		partFrom, partTo int) bool, cbData []byte) BufRef {
	dbm.m.Lock()
	slabLoc := dbm.arena.BufToLoc(dbm.arena.Alloc(size))
	dbm.m.Unlock()

	if slabLoc.IsNil() {
		return nil
	}

	dbr := &DefaultBufRef{slabLoc}

	return dbr.update(dbm, 0, size, partUpdater, cbData)
}

// -------------------------------------------------

func (dbr *DefaultBufRef) IsNil() bool {
	if dbr == nil {
		return true
	}

	return dbr.slabLoc.IsNil()
}

func (dbr *DefaultBufRef) Len(bm BufManager) int {
	dbm, ok := bm.(*DefaultBufManager)
	if !ok || dbm == nil {
		return 0
	}

	dbm.m.Lock()
	n := len(dbm.arena.LocToBuf(dbr.slabLoc))
	dbm.m.Unlock()

	return n
}

func (dbr *DefaultBufRef) AddRef(bm BufManager) {
	dbm, ok := bm.(*DefaultBufManager)
	if !ok || dbm == nil {
		return
	}

	dbm.m.Lock()
	dbm.arena.LocAddRef(dbr.slabLoc)
	dbm.m.Unlock()
}

func (dbr *DefaultBufRef) DecRef(bm BufManager) {
	dbm, ok := bm.(*DefaultBufManager)
	if !ok || dbm == nil {
		return
	}

	dbm.m.Lock()
	dbm.arena.LocDecRef(dbr.slabLoc)
	dbm.m.Unlock()
}

func (dbr *DefaultBufRef) Update(bm BufManager, from, to int,
	partUpdater func(cbData, partBuf []byte,
		partFrom, partTo int) bool,
	cbData []byte) BufRef {
	dbm, ok := bm.(*DefaultBufManager)
	if !ok || dbm == nil {
		return nil
	}

	return dbr.update(dbm, from, to, partUpdater, cbData)
}

func (dbr *DefaultBufRef) update(dbm *DefaultBufManager, from, to int,
	partUpdater func(cbData, partBuf []byte,
		partFrom, partTo int) bool,
	cbData []byte) BufRef {
	if partUpdater == nil {
		return dbr
	}

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	partUpdater(cbData, buf[from:to], from, to)
	dbm.m.Unlock()

	return dbr
}

func (dbr *DefaultBufRef) Visit(bm BufManager, from, to int,
	partVisitor func(cbData, partBuf []byte,
		partFrom, partTo int) bool,
	cbData []byte) BufRef {
	if partVisitor == nil {
		return dbr
	}

	dbm, ok := bm.(*DefaultBufManager)
	if !ok || dbm == nil {
		return nil
	}

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	dbm.m.Unlock()

	partVisitor(cbData, buf[from:to], from, to)

	return dbr
}

// -------------------------------------------------

// Layout of item bytes:
//   FUTURE_RESERVED(2) + PartitionId(2) +
//   KeyLen(4) + ValLen(4) + Seq(8) +
//   KeyBytes + ValBytes.
const itemFutureReservedBeg = 0
const itemFutureReservedEnd = 2 // 0 + 2.
const itemPartitionIdBeg = 2
const itemPartitionIdEnd = 4 // 2 + 2.
const itemKeyLenBeg = 4
const itemKeyLenEnd = 8 // 4 + 4.
const itemValLenBeg = 8
const itemValLenEnd = 12 // 8 + 4.
const itemSeqBeg = 12
const itemSeqEnd = 20 // 12 + 8.
const itemHdr = 20

var zeroes8 []byte = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0}
var zeroes4 []byte = zeroes8[0:4]

func (dbm *DefaultBufManager) AllocItem(keyLen int, valLen int) ItemBufRef {
	len := itemHdr + keyLen + valLen

	dbm.m.Lock()
	buf := dbm.arena.Alloc(len)
	dbm.m.Unlock()

	if buf == nil {
		return nil
	}

	// FUTURE_RESERVED + PartitionId.
	copy(buf[itemFutureReservedBeg:itemPartitionIdEnd], zeroes4)

	binary.BigEndian.PutUint32(buf[itemKeyLenBeg:itemKeyLenEnd],
		uint32(keyLen))

	binary.BigEndian.PutUint32(buf[itemValLenBeg:itemValLenEnd],
		uint32(valLen))

	copy(buf[itemSeqBeg:itemSeqEnd], zeroes8) // Seq.

	return &DefaultBufRef{dbm.arena.BufToLoc(buf)}
}

// -------------------------------------------------

func (dbr *DefaultBufRef) PartitionId(bm BufManager) PartitionId {
	dbm := bm.(*DefaultBufManager)

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	dbm.m.Unlock()

	return PartitionId(binary.BigEndian.Uint16(
		buf[itemPartitionIdBeg:itemPartitionIdEnd]))
}

func (dbr *DefaultBufRef) SetPartitionId(bm BufManager,
	partitionId PartitionId) {
	dbm := bm.(*DefaultBufManager)

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	dbm.m.Unlock()

	binary.BigEndian.PutUint16(
		buf[itemPartitionIdBeg:itemPartitionIdEnd],
		uint16(partitionId))
}

func (dbr *DefaultBufRef) KeyLen(bm BufManager) int {
	dbm := bm.(*DefaultBufManager)

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	dbm.m.Unlock()

	return int(binary.BigEndian.Uint32(buf[itemKeyLenBeg:itemKeyLenEnd]))
}

func (dbr *DefaultBufRef) KeyVisit(bm BufManager, from, to int,
	partVisitor func(cbData, partBuf []byte,
		partFrom, partTo int) bool, cbData []byte) {
	dbm := bm.(*DefaultBufManager)

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	dbm.m.Unlock()

	partVisitor(cbData, buf[itemHdr+from:itemHdr+to], from, to)
}

func (dbr *DefaultBufRef) KeyUpdate(bm BufManager, from, to int,
	partUpdater func(cbData, partBuf []byte,
		partFrom, partTo int) bool, cbData []byte) {
	dbm := bm.(*DefaultBufManager)

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	dbm.m.Unlock()

	partUpdater(cbData, buf[itemHdr+from:itemHdr+to], from, to)
}

func (dbr *DefaultBufRef) Seq(bm BufManager) Seq {
	dbm := bm.(*DefaultBufManager)

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	dbm.m.Unlock()

	return Seq(binary.BigEndian.Uint64(buf[itemSeqBeg:itemSeqEnd]))
}

func (dbr *DefaultBufRef) SetSeq(bm BufManager, seq Seq) {
	dbm := bm.(*DefaultBufManager)

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	dbm.m.Unlock()

	binary.BigEndian.PutUint64(buf[itemSeqBeg:itemSeqEnd], uint64(seq))
}

func (dbr *DefaultBufRef) ValLen(bm BufManager) int {
	dbm := bm.(*DefaultBufManager)

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	dbm.m.Unlock()

	return int(binary.BigEndian.Uint32(buf[itemValLenBeg:itemValLenEnd]))
}

func (dbr *DefaultBufRef) ValVisit(bm BufManager, from, to int,
	partVisitor func(cbData, partBuf []byte,
		partFrom, partTo int) bool, cbData []byte) {
	dbm := bm.(*DefaultBufManager)

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	dbm.m.Unlock()

	keyLen := int(binary.BigEndian.Uint32(buf[itemKeyLenBeg:itemKeyLenEnd]))

	beg := itemHdr + keyLen

	partVisitor(cbData, buf[beg+from:beg+to], from, to)
}

func (dbr *DefaultBufRef) ValUpdate(bm BufManager, from, to int,
	partUpdater func(cbData, partBuf []byte,
		partFrom, partTo int) bool, cbData []byte) {
	dbm := bm.(*DefaultBufManager)

	dbm.m.Lock()
	buf := dbm.arena.LocToBuf(dbr.slabLoc)
	dbm.m.Unlock()

	keyLen := int(binary.BigEndian.Uint32(buf[itemKeyLenBeg:itemKeyLenEnd]))

	beg := itemHdr + keyLen

	partUpdater(cbData, buf[beg+from:beg+to], from, to)
}
