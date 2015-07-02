package partizen

import (
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

func (dbm *DefaultBufManager) AllocItem(keyLen int, valLen int) ItemBufRef {
	// Layout of buf:
	//   RESERVED_PREFIX(2) + PartitionId(2) +
	//   KeyLen(4) + ValLen(4) + Seq(8) +
	//   KeyBytes + ValBytes.
	// len := 2 + 2 + 4 + 4 + 8 + keyLen + valLen
	// return bufManager.Alloc(len, nil)
	return nil
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
	partVisitor(cbData, buf[from:to], from, to)
	dbm.m.Unlock()

	return dbr
}

// -------------------------------------------------

func (dbr *DefaultBufRef) PartitionId(bm BufManager) PartitionId {
	return 0 // TODO.
}

func (dbr *DefaultBufRef) Key(bm BufManager, out Key) Key {
	return nil // TODO.
}

func (dbr *DefaultBufRef) KeyLen(bm BufManager) int {
	return 0 // TODO.
}

func (dbr *DefaultBufRef) Seq(bm BufManager) Seq {
	return 0
}

func (dbr *DefaultBufRef) Val(bm BufManager, out Val) Val {
	return nil
}

func (dbr *DefaultBufRef) ValLen(bm BufManager) int {
	return 0
}

func (dbr *DefaultBufRef) ValVisit(bm BufManager, from, to int,
	partVisitor func(cbData, partBuf []byte,
		partFrom, partTo int) bool, cbData []byte) {
}
