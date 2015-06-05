package partizen

import (
	"sync"

	"github.com/couchbaselabs/go-slab"
)

type DefaultBufManager struct {
	m     sync.Mutex
	arena *slab.Arena
}

func NewDefaultBufManager(startChunkSize int,
	slabSize int, growthFactor float64,
	malloc func(size int) []byte) *DefaultBufManager {
	arena := slab.NewArena(startChunkSize, slabSize, growthFactor, malloc)
	if arena == nil {
		return nil
	}

	return &DefaultBufManager{arena: arena}
}

func (d *DefaultBufManager) Alloc(size int) []byte {
	d.m.Lock()
	buf := d.arena.Alloc(size)
	d.m.Unlock()

	return buf
}

func (d *DefaultBufManager) WantRef(buf []byte) []byte {
	d.m.Lock()
	d.arena.AddRef(buf)
	d.m.Unlock()

	return buf
}

func (d *DefaultBufManager) DropRef(buf []byte) {
	d.m.Lock()
	d.arena.DecRef(buf)
	d.m.Unlock()
}

func (d *DefaultBufManager) Visit(buf []byte, from, to int,
	partVisitor func(partBuf []byte, partFrom, partTo int)) {
	partVisitor(buf[from:to], from, to)
}

func (d *DefaultBufManager) BufRef(buf []byte) BufRef {
	d.m.Lock()
	slabLoc := d.arena.BufToLoc(buf)
	d.m.Unlock()

	return &defaultBufRef{slabLoc}
}

type defaultBufRef struct {
	slabLoc slab.Loc
}

func (bref *defaultBufRef) IsNil() bool {
	if bref == nil {
		return true
	}

	return bref.slabLoc.IsNil()
}

func (bref *defaultBufRef) Buf(bm BufManager) []byte {
	if bref == nil {
		return nil
	}

	dbm, ok := bm.(*DefaultBufManager)
	if !ok || dbm == nil {
		return nil
	}

	return dbm.arena.LocToBuf(bref.slabLoc)
}
