package partizen

// A BufManager represents the functionality needed for memory
// management.
type BufManager interface {
	// Alloc might return nil if no memory is available.  The optional
	// partUpdater callback, if non-nil, is invoked via
	// BufRef.Update().
	Alloc(size int,
		partUpdater func(partBuf []byte, partFrom, partTo int) bool) BufRef
}

// A BufRef represents a reference to memory that's managed by a
// BufManager.  This extra level of indirection via a flyweight
// pattern allows a BufManager to optionally avoid GC scan traversals
// and other optimizations like chunking.
type BufRef interface {
	IsNil() bool

	Len(bm BufManager) int

	AddRef(bm BufManager)
	DecRef(bm BufManager)

	// A buffer might be implemented as one or more chunks, which can
	// be mutated by Update().  The callback can return false to stop
	// the callbacks.
	Update(bm BufManager, from, to int,
		partUpdater func(partBuf []byte, partFrom, partTo int) bool) BufRef

	// A buffer might be implemented as one or more chunks, which can
	// be visited in read-only fashion by Visit().  The callback can
	// return false to stop the callbacks.
	Visit(bm BufManager, from, to int,
		partVisitor func(partBuf []byte, partFrom, partTo int) bool) BufRef
}
