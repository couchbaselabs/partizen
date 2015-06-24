package partizen

// A BufManager represents the functionality needed for memory
// management.
type BufManager interface {
	// Alloc might return nil if no memory is available.  The optional
	// partUpdater callback, if non-nil, is invoked via
	// BufRef.Update().
	Alloc(size int,
		partUpdater func(cbData, partBuf []byte,
			partFrom, partTo int) bool, cbData []byte) BufRef
}

// A BufRef represents a reference to memory that's managed by a
// BufManager.  This extra level of indirection via a flyweight
// pattern allows a BufManager to optionally avoid GC scan traversals
// and other optimizations like chunking.
type BufRef interface {
	// IsNil returns true if the BufRef is nil, perhaps due to an
	// Alloc() error.
	IsNil() bool

	// Returns the length of the referenced byte slice.
	Len(bm BufManager) int

	// AddRef increases the ref-count on a BufRef.
	AddRef(bm BufManager)

	// DecRef decreases the ref-count on a BufRef.
	DecRef(bm BufManager)

	// A buffer might be implemented as one or more chunks, which can
	// be mutated by Update().  The callback can return false to stop
	// the callbacks.  The cbData parameter is passed through to the
	// callback.
	Update(bm BufManager, from, to int,
		partUpdater func(cbData, partBuf []byte,
			partFrom, partTo int) bool, cbData []byte) BufRef

	// A buffer might be implemented as one or more chunks, which can
	// be visited in read-only fashion by Visit().  The callback can
	// return false to stop the callbacks.  The cbData parameter is
	// passed through to the callback.
	Visit(bm BufManager, from, to int,
		partVisitor func(cbData, partBuf []byte,
			partFrom, partTo int) bool, cbData []byte) BufRef
}

// -------------------------------------------------

// ToBufRef helper function allocates a BufRef and copies bytes from
// the src bytes slice to the returned BufRef.
func ToBufRef(bufManager BufManager, src []byte) BufRef {
	return bufManager.Alloc(len(src), CopyToBufRef, src)
}

// FromBufRef helper function copies the bytes from a BufRef to a
// caller-supplied byte slice, and allocates a new byte slice if dst
// is nil.
func FromBufRef(dst []byte,
	bufRef BufRef, bufManager BufManager) []byte {
	if bufRef == nil || bufRef.IsNil() {
		return dst
	}

	bufLen := bufRef.Len(bufManager)

	if dst == nil {
		dst = make([]byte, bufLen)
	}

	bufRef.Visit(bufManager, 0, len(dst), CopyFromBufRef, dst)

	return dst
}

// -------------------------------------------------

// CopyFromBufRef copies bytes from partBuf to buf.  It's useful as a
// helper function that can be used with BufRef.Visit().
func CopyFromBufRef(buf, partBuf []byte, partFrom, partTo int) bool {
	copy(buf[partFrom:partTo], partBuf)
	return true
}

// CopyToBufRef copies bytes from buf to partBuf.  It's useful as a
// helper function that can be used with BufRef.Update() and
// BufManager.Alloc().
func CopyToBufRef(buf, partBuf []byte, partFrom, partTo int) bool {
	copy(partBuf, buf[partFrom:partTo])
	return true
}
