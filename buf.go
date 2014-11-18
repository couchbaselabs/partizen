package partizen

type defaultBufManager struct{}

func (d *defaultBufManager) Alloc(size int) []byte {
	return make([]byte, size)
}

func (d *defaultBufManager) Len(buf []byte) int {
	return len(buf)
}

func (d *defaultBufManager) AddRef(buf []byte) {
	// NOOP.
}

func (d *defaultBufManager) DecRef(buf []byte) bool {
	return true // TODO: Is this right?
}

func (d *defaultBufManager) Visit(buf []byte, from, to int,
	partVisitor func(partBuf []byte), partFrom, partTo int) {
	partVisitor(buf[from:to])
}
