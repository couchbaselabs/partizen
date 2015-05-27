package partizen

type defaultBufManager struct{}

func (d *defaultBufManager) Alloc(size int) []byte {
	return make([]byte, size)
}

func (d *defaultBufManager) Len(buf []byte) int {
	return len(buf)
}

func (d *defaultBufManager) WantRef(buf []byte) []byte {
	return buf // TODO.
}

func (d *defaultBufManager) DropRef(buf []byte) {
	// TODO.
}

func (d *defaultBufManager) Visit(buf []byte, from, to int,
	partVisitor func(partBuf []byte, partFrom, partTo int)) {
	partVisitor(buf[from:to], from, to)
}
