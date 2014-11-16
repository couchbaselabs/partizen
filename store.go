package partizen

import (
	"bytes"
	"math/rand"
	"sync"
)

const HEADER_MAGIC0 = 0xea45113d
const HEADER_MAGIC1 = 0xc03c1b04
const HEADER_VERSION = "0.0.0"

const maxUint64 = uint64(0xffffffffffffffff)

type store struct {
	// These fields are immutable.
	storeFile    StoreFile
	storeOptions StoreOptions
	header       *Header

	// These fields are mutable, protected by the m lock.
	m       sync.Mutex
	footer  *Footer
	changes *Footer // Unpersisted changes to a store.
}

func storeOpen(storeFile StoreFile, storeOptions StoreOptions) (Store, error) {
	storeOptions = initStoreOptions(storeOptions)
	header, err := readHeader(storeFile, &storeOptions)
	if err != nil {
		return nil, err
	}
	footer, err := readFooter(storeFile, &storeOptions, header, maxUint64)
	if err != nil {
		return nil, err
	}
	return &store{
		storeFile:    storeFile,
		storeOptions: storeOptions,
		header:       header,
		footer:       footer,
		changes:      footer,
	}, nil
}

func initStoreOptions(o StoreOptions) StoreOptions {
	if o.CompareFuncs == nil {
		o.CompareFuncs = defaultOptions.CompareFuncs
	}
	if o.CompareFuncs[""] == nil {
		o.CompareFuncs[""] = defaultOptions.CompareFuncs[""]
	}
	if o.BufAlloc == nil {
		o.BufAlloc = func(size int) []byte { return make([]byte, size) }
	}
	if o.BufAddRef == nil {
		o.BufAddRef = noopBufFunc
	}
	if o.BufDecRef == nil {
		o.BufDecRef = noopBufFunc
	}
	return o
}

func noopBufFunc(buf []byte) {}

var defaultOptions = StoreOptions{
	CompareFuncs: map[string]CompareFunc{
		"": bytes.Compare,
	},
}

func readHeader(f StoreFile, o *StoreOptions) (*Header, error) {
	header := &Header{
		Magic0: uint64(HEADER_MAGIC0),
		Magic1: uint64(HEADER_MAGIC1),
		UUID:   uint64(rand.Int63()),
	}
	if f == nil { // Memory only case.
		return header, nil
	}
	// TODO: Actually read the header from f.
	return header, nil
}

func readFooter(f StoreFile, o *StoreOptions, header *Header,
	startOffset uint64) (*Footer, error) {
	footer := &Footer{
		Magic0:                 header.Magic0,
		Magic1:                 header.Magic1,
		UUID:                   header.UUID,
		StoreDefLoc:            StoreDefLoc{},
		CollectionRootNodeLocs: make([]NodeLoc, 0),
	}
	footer.StoreDefLoc.Type = LocTypeStoreDef

	// TODO: Actually scan and read the footer from f, and initialize changes.
	// TODO: Read WALEntry log and apply to footer.

	return footer, nil
}

func (f *Footer) getStoreDef() (*StoreDef, error) {
	return f.StoreDefLoc.storeDef, nil // TODO.
}

func (s *store) HasChanges() bool {
	return s.changes != s.footer
}

func (s *store) CollectionNames() ([]string, error) {
	storeDef, err := s.changes.getStoreDef()
	if err != nil {
		return nil, err
	}
	rv := make([]string, 0, len(storeDef.Collections))
	for _, coll := range storeDef.Collections {
		rv = append(rv, coll.Name)
	}
	return rv, nil
}

func (s *store) GetCollection(collName string) (Collection, error) {
	return nil, nil
}

func (s *store) AddCollection(collName string, compareFuncName string) (Collection, error) {
	return nil, nil
}

func (s *store) RemoveCollection(collName string) error {
	return nil
}

func (s *store) CommitChanges(cs *ChangeStats) error {
	return nil
}

func (s *store) AbortChanges(cs *ChangeStats) error {
	return nil
}

func (s *store) Snapshot() (Store, error) {
	return nil, nil
}

func (s *store) SnapshotPreviousCommit(numCommitsBack int) (Store, error) {
	return nil, nil
}

func (s *store) CopyTo(StoreFile, keepCommitsTo interface{}) error {
	return nil
}

func (s *store) Stats(dest *StoreStats) error {
	return nil
}
