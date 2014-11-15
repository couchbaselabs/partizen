package partizen

import (
	"bytes"
	"math/rand"
)

const HEADER_MAGIC = 0xea45113d
const HEADER_VERSION = "0.0.0"

type store struct {
	storeFile    StoreFile
	storeOptions StoreOptions
	header       *Header
	footer       *Footer

	dirtyStoreDef *StoreDef
}

func storeOpen(storeFile StoreFile, storeOptions StoreOptions) (Store, error) {
	storeOptions = initStoreOptions(storeOptions)
	header, err := readHeader(storeFile, &storeOptions)
	if err != nil {
		return nil, err
	}
	footer, err := readFooter(storeFile, &storeOptions, header, 0)
	if err != nil {
		return nil, err
	}
	return &store{
		storeFile:    storeFile,
		storeOptions: storeOptions,
		header:       header,
		footer:       footer,
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
		Magic:      uint64(HEADER_MAGIC),
		UUID:       uint64(rand.Int63()),
		VersionLen: uint32(len(HEADER_VERSION)),
		VersionVal: []byte(HEADER_VERSION),
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
		Magic:                  header.Magic,
		UUID:                   header.UUID,
		StoreDefLoc:            Loc{Type: LocTypeStoreDef},
		CollectionRootNodeLocs: make([]Loc, 0),
	}
	// TODO: Actually scan and read the footer from f.
	return footer, nil
}

func (s *store) CollectionNames() ([]string, error) {
	return nil, nil
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
