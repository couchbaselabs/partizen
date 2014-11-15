package partizen

import (
	"bytes"
)

type store struct {
	storeFile    StoreFile
	storeOptions StoreOptions

	storeDef *StoreDef
	header   *Header
	footer   *Footer
}

func storeOpen(storeFile StoreFile, storeOptions StoreOptions) (Store, error) {
	storeOptions = initStoreOptions(storeOptions)
	header, err := readHeader(storeFile, &storeOptions)
	if err != nil {
		return nil, err
	}
	footer, err := readFooter(storeFile, &storeOptions, header)
	if err != nil {
		return nil, err
	}
	return &store{
		storeFile:    storeFile,
		storeOptions: storeOptions,
		storeDef:     &StoreDef{Collections: make([]*CollectionDef, 0)},
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
	return nil, nil
}

func readFooter(f StoreFile, o *StoreOptions, header *Header) (*Footer, error) {
	return nil, nil
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
