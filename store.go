package partizen

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
)

const HEADER_MAGIC0 = 0xea45113d
const HEADER_MAGIC1 = 0xc03c1b04
const HEADER_VERSION = "0.0.0"

const maxUint64 = uint64(0xffffffffffffffff)

// A store implements the Store interface.
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

func storeOpen(storeFile StoreFile, storeOptions *StoreOptions) (
	Store, error) {
	storeOptions = initStoreOptions(storeOptions)
	header, err := readHeader(storeFile, storeOptions)
	if err != nil {
		return nil, err
	}
	footer, err := readFooter(storeFile, storeOptions, header, maxUint64)
	if err != nil {
		return nil, err
	}
	return &store{
		storeFile:    storeFile,
		storeOptions: *storeOptions,
		header:       header,
		footer:       footer,
		changes:      footer,
	}, nil
}

func initStoreOptions(o *StoreOptions) *StoreOptions {
	if o == nil {
		o = &defaultOptions
	}
	rv := &StoreOptions{
		CompareFuncs: o.CompareFuncs,
		BufManager:   o.BufManager,
	}
	if rv.CompareFuncs == nil {
		rv.CompareFuncs = defaultOptions.CompareFuncs
	}
	if rv.CompareFuncs[""] == nil {
		rv.CompareFuncs[""] = defaultOptions.CompareFuncs[""]
	}
	if rv.BufManager == nil {
		rv.BufManager = defaultOptions.BufManager
	}
	return rv
}

var defaultOptions = StoreOptions{
	CompareFuncs: map[string]CompareFunc{"": bytes.Compare},
	BufManager:   &defaultBufManager{},
}

func readHeader(f StoreFile, o *StoreOptions) (*Header, error) {
	header := &Header{
		Magic0:   uint64(HEADER_MAGIC0),
		Magic1:   uint64(HEADER_MAGIC1),
		UUID:     uint64(rand.Int63()),
		PageSize: 4096,
	}
	copy(header.Version[:], []byte(HEADER_VERSION+"\x00"))
	if f == nil { // Memory only case.
		return header, nil
	}
	// TODO: Actually read the header from f.
	return nil, fmt.Errorf("unimplemented")
}

func readFooter(f StoreFile, o *StoreOptions, header *Header,
	startOffset uint64) (*Footer, error) {
	footer := &Footer{
		Magic0:       header.Magic0,
		Magic1:       header.Magic1,
		UUID:         header.UUID,
		CollRootLocs: make([]*RootLoc, 0),
	}
	footer.StoreDefLoc.Type = LocTypeStoreDef
	footer.StoreDefLoc.storeDef = &StoreDef{}
	if f == nil { // Memory only case.
		return footer, nil
	}
	// TODO: Actually scan and read the footer from f, and initialize changes.
	// TODO: Read WALEntry log and apply to footer.
	return footer, nil
}

func (s *store) CollectionNames() ([]string, error) {
	s.m.Lock()
	defer s.m.Unlock()

	storeDef := s.changes.StoreDefLoc.storeDef
	rv := make([]string, 0, len(storeDef.CollDefs))
	for _, coll := range storeDef.CollDefs {
		rv = append(rv, coll.Name)
	}
	return rv, nil
}

func (s *store) GetCollection(collName string) (Collection, error) {
	s.m.Lock()
	defer s.m.Unlock()

	for i, collDef := range s.changes.StoreDefLoc.storeDef.CollDefs {
		if collDef.Name == collName {
			return s.changes.CollRootLocs[i], nil
		}
	}
	return nil, fmt.Errorf("no collection, collName: %s", collName)
}

func (s *store) AddCollection(collName string, compareFuncName string) (Collection, error) {
	s.m.Lock()
	defer s.m.Unlock()

	compareFunc, exists := s.storeOptions.CompareFuncs[compareFuncName]
	if !exists || compareFunc == nil {
		return nil, fmt.Errorf("no compareFunc, compareFuncName: %s", compareFuncName)
	}
	for _, collDef := range s.changes.StoreDefLoc.storeDef.CollDefs {
		if collDef.Name == collName {
			return nil, fmt.Errorf("collection exists, collName: %s", collName)
		}
	}

	c := &CollDef{
		Name:            collName,
		CompareFuncName: compareFuncName,
	}
	r := &RootLoc{
		store:       s,
		name:        collName,
		compareFunc: compareFunc,
	}

	var changes = s.changes.startChanges()

	changes.StoreDefLoc.storeDef.CollDefs =
		append(changes.StoreDefLoc.storeDef.CollDefs, c)
	changes.CollRootLocs =
		append(changes.CollRootLocs, r)

	s.changes = changes

	return r, nil
}

func (s *store) RemoveCollection(collName string) error {
	s.m.Lock()
	defer s.m.Unlock()

	for i, collDef := range s.changes.StoreDefLoc.storeDef.CollDefs {
		if collDef.Name == collName {
			var changes = s.changes.startChanges()

			a := changes.StoreDefLoc.storeDef.CollDefs
			copy(a[i:], a[i+1:])
			a[len(a)-1] = nil
			changes.StoreDefLoc.storeDef.CollDefs = a[:len(a)-1]

			b := changes.CollRootLocs
			copy(b[i:], b[i+1:])
			b[len(b)-1] = nil
			changes.CollRootLocs = b[:len(b)-1]

			s.changes = changes

			return nil
		}
	}
	return fmt.Errorf("unknown collection, collName: %s", collName)
}

func (s *store) HasChanges() bool {
	return s.changes != s.footer
}

func (s *store) CommitChanges(cs *ChangeStats) error {
	return fmt.Errorf("unimplemented")
}

func (s *store) AbortChanges(cs *ChangeStats) error {
	return fmt.Errorf("unimplemented")
}

func (s *store) Snapshot() (Store, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (s *store) SnapshotPreviousCommit(numCommitsBack int) (Store, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (s *store) CopyTo(StoreFile, keepCommitsTo interface{}) error {
	return fmt.Errorf("unimplemented")
}

func (s *store) Stats(dest *StoreStats) error {
	return fmt.Errorf("unimplemented")
}

// --------------------------------------------

func (f *Footer) startChanges() *Footer {
	var c Footer = *f // First, shallow copy.

	c.StoreDefLoc.storeDef =
		&StoreDef{CollDefs: append([]*CollDef(nil), c.StoreDefLoc.storeDef.CollDefs...)}

	c.CollRootLocs = append([]*RootLoc(nil), c.CollRootLocs...)

	return &c
}
