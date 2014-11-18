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
		CollRootLocs: make([]NodeLoc, 0),
	}
	footer.StoreDefLoc.Type = LocTypeStoreDef
	footer.StoreDefLoc.storeDef = &StoreDef{
		collDefsByName: make(map[string]*CollDef),
	}
	if f == nil { // Memory only case.
		return footer, nil
	}
	// TODO: Actually scan and read the footer from f, and initialize changes.
	// TODO: Read WALEntry log and apply to footer.
	return footer, nil
}

func (f *Footer) getStoreDef() (*StoreDef, error) {
	if f.StoreDefLoc.storeDef == nil {
		return nil, fmt.Errorf("unimplemented")
	}
	return f.StoreDefLoc.storeDef, nil
}

func (s *store) CollectionNames() ([]string, error) {
	s.m.Lock()
	defer s.m.Unlock()

	storeDef, err := s.changes.getStoreDef()
	if err != nil {
		return nil, err
	}
	rv := make([]string, 0, len(storeDef.CollDefs))
	for _, coll := range storeDef.CollDefs {
		rv = append(rv, coll.Name)
	}
	return rv, nil
}

func (s *store) GetCollection(collName string) (Collection, error) {
	s.m.Lock()
	defer s.m.Unlock()

	storeDef, err := s.changes.getStoreDef()
	if err != nil {
		return nil, err
	}
	collDef, exists := storeDef.collDefsByName[collName]
	if !exists || collDef == nil {
		return nil, fmt.Errorf("no collection, collName: %s", collName)
	}
	return collDef, nil
}

func (s *store) AddCollection(collName string, compareFuncName string) (Collection, error) {
	s.m.Lock()
	defer s.m.Unlock()

	storeDef, err := s.changes.getStoreDef()
	if err != nil {
		return nil, err
	}
	_, exists := storeDef.collDefsByName[collName]
	if exists {
		return nil, fmt.Errorf("collection exists, collName: %s", collName)
	}

	collDef := &CollDef{
		Name:            collName,
		CompareFuncName: compareFuncName,
		s:               s,
	}

	var changes Footer = *s.changes // First, shallow copy.

	changes.StoreDefLoc = StoreDefLoc{storeDef: storeDef.Copy()}
	changes.StoreDefLoc.Type = LocTypeStoreDef

	changes.StoreDefLoc.storeDef.CollDefs =
		append(changes.StoreDefLoc.storeDef.CollDefs, collDef)
	changes.StoreDefLoc.storeDef.collDefsByName[collName] = collDef
	changes.CollRootLocs = append(changes.CollRootLocs, NodeLoc{})

	s.changes = &changes

	return collDef, nil
}

func (s *store) RemoveCollection(collName string) error {
	return fmt.Errorf("unimplemented")
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

func (sd *StoreDef) Copy() *StoreDef {
	rv := &StoreDef{}
	rv.CollDefs = append(rv.CollDefs, sd.CollDefs...)
	rv.collDefsByName = make(map[string]*CollDef)
	for collName, collDef := range sd.collDefsByName {
		rv.collDefsByName[collName] = collDef
	}
	return rv
}

