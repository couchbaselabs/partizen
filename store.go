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
		o = &DefaultOptions
	}
	rv := &StoreOptions{
		CompareFuncs:     o.CompareFuncs,
		DefaultPageSize:  o.DefaultPageSize,
		DefaultMinFanOut: o.DefaultMinFanOut,
		DefaultMaxFanOut: o.DefaultMaxFanOut,
		BufManager:       o.BufManager,
	}
	if rv.CompareFuncs == nil {
		rv.CompareFuncs = DefaultOptions.CompareFuncs
	}
	if rv.CompareFuncs[""] == nil {
		rv.CompareFuncs[""] = DefaultOptions.CompareFuncs[""]
	}
	if rv.DefaultPageSize <= 0 {
		rv.DefaultPageSize = DefaultOptions.DefaultPageSize
	}
	if rv.DefaultMinFanOut <= 0 {
		rv.DefaultMinFanOut = DefaultOptions.DefaultMinFanOut
	}
	if rv.DefaultMaxFanOut <= 0 {
		rv.DefaultMaxFanOut = DefaultOptions.DefaultMaxFanOut
	}
	if rv.BufManager == nil {
		rv.BufManager = DefaultOptions.BufManager
	}
	return rv
}

var DefaultOptions = StoreOptions{
	CompareFuncs:     map[string]CompareFunc{"": bytes.Compare},
	DefaultPageSize:  4096,
	DefaultMinFanOut: 2, // TODO: Larger DefaultMinFanOut.
	DefaultMaxFanOut: 5, // TODO: Larger DefaultMaxFanOut.
	BufManager:       &defaultBufManager{},
}

func readHeader(f StoreFile, o *StoreOptions) (*Header, error) {
	header := &Header{
		Magic0:   uint64(HEADER_MAGIC0),
		Magic1:   uint64(HEADER_MAGIC1),
		UUID:     uint64(rand.Int63()),
		PageSize: o.DefaultPageSize,
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
		CollRoots: make([]*CollRoot, 0),
	}
	footer.StoreDefLoc.Type = LocTypeStoreDef
	footer.StoreDefLoc.storeDef = &StoreDef{}
	if f == nil { // Memory only case.
		return footer, nil
	}
	// TODO: Actually scan and read the footer from f, and initialize changes.
	// TODO: Footer should have copies of magic & uuid bytes for double-check.
	// TODO: Read WALEntry log and apply to footer.
	return footer, nil
}

func (s *store) CollectionNames() ([]string, error) {
	s.m.Lock()
	storeDef := s.changes.StoreDefLoc.storeDef
	rv := make([]string, 0, len(storeDef.CollDefs))
	for _, coll := range storeDef.CollDefs {
		rv = append(rv, coll.Name)
	}
	s.m.Unlock()
	return rv, nil
}

func (s *store) GetCollection(collName string) (Collection, error) {
	s.m.Lock()
	defer s.m.Unlock()

	for i, collDef := range s.changes.StoreDefLoc.storeDef.CollDefs {
		if collDef.Name == collName {
			return s.changes.CollRoots[i], nil
		}
	}
	return nil, fmt.Errorf("no collection, collName: %s", collName)
}

func (s *store) AddCollection(collName string, compareFuncName string) (
	Collection, error) {
	compareFunc, exists := s.storeOptions.CompareFuncs[compareFuncName]
	if !exists || compareFunc == nil {
		return nil, fmt.Errorf("no compareFunc, compareFuncName: %s",
			compareFuncName)
	}

	s.m.Lock()
	defer s.m.Unlock()

	for _, collDef := range s.changes.StoreDefLoc.storeDef.CollDefs {
		if collDef.Name == collName {
			return nil, fmt.Errorf("collection exists, collName: %s",
				collName)
		}
	}

	c := &CollDef{
		Name:            collName,
		CompareFuncName: compareFuncName,
		MinFanOut:       s.storeOptions.DefaultMinFanOut,
		MaxFanOut:       s.storeOptions.DefaultMaxFanOut,
	}
	r := &CollRoot{
		store:       s,
		name:        c.Name,
		compareFunc: compareFunc,
		minFanOut:   c.MinFanOut,
		maxFanOut:   c.MaxFanOut,
	}

	var changes = s.startChanges()

	changes.StoreDefLoc.storeDef.CollDefs =
		append(changes.StoreDefLoc.storeDef.CollDefs, c)
	changes.CollRoots =
		append(changes.CollRoots, r)

	// TODO: Sort the above arrays, but need to carefully ensure that
	// positions match, perhaps by inserting at the right index.

	s.changes = changes

	return r, nil
}

func (s *store) RemoveCollection(collName string) error {
	s.m.Lock()
	defer s.m.Unlock()

	for i, collDef := range s.changes.StoreDefLoc.storeDef.CollDefs {
		if collDef.Name == collName {
			var changes = s.startChanges()

			a := changes.StoreDefLoc.storeDef.CollDefs
			copy(a[i:], a[i+1:])
			a[len(a)-1] = nil
			changes.StoreDefLoc.storeDef.CollDefs = a[:len(a)-1]

			b := changes.CollRoots
			copy(b[i:], b[i+1:])
			b[len(b)-1] = nil
			changes.CollRoots = b[:len(b)-1]

			s.changes = changes
			return nil
		}
	}

	return fmt.Errorf("unknown collection, collName: %s", collName)
}

func (s *store) HasChanges() bool {
	s.m.Lock()
	rv := s.changes != s.footer
	s.m.Unlock()
	return rv
}

func (s *store) CommitChanges(cs *ChangeStats) error {
	return fmt.Errorf("unimplemented")
}

func (s *store) AbortChanges(cs *ChangeStats) error {
	return fmt.Errorf("unimplemented")
}

// --------------------------------------------

// startChanges returns a new Footer copy that's ready for
// modifications.  Must be invoked while store.m is locked.
func (s *store) startChanges() *Footer {
	if s.changes != s.footer {
		// We're already changed compared to the footer.
		return s.changes
	}

	var c Footer = *s.changes // Shallow copy.

	c.StoreDefLoc.storeDef = &StoreDef{
		CollDefs: append([]*CollDef(nil),
			s.changes.StoreDefLoc.storeDef.CollDefs...),
	}

	c.CollRoots = append([]*CollRoot(nil), s.changes.CollRoots...)

	s.changes = &c

	return s.changes // TODO: Mem mgmt.
}
