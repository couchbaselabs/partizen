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
	m      sync.Mutex
	footer *Footer
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
	DefaultMinFanOut: 15, // TODO: Better DefaultMinFanOut.
	DefaultMaxFanOut: 32, // TODO: Better DefaultMaxFanOut.
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
	// TODO: Actually scan and read the footer from f.
	// TODO: Footer should have copies of magic & uuid bytes for double-check.
	// TODO: Read WALEntry log and apply to footer.
	return footer, nil
}

func (s *store) Close() error {
	return nil // TODO.
}

func (s *store) CollectionNames(rv []string) ([]string, error) {
	s.m.Lock()
	storeDef := s.footer.StoreDefLoc.storeDef
	for _, coll := range storeDef.CollDefs {
		rv = append(rv, coll.Name)
	}
	s.m.Unlock()
	return rv, nil
}

func (s *store) GetCollection(collName string) (Collection, error) {
	s.m.Lock()
	defer s.m.Unlock()

	for i, collDef := range s.footer.StoreDefLoc.storeDef.CollDefs {
		if collDef.Name == collName {
			return s.footer.CollRoots[i].addRefUnlocked(), nil
		}
	}

	return nil, ErrUnknownCollection
}

func (s *store) AddCollection(collName string, compareFuncName string) (
	Collection, error) {
	compareFunc, exists := s.storeOptions.CompareFuncs[compareFuncName]
	if !exists || compareFunc == nil {
		return nil, ErrNoCompareFunc
	}

	s.m.Lock()
	defer s.m.Unlock()

	for _, collDef := range s.footer.StoreDefLoc.storeDef.CollDefs {
		if collDef.Name == collName {
			return nil, ErrCollectionExists
		}
	}

	c := &CollDef{
		Name:            collName,
		CompareFuncName: compareFuncName,
		MinFanOut:       s.storeOptions.DefaultMinFanOut,
		MaxFanOut:       s.storeOptions.DefaultMaxFanOut,
	}
	r := &CollRoot{
		refs:        1,
		store:       s,
		name:        c.Name,
		compareFunc: compareFunc,
		minFanOut:   c.MinFanOut,
		maxFanOut:   c.MaxFanOut,
	}

	s.footer.StoreDefLoc.storeDef.CollDefs =
		append(s.footer.StoreDefLoc.storeDef.CollDefs, c)
	s.footer.CollRoots =
		append(s.footer.CollRoots, r)

	// TODO: Sort the above arrays, but need to carefully ensure that
	// positions match, perhaps by inserting at the right index.

	return r.addRefUnlocked(), nil
}

func (s *store) RemoveCollection(collName string) error {
	s.m.Lock()
	defer s.m.Unlock()

	for i, collDef := range s.footer.StoreDefLoc.storeDef.CollDefs {
		if collDef.Name == collName {
			c := s.footer.CollRoots[i]

			// TODO: Keep CollDefs sorted.
			a := s.footer.StoreDefLoc.storeDef.CollDefs
			copy(a[i:], a[i+1:])
			a[len(a)-1] = nil
			s.footer.StoreDefLoc.storeDef.CollDefs = a[:len(a)-1]

			// TODO: Keep CollRoots sorted.
			b := s.footer.CollRoots
			copy(b[i:], b[i+1:])
			b[len(b)-1] = nil
			s.footer.CollRoots = b[:len(b)-1]

			c.decRefUnlocked()
			return nil
		}
	}

	return ErrUnknownCollection
}
