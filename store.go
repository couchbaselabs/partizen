package partizen

import (
	"bytes"
	"fmt"
	"math/rand"
)

func storeOpen(storeFile StoreFile, storeOptions *StoreOptions) (
	Store, error) {
	storeOptions = initStoreOptions(storeOptions)
	header, err := readHeader(storeFile, storeOptions)
	if err != nil {
		return nil, err
	}
	footer, err := readFooter(storeFile, storeOptions, header, MAX_UINT64)
	if err != nil {
		return nil, err
	}
	bufManager := storeOptions.BufManager
	if bufManager == nil {
		bufManager = NewDefaultBufManager(32, 1024*1024, 1.25, nil)
	}
	return &store{
		storeFile:    storeFile,
		storeOptions: *storeOptions,
		bufManager:   bufManager,
		readOnly:     false,
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
	BufManager:       nil,
}

func readHeader(f StoreFile, o *StoreOptions) (*Header, error) {
	header := &Header{
		Magic0: uint64(HEADER_MAGIC0),
		Magic1: uint64(HEADER_MAGIC1),
		UUID:   uint64(rand.Int63()),
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
		Collections: make([]*collection, 0),
	}
	footer.StoreDefLoc.Type = LocTypeStoreDef
	footer.StoreDefLoc.storeDef = &StoreDef{}
	if f == nil { // Memory only case.
		return footer, nil
	}
	// TODO: Actually scan and read the footer from f.
	// TODO: Footer should have copies of magic & uuid bytes for double-check.
	return footer, nil
}

func (s *store) Close() error {
	// TODO: change all collections read-only?
	// TODO: change to read-only?
	// TODO: close storeFile?
	// TODO: zero the store?
	return nil
}

func (s *store) CollectionNames(rv []string) ([]string, error) {
	s.m.Lock()
	storeDef := s.footer.StoreDefLoc.storeDef
	for _, coll := range storeDef.CollectionDefs {
		rv = append(rv, coll.Name)
	}
	s.m.Unlock()

	return rv, nil
}

func (s *store) GetCollection(collName string) (Collection, error) {
	s.m.Lock()
	for i, collDef := range s.footer.StoreDefLoc.storeDef.CollectionDefs {
		if collDef.Name == collName {
			c := s.footer.Collections[i].addRefUnlocked()
			s.m.Unlock()
			return c, nil
		}
	}
	s.m.Unlock()

	return nil, ErrCollectionUnknown
}

func (s *store) AddCollection(collName string, compareFuncName string) (
	Collection, error) {
	if s.readOnly {
		return nil, ErrReadOnly
	}

	compareFunc, exists := s.storeOptions.CompareFuncs[compareFuncName]
	if !exists || compareFunc == nil {
		return nil, ErrNoCompareFunc
	}

	s.m.Lock()

	for _, collDef := range s.footer.StoreDefLoc.storeDef.CollectionDefs {
		if collDef.Name == collName {
			s.m.Unlock()

			return nil, ErrCollectionExists
		}
	}

	c := &CollectionDef{
		Name:            collName,
		CompareFuncName: compareFuncName,
		MinFanOut:       s.storeOptions.DefaultMinFanOut,
		MaxFanOut:       s.storeOptions.DefaultMaxFanOut,
	}
	r := &collection{
		refs:        1,
		store:       s,
		name:        c.Name,
		compareFunc: compareFunc,
		minFanOut:   c.MinFanOut,
		maxFanOut:   c.MaxFanOut,
	}

	s.footer.StoreDefLoc.storeDef.CollectionDefs =
		append(s.footer.StoreDefLoc.storeDef.CollectionDefs, c)
	s.footer.Collections =
		append(s.footer.Collections, r)

	// TODO: Perhaps sort the above arrays, but need to carefully
	// ensure positions match across the two arrays.

	rv := r.addRefUnlocked()

	s.m.Unlock()

	return rv, nil
}

func (s *store) RemoveCollection(collName string) error {
	if s.readOnly {
		return ErrReadOnly
	}

	s.m.Lock()

	for i, collDef := range s.footer.StoreDefLoc.storeDef.CollectionDefs {
		if collDef.Name == collName {
			c := s.footer.Collections[i]

			// TODO: Keep CollectionDefs sorted.
			a := s.footer.StoreDefLoc.storeDef.CollectionDefs
			copy(a[i:], a[i+1:])
			a[len(a)-1] = nil
			s.footer.StoreDefLoc.storeDef.CollectionDefs = a[:len(a)-1]

			// TODO: Keep Collections sorted.
			b := s.footer.Collections
			copy(b[i:], b[i+1:])
			b[len(b)-1] = nil
			s.footer.Collections = b[:len(b)-1]

			c.decRefUnlocked()

			s.m.Unlock()

			return nil
		}
	}

	s.m.Unlock()

	return ErrCollectionUnknown
}

func (s *store) BufManager() BufManager {
	return s.bufManager
}
