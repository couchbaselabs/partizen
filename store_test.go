package partizen

import (
	"testing"
)

func TestMemStoreOpen(t *testing.T) {
	s, err := StoreOpen(nil, nil)
	if err != nil || s == nil {
		t.Errorf("expected mem StoreOpen() to work, got err: %v", err)
	}
	names, err := s.CollectionNames()
	if err != nil || names == nil || len(names) != 0 {
		t.Errorf("expected empty CollectionNames()")
	}
	coll, err := s.GetCollection("not-there")
	if err == nil || coll != nil {
		t.Errorf("expected not-there collection to not be there")
	}
}
