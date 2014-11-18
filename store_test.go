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
	if s.HasChanges() {
		t.Errorf("expected new coll to have no changes")
	}
	coll, err = s.AddCollection("x", "")
	if err != nil || coll == nil {
		t.Errorf("expected AddCollection to work")
	}
	if !s.HasChanges() {
		t.Errorf("expected coll to have changes after AddCollection()")
	}
	coll2, err := s.AddCollection("x", "")
	if err == nil || coll2 != nil {
		t.Errorf("expected re-AddCollection to error")
	}
}
