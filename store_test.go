package partizen

import (
	"reflect"
	"testing"
)

func TestMemStoreOpen(t *testing.T) {
	s, err := StoreOpen(nil, nil)
	if err != nil || s == nil {
		t.Errorf("expected mem StoreOpen() to work, got err: %v", err)
	}
	names, err := s.CollectionNames(nil)
	if err != nil || len(names) != 0 {
		t.Errorf("expected empty CollectionNames()")
	}
	coll, err := s.GetCollection("not-there")
	if err == nil || coll != nil {
		t.Errorf("expected not-there collection to not be there")
	}
	err = s.RemoveCollection("not-there")
	if err == nil {
		t.Errorf("expected not-there collection to not be there")
	}
	coll, err = s.AddCollection("x", "")
	if err != nil || coll == nil {
		t.Errorf("expected AddCollection to work")
	}
	names, err = s.CollectionNames(nil)
	if err != nil || len(names) != 1 {
		t.Errorf("expected non-empty CollectionNames()")
	}
	if !reflect.DeepEqual(names, []string{"x"}) {
		t.Errorf("expected x collection name")
	}
	coll2, err := s.AddCollection("x", "")
	if err == nil || coll2 != nil {
		t.Errorf("expected re-AddCollection to error")
	}
	err = s.RemoveCollection("x")
	if err != nil {
		t.Errorf("expected RemoveCollection to work")
	}
	names, err = s.CollectionNames(nil)
	if err != nil || len(names) != 0 {
		t.Errorf("expected empty CollectionNames()")
	}
	if !reflect.DeepEqual(names, []string(nil)) {
		t.Errorf("expected no collection name")
	}
	coll, err = s.GetCollection("x")
	if err == nil || coll != nil {
		t.Errorf("expected x collection to not be there")
	}
	err = s.RemoveCollection("x")
	if err == nil {
		t.Errorf("expected 2nd RemoveCollection to fail")
	}
}
