package partizen

import (
	"testing"
)

func TestSimpleMemColl(t *testing.T) {
	s, err := StoreOpen(nil, nil)
	if err != nil || s == nil {
		t.Errorf("expected mem StoreOpen() to work, got err: %v", err)
	}
	c, err := s.AddCollection("x", "")
	if err != nil || c == nil {
		t.Errorf("expected AddCollection to work")
	}
	if !s.HasChanges() {
		t.Errorf("expected coll to have changes after AddCollection()")
	}
	seq, val, err := c.Get(0, []byte("a"), true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected Get on empty coll to be empty")
	}
	err = c.Set(0, []byte("a"), 1, []byte("A"))
	if err != nil {
		t.Errorf("expected Set on empty coll to work")
	}
	seq, val, err = c.Get(0, []byte("a"), true)
	if err != nil || seq != 1 || string(val) != "A" {
		t.Errorf("expected Get(a) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get(0, []byte("not-there"), true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected Get on missing key to be empty")
	}
	seq, val, err = c.Get(0xafff, []byte("a"), true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected Get with wrong partition to be empty")
	}
}
