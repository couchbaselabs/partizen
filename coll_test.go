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
	seq, val, err := c.Get(0, []byte("a"), NO_MATCH_SEQ, true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected Get on empty coll to be empty")
	}
	err = c.Set(0, []byte("a"), NO_MATCH_SEQ, 1, []byte("A"))
	if err != nil {
		t.Errorf("expected Set on empty coll to work")
	}
	seq, val, err = c.Get(0, []byte("a"), NO_MATCH_SEQ, true)
	if err != nil || seq != 1 || string(val) != "A" {
		t.Errorf("expected Get(a) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get(0, []byte("not-there"), NO_MATCH_SEQ, true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected Get on missing key to be empty")
	}
	err = c.Set(0, []byte("b"), NO_MATCH_SEQ, 2, []byte("B"))
	if err != nil {
		t.Errorf("expected Set on 1 item coll to work")
	}
	seq, val, err = c.Get(0, []byte("a"), NO_MATCH_SEQ, true)
	if err != nil || seq != 1 || string(val) != "A" {
		t.Errorf("expected Get(a) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get(0, []byte("b"), NO_MATCH_SEQ, true)
	if err != nil || seq != 2 || string(val) != "B" {
		t.Errorf("expected Get(b) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get(0, []byte("not-there"), NO_MATCH_SEQ, true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected Get on missing key to be empty")
	}
	err = c.Set(0, []byte("0"), NO_MATCH_SEQ, 3, []byte("00"))
	if err != nil {
		t.Errorf("expected Set on 2 item coll to work")
	}
	seq, val, err = c.Get(0, []byte("a"), NO_MATCH_SEQ, true)
	if err != nil || seq != 1 || string(val) != "A" {
		t.Errorf("expected Get(a) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get(0, []byte("b"), NO_MATCH_SEQ, true)
	if err != nil || seq != 2 || string(val) != "B" {
		t.Errorf("expected Get(b) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get(0, []byte("0"), NO_MATCH_SEQ, true)
	if err != nil || seq != 3 || string(val) != "00" {
		t.Errorf("expected Get(0) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get(0, []byte("not-there"), NO_MATCH_SEQ, true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected Get on missing key to be empty")
	}
}
