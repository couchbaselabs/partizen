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
	err = c.Set(0, []byte("a"), CREATE_MATCH_SEQ, 1, []byte("AAAA"))
	if err != ErrMatchSeq {
		t.Errorf("expected ErrMatchSeq during create on existing item")
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
	wrongMatchSeq := Seq(1234321)
	err = c.Set(0, []byte("won't-be-created"), wrongMatchSeq, 400,
		[]byte("wrongMatchSeq"))
	if err != ErrMatchSeq {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	err = c.Set(0, []byte("a"), wrongMatchSeq, 4, []byte("wrongMatchSeq"))
	if err != ErrMatchSeq {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	err = c.Del(0, []byte("a"), wrongMatchSeq, 4)
	if err != ErrMatchSeq {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	err = c.Del(0, []byte("not-there"), wrongMatchSeq, 4)
	if err != ErrMatchSeq {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	seq, val, err = c.Get(0, []byte("not-there"), wrongMatchSeq, true)
	if err != ErrMatchSeq {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	seq, val, err = c.Get(0, []byte("a"), wrongMatchSeq, true)
	if err != ErrMatchSeq {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	seq, val, err = c.Get(0, []byte("a"), NO_MATCH_SEQ, true)
	if err != nil {
		t.Errorf("expected no err")
	}
	err = c.Set(0, []byte("a"), seq, 10, []byte("AA"))
	if err != nil {
		t.Errorf("expected no err on Set with correct seq")
	}
	_, _, err = c.Get(0, []byte("a"), seq, true)
	if err != ErrMatchSeq {
		t.Errorf("expected ErrMatchSeq")
	}
	seq, val, err = c.Get(0, []byte("a"), 10, true)
	if err != nil || string(val) != "AA" {
		t.Errorf("expected no err and AA")
	}
	if seq != 10 {
		t.Errorf("expected seq of 10")
	}
	err = c.Set(0, []byte("x"), CREATE_MATCH_SEQ, 11, []byte("X"))
	if err != nil {
		t.Errorf("expected ok during clean CREATE_MATCH_SEQ")
	}
}
