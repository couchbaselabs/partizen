package partizen

import (
	"strings"
	"testing"
)

func testSimpleCursorKeys(t *testing.T, c Collection, name string,
	startKey, keys string) {
	cur, err := c.Scan([]byte(startKey), true, []PartitionId(nil), true)
	if err != nil {
		t.Errorf(name + ": expected no Scan err")
	}
	if cur == nil {
		t.Errorf(name + ": expected Scan has cur")
	}
	checkCursorKeys(t, name, startKey, cur, keys)
}

func checkCursorKeys(t *testing.T, name, startKey string,
	cur Cursor, keys string) {
	if cur == nil {
		t.Errorf(name + ": expected cur")
	}

	if len(keys) > 0 {
		for i, key := range strings.Split(keys, ",") {
			ok, err := cur.Next()
			if err != nil {
				t.Errorf(name+": startKey: %s, expected no Next error,"+
					" err: %#v, i: %d, key: %s", startKey, err, i, key)
			}
			if !ok {
				t.Errorf(name+": startKey: %s, expected Next ok,"+
					" i: %d, key: %s", startKey, i, key)
			}
			ksl, err := cur.Current()
			if err != nil {
				t.Errorf(name+": startKey: %s, expected no Current error,"+
					" err: %#v, i: %d, key: %s", startKey, err, i, key)
			}
			if ksl == nil {
				t.Errorf(name+": startKey: %s, expected Current ksl,"+
					" err: %#v, i: %d, key: %s", startKey, err, i, key)
			}
			if key != string(ksl.Key) {
				t.Errorf(name+": startKey: %s, expected Current key: %s,"+
					" ksl: %#v, i: %d", startKey, key, ksl, i)
			}
		}
	}

	ok, err := cur.Next()
	if err != nil {
		t.Errorf("expected last Next to have no err, got: %#v", err)
	}
	if ok {
		t.Errorf("expected last Next to be done")
	}
}

func TestSimpleMemOps(t *testing.T) {
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
	partitionId, key, seq, val, err := c.Min(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || key != nil || seq != 0 || val != nil {
		t.Errorf("expected no min")
	}
	partitionId, key, seq, val, err = c.Max(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || key != nil || seq != 0 || val != nil {
		t.Errorf("expected no max")
	}
	testSimpleCursorKeys(t, c, "empty coll", "", "")
	testSimpleCursorKeys(t, c, "empty coll", "a", "")

	// ------------------------------------------------
	err = c.Set(0, []byte("a"), NO_MATCH_SEQ, 1, []byte("A"))
	if err != nil {
		t.Errorf("expected Set on empty coll to work")
	}
	err = c.Set(0, []byte("a"), CREATE_MATCH_SEQ, 1111, []byte("AAAA"))
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
	partitionId, key, seq, val, err = c.Min(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "a" || seq != 1 ||
		string(val) != "A" {
		t.Errorf("expected no min")
	}
	partitionId, key, seq, val, err = c.Max(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "a" || seq != 1 ||
		string(val) != "A" {
		t.Errorf("expected no max")
	}
	testSimpleCursorKeys(t, c, "1 key", "a", "a")
	testSimpleCursorKeys(t, c, "1 key", "0", "a")
	testSimpleCursorKeys(t, c, "1 key", "aa", "")
	testSimpleCursorKeys(t, c, "1 key", "z", "")

	// ------------------------------------------------
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
	partitionId, key, seq, val, err = c.Min(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "a" || seq != 1 || string(val) != "A" {
		t.Errorf("expected min a")
	}
	partitionId, key, seq, val, err = c.Max(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "b" || seq != 2 || string(val) != "B" {
		t.Errorf("expected max b, got key: %s, seq: %d, val: %s", key, seq, val)
	}
	testSimpleCursorKeys(t, c, "2 key", "a", "a,b")
	testSimpleCursorKeys(t, c, "2 key", "0", "a,b")
	testSimpleCursorKeys(t, c, "2 key", "aa", "b")
	testSimpleCursorKeys(t, c, "2 key", "z", "")

	// ------------------------------------------------
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
	partitionId, key, seq, val, err = c.Min(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "0" || seq != 3 ||
		string(val) != "00" {
		t.Errorf("expected min 0, got key: %s, seq: %d, val: %s", key, seq, val)
	}
	partitionId, key, seq, val, err = c.Max(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "b" || seq != 2 ||
		string(val) != "B" {
		t.Errorf("expected max b, got key: %s, seq: %d, val: %s", key, seq, val)
	}
	testSimpleCursorKeys(t, c, "3 key", "a", "a,b")
	testSimpleCursorKeys(t, c, "3 key", "0", "0,a,b")
	testSimpleCursorKeys(t, c, "3 key", "aa", "b")
	testSimpleCursorKeys(t, c, "3 key", "z", "")

	// ------------------------------------------------
	err = c.Set(0, []byte("a"), 1, 10, []byte("AA"))
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
	testSimpleCursorKeys(t, c, "3 key after update", "a", "a,b")
	testSimpleCursorKeys(t, c, "3 key after update", "0", "0,a,b")
	testSimpleCursorKeys(t, c, "3 key after update", "aa", "b")
	testSimpleCursorKeys(t, c, "3 key after update", "z", "")

	// ------------------------------------------------
	err = c.Set(0, []byte("x"), CREATE_MATCH_SEQ, 11, []byte("X"))
	if err != nil {
		t.Errorf("expected ok during clean CREATE_MATCH_SEQ")
	}
	partitionId, key, seq, val, err = c.Min(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "0" || seq != 3 ||
		string(val) != "00" {
		t.Errorf("expected min 0, got key: %s, seq: %d, val: %s", key, seq, val)
	}
	partitionId, key, seq, val, err = c.Max(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "x" || seq != 11 ||
		string(val) != "X" {
		t.Errorf("expected max x, got key: %s, seq: %d, val: %s", key, seq, val)
	}
	testSimpleCursorKeys(t, c, "4 key", "a", "a,b,x")
	testSimpleCursorKeys(t, c, "4 key", "0", "0,a,b,x")
	testSimpleCursorKeys(t, c, "4 key", "aa", "b,x")
	testSimpleCursorKeys(t, c, "4 key", "b", "b,x")
	testSimpleCursorKeys(t, c, "4 key", "c", "x")
	testSimpleCursorKeys(t, c, "4 key", "x", "x")
	testSimpleCursorKeys(t, c, "4 key", "z", "")

	// ------------------------------------------------
	err = c.Del(0, []byte("0"), 3, 20)
	if err != nil {
		t.Errorf("expected ok delete")
	}
	seq, val, err = c.Get(0, []byte("0"), NO_MATCH_SEQ, true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected no 0")
	}
	partitionId, key, seq, val, err = c.Min(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "a" || seq != 10 ||
		string(val) != "AA" {
		t.Errorf("expected min 0, got key: %s, seq: %d, val: %s", key, seq, val)
	}
	partitionId, key, seq, val, err = c.Max(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "x" || seq != 11 ||
		string(val) != "X" {
		t.Errorf("expected max x, got key: %s, seq: %d, val: %s", key, seq, val)
	}
	testSimpleCursorKeys(t, c, "3 key after del 0", "a", "a,b,x")
	testSimpleCursorKeys(t, c, "3 key after del 0", "0", "a,b,x")
	testSimpleCursorKeys(t, c, "3 key after del 0", "aa", "b,x")
	testSimpleCursorKeys(t, c, "3 key after del 0", "b", "b,x")
	testSimpleCursorKeys(t, c, "3 key after del 0", "c", "x")
	testSimpleCursorKeys(t, c, "3 key after del 0", "x", "x")
	testSimpleCursorKeys(t, c, "3 key after del 0", "z", "")
}
