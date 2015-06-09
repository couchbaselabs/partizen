package partizen

import (
	"fmt"
	"strings"
	"testing"
)

func testSimpleCursorKeys(t *testing.T, c Collection, name string,
	ascending bool, startKey, keys string) {
	cur, err := c.Scan([]byte(startKey), ascending, []PartitionId(nil), true)
	if err != nil {
		t.Errorf(name + ": expected no Scan err")
	}
	if cur == nil {
		t.Errorf(name + ": expected Scan has cur")
	}
	checkCursorKeys(t, name, ascending, startKey, cur, keys)
}

func checkCursorKeys(t *testing.T, name string, ascending bool,
	startKey string, cur Cursor, keys string) {
	if cur == nil {
		t.Errorf(name + ": expected cur")
	}

	prefix := fmt.Sprintf("%s: startKey: %s, ascending: %t",
		name, startKey, ascending)

	if len(keys) > 0 {
		for i, key := range strings.Split(keys, ",") {
			partitionId, keyRes, seq, val, err := cur.Next()
			if err != nil {
				t.Errorf(prefix+" expected no Next error,"+
					" err: %#v, i: %d, key: %s", err, i, key)
			}
			if partitionId != 0 || seq == 0 || val == nil {
				t.Errorf(prefix+" expected 0 partitionId, seq, val"+
					" i: %d, key: %s", i, key)
			}
			if keyRes == nil || key != string(keyRes) {
				t.Errorf(prefix+" expected key: %s,"+
					" keyRes: %s, i: %d", key, keyRes, i)
			}
		}
	}

	_, key, _, _, err := cur.Next()
	if err != nil {
		t.Errorf(prefix+" expected last Next to have no err, got: %#v", err)
	}
	if key != nil {
		t.Errorf(prefix+" expected last Next to be done, key: %s", key)
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
	seq, val, err := c.Get([]byte("a"), NO_MATCH_SEQ, true)
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
	seq, val, err = c.Get([]byte("a"), CREATE_MATCH_SEQ, true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected no error for CREATE_MATCH_SEQ get on empty")
	}

	testSimpleCursorKeys(t, c, "empty coll", true, "", "")
	testSimpleCursorKeys(t, c, "empty coll", true, "a", "")

	testSimpleCursorKeys(t, c, "empty coll", false, "", "")
	testSimpleCursorKeys(t, c, "empty coll", false, "a", "")

	// ------------------------------------------------
	err = c.Set(0, []byte("a"), NO_MATCH_SEQ, 1, []byte("A"))
	if err != nil {
		t.Errorf("expected Set on empty coll to work")
	}
	err = c.Set(0, []byte("a"), CREATE_MATCH_SEQ, 1111, []byte("AAAA"))
	if err == nil {
		t.Errorf("expected ErrMatchSeq during create on existing item,"+
			" err: %v", err)
	}
	seq, val, err = c.Get([]byte("a"), NO_MATCH_SEQ, true)
	if err != nil || seq != 1 || string(val) != "A" {
		t.Errorf("expected Get(a) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get([]byte("not-there"), NO_MATCH_SEQ, true)
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
	seq, val, err = c.Get([]byte("a"), CREATE_MATCH_SEQ, true)
	if err != ErrMatchSeq {
		t.Errorf("expected ErrMatchSeq for CREATE_MATCH_SEQ get on real key")
	}
	seq, val, err = c.Get([]byte("a"), 100, true)
	if err != ErrMatchSeq {
		t.Errorf("expected ErrMatchSeq for wrong seq get on real key")
	}
	seq, val, err = c.Get([]byte("x"), CREATE_MATCH_SEQ, true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected no error for CREATE_MATCH_SEQ get on mising key")
	}

	testSimpleCursorKeys(t, c, "1 key", true, "a", "a")
	testSimpleCursorKeys(t, c, "1 key", true, "0", "a")
	testSimpleCursorKeys(t, c, "1 key", true, "aa", "")
	testSimpleCursorKeys(t, c, "1 key", true, "z", "")

	testSimpleCursorKeys(t, c, "1 key", false, "a", "a")
	testSimpleCursorKeys(t, c, "1 key", false, "0", "")
	testSimpleCursorKeys(t, c, "1 key", false, "aa", "a")
	testSimpleCursorKeys(t, c, "1 key", false, "z", "a")

	// ------------------------------------------------
	err = c.Set(0, []byte("b"), NO_MATCH_SEQ, 2, []byte("B"))
	if err != nil {
		t.Errorf("expected Set on 1 item coll to work")
	}
	seq, val, err = c.Get([]byte("a"), NO_MATCH_SEQ, true)
	if err != nil || seq != 1 || string(val) != "A" {
		t.Errorf("expected Get(a) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get([]byte("b"), NO_MATCH_SEQ, true)
	if err != nil || seq != 2 || string(val) != "B" {
		t.Errorf("expected Get(b) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get([]byte("not-there"), NO_MATCH_SEQ, true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected Get on missing key to be empty")
	}
	partitionId, key, seq, val, err = c.Min(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "a" || seq != 1 ||
		string(val) != "A" {
		t.Errorf("expected min a")
	}
	partitionId, key, seq, val, err = c.Max(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "b" || seq != 2 ||
		string(val) != "B" {
		t.Errorf("expected max b, got key: %s, seq: %d, val: %s",
			key, seq, val)
	}

	testSimpleCursorKeys(t, c, "2 key", true, "a", "a,b")
	testSimpleCursorKeys(t, c, "2 key", true, "b", "b")
	testSimpleCursorKeys(t, c, "2 key", true, "0", "a,b")
	testSimpleCursorKeys(t, c, "2 key", true, "aa", "b")
	testSimpleCursorKeys(t, c, "2 key", true, "z", "")

	testSimpleCursorKeys(t, c, "2 key", false, "a", "a")
	testSimpleCursorKeys(t, c, "2 key", false, "b", "b,a")
	testSimpleCursorKeys(t, c, "2 key", false, "0", "")
	testSimpleCursorKeys(t, c, "2 key", false, "aa", "a")
	testSimpleCursorKeys(t, c, "2 key", false, "z", "b,a")

	// ------------------------------------------------
	err = c.Set(0, []byte("0"), NO_MATCH_SEQ, 3, []byte("00"))
	if err != nil {
		t.Errorf("expected Set on 2 item coll to work")
	}
	seq, val, err = c.Get([]byte("a"), NO_MATCH_SEQ, true)
	if err != nil || seq != 1 || string(val) != "A" {
		t.Errorf("expected Get(a) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get([]byte("b"), NO_MATCH_SEQ, true)
	if err != nil || seq != 2 || string(val) != "B" {
		t.Errorf("expected Get(b) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get([]byte("0"), NO_MATCH_SEQ, true)
	if err != nil || seq != 3 || string(val) != "00" {
		t.Errorf("expected Get(0) to work, got seq: %d, val: %s, err: %v",
			seq, val, err)
	}
	seq, val, err = c.Get([]byte("not-there"), NO_MATCH_SEQ, true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected Get on missing key to be empty")
	}
	wrongMatchSeq := Seq(1234321)
	err = c.Set(0, []byte("won't-be-created"), wrongMatchSeq, 400,
		[]byte("wrongMatchSeq"))
	if err == nil {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	err = c.Set(0, []byte("a"), wrongMatchSeq, 4, []byte("wrongMatchSeq"))
	if err == nil {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	err = c.Del(0, []byte("a"), wrongMatchSeq, 4)
	if err == nil {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	err = c.Del(0, []byte("not-there"), wrongMatchSeq, 4)
	if err == nil {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	seq, val, err = c.Get([]byte("not-there"), wrongMatchSeq, true)
	if err == nil {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	seq, val, err = c.Get([]byte("a"), wrongMatchSeq, true)
	if err == nil {
		t.Errorf("expected ErrMatchSeq, err: %#v", err)
	}
	seq, val, err = c.Get([]byte("a"), NO_MATCH_SEQ, true)
	if err != nil {
		t.Errorf("expected no err")
	}
	partitionId, key, seq, val, err = c.Min(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "0" || seq != 3 ||
		string(val) != "00" {
		t.Errorf("expected min 0, got key: %s, seq: %d, val: %s",
			key, seq, val)
	}
	partitionId, key, seq, val, err = c.Max(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "b" || seq != 2 ||
		string(val) != "B" {
		t.Errorf("expected max b, got key: %s, seq: %d, val: %s",
			key, seq, val)
	}

	testSimpleCursorKeys(t, c, "3 key", true, "a", "a,b")
	testSimpleCursorKeys(t, c, "3 key", true, "0", "0,a,b")
	testSimpleCursorKeys(t, c, "3 key", true, "aa", "b")
	testSimpleCursorKeys(t, c, "3 key", true, "z", "")

	testSimpleCursorKeys(t, c, "3 key", false, "a", "a,0")
	testSimpleCursorKeys(t, c, "3 key", false, "0", "0")
	testSimpleCursorKeys(t, c, "3 key", false, "aa", "a,0")
	testSimpleCursorKeys(t, c, "3 key", false, "z", "b,a,0")

	// ------------------------------------------------
	err = c.Set(0, []byte("a"), 1, 10, []byte("AA"))
	if err != nil {
		t.Errorf("expected no err on Set with correct seq")
	}
	_, _, err = c.Get([]byte("a"), seq, true)
	if err != ErrMatchSeq {
		t.Errorf("expected ErrMatchSeq")
	}
	seq, val, err = c.Get([]byte("a"), 10, true)
	if err != nil || string(val) != "AA" {
		t.Errorf("expected no err and AA")
	}
	if seq != 10 {
		t.Errorf("expected seq of 10")
	}
	seq, val, err = c.Get([]byte("a"), NO_MATCH_SEQ, true)
	if err != nil || string(val) != "AA" {
		t.Errorf("expected no err and AA")
	}
	if seq != 10 {
		t.Errorf("expected seq of 10")
	}

	testSimpleCursorKeys(t, c, "3 key after update", true, "a", "a,b")
	testSimpleCursorKeys(t, c, "3 key after update", true, "0", "0,a,b")
	testSimpleCursorKeys(t, c, "3 key after update", true, "aa", "b")
	testSimpleCursorKeys(t, c, "3 key after update", true, "z", "")

	testSimpleCursorKeys(t, c, "3 key after update", false, "a", "a,0")
	testSimpleCursorKeys(t, c, "3 key after update", false, "0", "0")
	testSimpleCursorKeys(t, c, "3 key after update", false, "aa", "a,0")
	testSimpleCursorKeys(t, c, "3 key after update", false, "z", "b,a,0")

	// ------------------------------------------------
	c2, err := c.Snapshot()
	if err != nil || c2 == nil {
		t.Errorf("expected ss to work")
	}
	err = c2.Set(0, []byte("snapshot-update"), NO_MATCH_SEQ, 30, []byte("t"))
	if err != ErrReadOnly {
		t.Errorf("expected update on snapshot to fail")
	}
	err = c2.Del(0, []byte("snapshot-delete"), NO_MATCH_SEQ, 30)
	if err != ErrReadOnly {
		t.Errorf("expected delete on snapshot to fail")
	}

	testSnapshotIsUnchanged := func() {
		testSimpleCursorKeys(t, c2, "3 key after update", true, "a", "a,b")
		testSimpleCursorKeys(t, c2, "3 key after update", true, "0", "0,a,b")
		testSimpleCursorKeys(t, c2, "3 key after update", true, "aa", "b")
		testSimpleCursorKeys(t, c2, "3 key after update", true, "z", "")

		testSimpleCursorKeys(t, c2, "3 key after update", false, "a", "a,0")
		testSimpleCursorKeys(t, c2, "3 key after update", false, "0", "0")
		testSimpleCursorKeys(t, c2, "3 key after update", false, "aa", "a,0")
		testSimpleCursorKeys(t, c2, "3 key after update", false, "z", "b,a,0")
	}
	testSnapshotIsUnchanged()

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
		t.Errorf("expected min 0, got key: %s, seq: %d, val: %s",
			key, seq, val)
	}
	partitionId, key, seq, val, err = c.Max(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "x" || seq != 11 ||
		string(val) != "X" {
		t.Errorf("expected max x, got key: %s, seq: %d, val: %s",
			key, seq, val)
	}

	testSimpleCursorKeys(t, c, "4 key", true, "a", "a,b,x")
	testSimpleCursorKeys(t, c, "4 key", true, "0", "0,a,b,x")
	testSimpleCursorKeys(t, c, "4 key", true, "aa", "b,x")
	testSimpleCursorKeys(t, c, "4 key", true, "b", "b,x")
	testSimpleCursorKeys(t, c, "4 key", true, "c", "x")
	testSimpleCursorKeys(t, c, "4 key", true, "x", "x")
	testSimpleCursorKeys(t, c, "4 key", true, "z", "")

	testSimpleCursorKeys(t, c, "4 key", false, "a", "a,0")
	testSimpleCursorKeys(t, c, "4 key", false, ".", "") // "." < "0"
	testSimpleCursorKeys(t, c, "4 key", false, "0", "0")
	testSimpleCursorKeys(t, c, "4 key", false, "aa", "a,0")
	testSimpleCursorKeys(t, c, "4 key", false, "b", "b,a,0")
	testSimpleCursorKeys(t, c, "4 key", false, "c", "b,a,0")
	testSimpleCursorKeys(t, c, "4 key", false, "x", "x,b,a,0")
	testSimpleCursorKeys(t, c, "4 key", false, "z", "x,b,a,0")

	testSnapshotIsUnchanged()

	// ------------------------------------------------
	err = c.Del(0, []byte("0"), 3, 20)
	if err != nil {
		t.Errorf("expected ok delete")
	}
	seq, val, err = c.Get([]byte("0"), NO_MATCH_SEQ, true)
	if err != nil || seq != 0 || val != nil {
		t.Errorf("expected no 0")
	}
	partitionId, key, seq, val, err = c.Min(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "a" || seq != 10 ||
		string(val) != "AA" {
		t.Errorf("expected min 0, got key: %s, seq: %d, val: %s",
			key, seq, val)
	}
	partitionId, key, seq, val, err = c.Max(true)
	if err != nil {
		t.Errorf("expected no err")
	}
	if partitionId != 0 || string(key) != "x" || seq != 11 ||
		string(val) != "X" {
		t.Errorf("expected max x, got key: %s, seq: %d, val: %s",
			key, seq, val)
	}

	testSimpleCursorKeys(t, c, "3 key after del 0", true, "a", "a,b,x")
	testSimpleCursorKeys(t, c, "3 key after del 0", true, "0", "a,b,x")
	testSimpleCursorKeys(t, c, "3 key after del 0", true, "aa", "b,x")
	testSimpleCursorKeys(t, c, "3 key after del 0", true, "b", "b,x")
	testSimpleCursorKeys(t, c, "3 key after del 0", true, "c", "x")
	testSimpleCursorKeys(t, c, "3 key after del 0", true, "x", "x")
	testSimpleCursorKeys(t, c, "3 key after del 0", true, "z", "")

	testSimpleCursorKeys(t, c, "3 key after del 0", false, "a", "a")
	testSimpleCursorKeys(t, c, "3 key after del 0", false, ".", "")
	testSimpleCursorKeys(t, c, "3 key after del 0", false, "0", "")
	testSimpleCursorKeys(t, c, "3 key after del 0", false, "aa", "a")
	testSimpleCursorKeys(t, c, "3 key after del 0", false, "b", "b,a")
	testSimpleCursorKeys(t, c, "3 key after del 0", false, "c", "b,a")
	testSimpleCursorKeys(t, c, "3 key after del 0", false, "x", "x,b,a")
	testSimpleCursorKeys(t, c, "3 key after del 0", false, "z", "x,b,a")

	testSnapshotIsUnchanged()
}
