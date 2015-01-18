package partizen

import (
	"testing"
)

func isSomeMemLoc(loc *Loc, expectedLocType uint8) bool {
	if !(loc.Offset == 0 && loc.Flags == 0 && loc.CheckSum == 0) {
		return false
	}
	if loc.Type != expectedLocType {
		return false
	}
	if loc.Type == LocTypeVal && loc.Size != uint32(len(loc.buf)) {
		return false
	}
	return loc.buf != nil || loc.node != nil
}

func TestEmptyMutate(t *testing.T) {
	kl, err := rootNodeLocProcessMutations(nil, nil, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if kl != nil {
		t.Errorf("expected nil keyloc on nil, nil")
	}

	rootLoc := &Loc{
		Type: LocTypeNode,
		node: &NodeMem{},
	}
	kl, err = rootNodeLocProcessMutations(rootLoc, nil, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if kl != nil {
		t.Errorf("expected nil keyloc")
	}

	m := []Mutation{}
	kl, err = rootNodeLocProcessMutations(nil, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if kl != nil {
		t.Errorf("expected nil keyloc on nil, nil")
	}

	m = []Mutation{Mutation{}} // Mutation.Op is unknown.
	kl, err = rootNodeLocProcessMutations(nil, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if kl != nil {
		t.Errorf("expected nil keyloc on nil, nil")
	}

	m = []Mutation{Mutation{
		Key: []byte("x"),
		Op:  MUTATION_OP_DELETE,
	}}
	kl, err = rootNodeLocProcessMutations(nil, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, missing key delete on nil root, err: %#v", err)
	}
	if kl != nil {
		t.Errorf("expected nil keyloc, missing key delete on nil root")
	}
}

func TestMutationsOn1Val(t *testing.T) {
	m := []Mutation{Mutation{
		Key: []byte("a"),
		Val: []byte("A"),
		Op:  MUTATION_OP_UPDATE,
	}}
	kl, err := rootNodeLocProcessMutations(nil, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if kl == nil {
		t.Errorf("expected a keyloc")
	}
	if string(kl.Key) != "a" {
		t.Errorf("expected a keyLoc with a Key")
	}
	if !isSomeMemLoc(&kl.Loc, LocTypeNode) {
		t.Errorf("expected some keyLoc")
	}
	if kl.Loc.node == nil || kl.Loc.buf != nil {
		t.Errorf("expected a keyLoc with node, no buf")
	}
	if len(kl.Loc.node.(*NodeMem).KeyLocs) != 1 {
		t.Errorf("expected 1 child")
	}
	if string(kl.Loc.node.(*NodeMem).KeyLocs[0].Key) != "a" {
		t.Errorf("expected 1 child")
	}
	if !isSomeMemLoc(&kl.Loc.node.(*NodeMem).KeyLocs[0].Loc, LocTypeVal) {
		t.Errorf("expected val child")
	}
	if string(kl.Loc.node.(*NodeMem).KeyLocs[0].Loc.buf) != "A" {
		t.Errorf("expected val child is A")
	}

	// Try some DELETE's of key that's not in the tree.
	kl2 := kl
	for _, keyNotThere := range []string{"x", "0", "aa", ""} {
		m = []Mutation{
			Mutation{
				Key: []byte(keyNotThere),
				Op:  MUTATION_OP_DELETE,
			},
		}
		kl2, err := rootNodeLocProcessMutations(&kl2.Loc, m, 32, nil)
		if err != nil {
			t.Errorf("expected ok, err: %#v", err)
		}
		if kl2 == nil {
			t.Errorf("expected kl2")
		}
		if string(kl2.Key) != "a" {
			t.Errorf("expected a keyLoc with a Key")
		}
		if !isSomeMemLoc(&kl2.Loc, LocTypeNode) {
			t.Errorf("expected some keyLoc")
		}
		if kl2.Loc.node == nil || kl2.Loc.buf != nil {
			t.Errorf("expected a keyLoc with node, no buf")
		}
		if len(kl2.Loc.node.(*NodeMem).KeyLocs) != 1 {
			t.Errorf("expected 1 child")
		}
		if string(kl2.Loc.node.(*NodeMem).KeyLocs[0].Key) != "a" {
			t.Errorf("expected 1 child")
		}
		if !isSomeMemLoc(&kl2.Loc.node.(*NodeMem).KeyLocs[0].Loc, LocTypeVal) {
			t.Errorf("expected val child")
		}
		if string(kl2.Loc.node.(*NodeMem).KeyLocs[0].Loc.buf) != "A" {
			t.Errorf("expected val child is A")
		}
	}

	m = []Mutation{ // Delete the only key.
		Mutation{
			Key: []byte("a"),
			Op:  MUTATION_OP_DELETE,
		},
	}
	kl3, err := rootNodeLocProcessMutations(&kl2.Loc, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if kl3 != nil {
		t.Errorf("expected no keyloc")
	}
}

func TestMutationsOn2Vals(t *testing.T) {
	m := []Mutation{
		Mutation{
			Key: []byte("a"),
			Val: []byte("A"),
			Op:  MUTATION_OP_UPDATE,
		},
		Mutation{
			Key: []byte("b"),
			Val: []byte("B"),
			Op:  MUTATION_OP_UPDATE,
		},
	}
	kl, err := rootNodeLocProcessMutations(nil, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}

	checkHasNVals := func(kl *KeyLoc, numVals int) {
		if kl == nil {
			t.Errorf("expected a keyloc")
		}
		if string(kl.Key) != "a" {
			t.Errorf("expected a keyLoc with a Key")
		}
		if !isSomeMemLoc(&kl.Loc, LocTypeNode) {
			t.Errorf("expected some keyLoc")
		}
		if kl.Loc.node == nil || kl.Loc.buf != nil {
			t.Errorf("expected a keyLoc with node, no buf")
		}
		if len(kl.Loc.node.(*NodeMem).KeyLocs) != numVals {
			t.Errorf("expected %d children", numVals)
		}
		if numVals >= 1 {
			return
		}
		if string(kl.Loc.node.(*NodeMem).KeyLocs[0].Key) != "a" {
			t.Errorf("expected child 0 is a")
		}
		if !isSomeMemLoc(&kl.Loc.node.(*NodeMem).KeyLocs[0].Loc, LocTypeVal) {
			t.Errorf("expected val child")
		}
		if string(kl.Loc.node.(*NodeMem).KeyLocs[0].Loc.buf) != "A" {
			t.Errorf("expected val child is A")
		}
		if numVals >= 2 {
			return
		}
		if string(kl.Loc.node.(*NodeMem).KeyLocs[1].Key) != "b" {
			t.Errorf("expected child 1 is b")
		}
		if !isSomeMemLoc(&kl.Loc.node.(*NodeMem).KeyLocs[1].Loc, LocTypeVal) {
			t.Errorf("expected val child")
		}
		if string(kl.Loc.node.(*NodeMem).KeyLocs[1].Loc.buf) != "B" {
			t.Errorf("expected val child is B")
		}
	}

	checkHasNVals(kl, 2)

	// Try some DELETE's of key that's not in the tree.
	kl2 := kl
	for _, keyNotThere := range []string{"x", "0", "aa", ""} {
		m = []Mutation{
			Mutation{
				Key: []byte(keyNotThere),
				Op:  MUTATION_OP_DELETE,
			},
		}
		kl2, err := rootNodeLocProcessMutations(&kl2.Loc, m, 32, nil)
		if err != nil {
			t.Errorf("expected ok, err: %#v", err)
		}
		checkHasNVals(kl2, 2)
	}

	m = []Mutation{ // Delete the key b.
		Mutation{
			Key: []byte("b"),
			Op:  MUTATION_OP_DELETE,
		},
	}
	kl3, err := rootNodeLocProcessMutations(&kl2.Loc, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}

	checkHasNVals(kl3, 1)

	m = []Mutation{ // Delete the key a.
		Mutation{
			Key: []byte("a"),
			Op:  MUTATION_OP_DELETE,
		},
	}
	kl4, err := rootNodeLocProcessMutations(&kl3.Loc, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if kl4 != nil {
		t.Errorf("expected no keyloc")
	}
}