package partizen

import (
	"fmt"
	"testing"
)

func printPrefix(n int) {
	for i := 0; i < n; i++ {
		fmt.Print(".")
	}
}

func printTree(ksl *KeySeqLoc, depth int) {
	a := ksl.Loc.node.GetKeySeqLocs()
	n := a.Len()
	depth1 := depth + 1
	for i := 0; i < n; i++ {
		printPrefix(depth)
		cksl := a.KeySeqLoc(i)
		if cksl.Loc.Type == LocTypeVal {
			fmt.Printf("%s = %s\n", cksl.Key, cksl.Loc.buf)
		} else if cksl.Loc.Type == LocTypeNode {
			fmt.Printf("%s:\n", cksl.Key)
			printTree(cksl, depth1)
		} else {
			fmt.Printf("UNEXPECTED TYPE: %#v", cksl)
		}
	}
}

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
	ksl, err := rootKeySeqLocProcessMutations(nil, nil, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if ksl != nil {
		t.Errorf("expected nil ksl on nil, nil")
	}

	rootKeySeqLoc := &KeySeqLoc{
		Loc: Loc{
			Type: LocTypeNode,
			node: &NodeMem{},
		},
	}
	ksl, err = rootKeySeqLocProcessMutations(rootKeySeqLoc, nil, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if ksl != nil {
		t.Errorf("expected nil ksl")
	}

	m := []Mutation{}
	ksl, err = rootKeySeqLocProcessMutations(nil, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if ksl != nil {
		t.Errorf("expected nil ksl on nil, nil")
	}

	m = []Mutation{
		Mutation{},
	} // Mutation.Op is unknown.
	ksl, err = rootKeySeqLocProcessMutations(nil, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if ksl != nil {
		t.Errorf("expected nil keyloc on nil, nil")
	}

	m = []Mutation{
		Mutation{
			Key: []byte("x"),
			Op:  MUTATION_OP_DELETE,
		},
	}
	ksl, err = rootKeySeqLocProcessMutations(nil, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, missing key delete on nil root, err: %#v", err)
	}
	if ksl != nil {
		t.Errorf("expected nil keyloc, missing key delete on nil root")
	}
}

func TestMutationsOn1Val(t *testing.T) {
	m := []Mutation{
		Mutation{
			Key: []byte("a"),
			Val: []byte("A"),
			Op:  MUTATION_OP_UPDATE,
		},
	}
	ksl, err := rootKeySeqLocProcessMutations(nil, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if ksl == nil {
		t.Errorf("expected a ksl")
	}
	if string(ksl.Key) != "a" {
		t.Errorf("expected a ksl with a Key")
	}
	if !isSomeMemLoc(&ksl.Loc, LocTypeNode) {
		t.Errorf("expected some keyLoc")
	}
	if ksl.Loc.node == nil || ksl.Loc.buf != nil {
		t.Errorf("expected a keyLoc with node, no buf")
	}
	if ksl.Loc.node.(*NodeMem).KeySeqLocs.Len() != 1 {
		t.Errorf("expected 1 child")
	}
	if string(ksl.Loc.node.(*NodeMem).KeySeqLocs.Key(0)) != "a" {
		t.Errorf("expected 1 child")
	}
	if !isSomeMemLoc(ksl.Loc.node.(*NodeMem).KeySeqLocs.Loc(0), LocTypeVal) {
		t.Errorf("expected val child")
	}
	if string(ksl.Loc.node.(*NodeMem).KeySeqLocs.Loc(0).buf) != "A" {
		t.Errorf("expected val child is A")
	}

	// Try some DELETE's of key that's not in the tree.
	ksl2 := ksl
	for _, keyNotThere := range []string{"x", "0", "aa", ""} {
		m = []Mutation{
			Mutation{
				Key: []byte(keyNotThere),
				Op:  MUTATION_OP_DELETE,
			},
		}
		ksl2, err := rootKeySeqLocProcessMutations(ksl2, m, 32, nil)
		if err != nil {
			t.Errorf("expected ok, err: %#v", err)
		}
		if ksl2 == nil {
			t.Errorf("expected ksl2")
		}
		if string(ksl2.Key) != "a" {
			t.Errorf("expected a ksl with a Key")
		}
		if !isSomeMemLoc(&ksl2.Loc, LocTypeNode) {
			t.Errorf("expected some ksl")
		}
		if ksl2.Loc.node == nil || ksl2.Loc.buf != nil {
			t.Errorf("expected a ksl with node, no buf")
		}
		if ksl2.Loc.node.(*NodeMem).KeySeqLocs.Len() != 1 {
			t.Errorf("expected 1 child")
		}
		if string(ksl2.Loc.node.(*NodeMem).KeySeqLocs.Key(0)) != "a" {
			t.Errorf("expected 1 child")
		}
		if !isSomeMemLoc(ksl2.Loc.node.(*NodeMem).KeySeqLocs.Loc(0), LocTypeVal) {
			t.Errorf("expected val child")
		}
		if string(ksl2.Loc.node.(*NodeMem).KeySeqLocs.Loc(0).buf) != "A" {
			t.Errorf("expected val child is A")
		}
	}

	m = []Mutation{ // Delete the only key.
		Mutation{
			Key: []byte("a"),
			Op:  MUTATION_OP_DELETE,
		},
	}
	ksl3, err := rootKeySeqLocProcessMutations(ksl2, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if ksl3 != nil {
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
	ksl, err := rootKeySeqLocProcessMutations(nil, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}

	checkHasNVals := func(ksl *KeySeqLoc, numVals int) {
		if ksl == nil {
			t.Errorf("expected a ksl")
		}
		if string(ksl.Key) != "a" {
			t.Errorf("expected a ksl with a Key")
		}
		if !isSomeMemLoc(&ksl.Loc, LocTypeNode) {
			t.Errorf("expected some ksl")
		}
		if ksl.Loc.node == nil || ksl.Loc.buf != nil {
			t.Errorf("expected a ksl with node, no buf")
		}
		if ksl.Loc.node.(*NodeMem).KeySeqLocs.Len() != numVals {
			t.Errorf("expected %d children", numVals)
		}
		if numVals >= 1 {
			return
		}
		if string(ksl.Loc.node.(*NodeMem).KeySeqLocs.Key(0)) != "a" {
			t.Errorf("expected child 0 is a")
		}
		if !isSomeMemLoc(ksl.Loc.node.(*NodeMem).KeySeqLocs.Loc(0), LocTypeVal) {
			t.Errorf("expected val child")
		}
		if string(ksl.Loc.node.(*NodeMem).KeySeqLocs.Loc(0).buf) != "A" {
			t.Errorf("expected val child is A")
		}
		if numVals >= 2 {
			return
		}
		if string(ksl.Loc.node.(*NodeMem).KeySeqLocs.Key(1)) != "b" {
			t.Errorf("expected child 1 is b")
		}
		if !isSomeMemLoc(ksl.Loc.node.(*NodeMem).KeySeqLocs.Loc(1), LocTypeVal) {
			t.Errorf("expected val child")
		}
		if string(ksl.Loc.node.(*NodeMem).KeySeqLocs.Loc(1).buf) != "B" {
			t.Errorf("expected val child is B")
		}
	}

	checkHasNVals(ksl, 2)

	// Try some DELETE's of key that's not in the tree.
	ksl2 := ksl
	for _, keyNotThere := range []string{"x", "0", "aa", ""} {
		m = []Mutation{
			Mutation{
				Key: []byte(keyNotThere),
				Op:  MUTATION_OP_DELETE,
			},
		}
		ksl2, err := rootKeySeqLocProcessMutations(ksl2, m, 32, nil)
		if err != nil {
			t.Errorf("expected ok, err: %#v", err)
		}
		checkHasNVals(ksl2, 2)
	}

	m = []Mutation{ // Delete the key b.
		Mutation{
			Key: []byte("b"),
			Op:  MUTATION_OP_DELETE,
		},
	}
	ksl3, err := rootKeySeqLocProcessMutations(ksl2, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}

	checkHasNVals(ksl3, 1)

	m = []Mutation{ // Delete the key a.
		Mutation{
			Key: []byte("a"),
			Op:  MUTATION_OP_DELETE,
		},
	}
	ksl4, err := rootKeySeqLocProcessMutations(ksl3, m, 32, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if ksl4 != nil {
		t.Errorf("expected no ksl")
	}
}

var PRIME = 47

func TestMutationsDepth(t *testing.T) {
	n := 100 // Num inserts.
	iStart := 0
	iRange := 100 // Key range is [0, iRange).
	delta := func(c, i int) int { return (i + 1) % iRange }
	if true {
		// Use true for repeatably "random", false for simple increasing keys.
		if true {
			iStart = iRange / 2
			delta = func(c, i int) int { return (i + PRIME) % iRange }
		}
	} else {
		iStart = iRange - 1 // Reverse keys (simple decreasing keys).
		delta = func(c, i int) int { return i - 1 }
	}

	var rootKeySeqLoc *KeySeqLoc
	var err error
	m := make([]Mutation, 1, 1)
	m[0].Op = MUTATION_OP_UPDATE
	for c, i := 0, iStart; c < n; c, i = c+1, delta(c, i) {
		m[0].Key = []byte(fmt.Sprintf("%4d", i))
		m[0].Val = Val(m[0].Key)
		rootKeySeqLoc, err =
			rootKeySeqLocProcessMutations(rootKeySeqLoc, m, 4, nil)
		if err != nil {
			t.Errorf("unexpected err: %#v", err)
		}

		fmt.Printf("================= %d\n", i)
		printTree(rootKeySeqLoc, 0)
	}
}
