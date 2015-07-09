package partizen

import (
	"fmt"
	"testing"
)

var testBufManager BufManager

func init() {
	testBufManager = makeTestBufManager()
	if testBufManager == nil {
		panic("no testBufManager")
	}
}

func makeTestBufManager() BufManager {
	return NewDefaultBufManager(32, 1024*1024, 1.5, nil)
}

func locValBuf(loc *Loc) []byte {
	if loc.itemBufRef == nil {
		return nil
	}

	n := loc.itemBufRef.ValLen(testBufManager)

	dst := make([]byte, n, n)

	ItemBufRefAccess(loc.itemBufRef, false, false, testBufManager, 0, n,
		CopyFromBufRef, dst)

	return dst
}

func printPrefix(n int) {
	for i := 0; i < n; i++ {
		fmt.Print(".")
	}
}

func printTree(il *ChildLoc, depth int) {
	a := il.Loc.node.GetChildLocs()
	n := a.Len()
	depth1 := depth + 1
	for i := 0; i < n; i++ {
		printPrefix(depth)
		cil := a.ChildLoc(i)
		if cil.Loc.Type == LocTypeVal {
			fmt.Printf("%s = %s\n",
				cil.Key, locValBuf(&cil.Loc))
		} else if cil.Loc.Type == LocTypeNode {
			fmt.Printf("%s:\n", cil.Key)
			printTree(cil, depth1)
		} else {
			fmt.Printf("UNEXPECTED TYPE: %#v", cil)
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
	if locValBuf(loc) != nil {
		return true
	}
	return loc.node != nil
}

func TestEmptyMutate(t *testing.T) {
	il, err := rootProcessMutations(nil, nil, nil, 15, 32,
		&PtrChildLocsArrayHolder{},
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if il != nil {
		t.Errorf("expected nil il on nil, nil")
	}

	rootChildLoc := &ChildLoc{
		Loc: Loc{
			Type: LocTypeNode,
			node: &Node{},
		},
	}
	il, err = rootProcessMutations(rootChildLoc, nil, nil, 15, 32,
		&PtrChildLocsArrayHolder{},
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if il != nil {
		t.Errorf("expected nil il")
	}

	m := []Mutation{}
	il, err = rootProcessMutations(nil, m, nil, 15, 32,
		&PtrChildLocsArrayHolder{},
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if il != nil {
		t.Errorf("expected nil il on nil, nil")
	}

	m = []Mutation{
		Mutation{},
	} // Mutation.Op is unknown.
	il, err = rootProcessMutations(nil, m, nil, 15, 32,
		&PtrChildLocsArrayHolder{},
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if il != nil {
		t.Errorf("expected nil keyloc on nil, nil")
	}

	mutation, _ := NewMutation(testBufManager, MUTATION_OP_DELETE,
		0, []byte("x"), 0, nil, NO_MATCH_SEQ)

	m = []Mutation{mutation}

	il, err = rootProcessMutations(nil, m, nil, 15, 32,
		&PtrChildLocsArrayHolder{},
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, missing key delete on nil root, err: %#v", err)
	}
	if il != nil {
		t.Errorf("expected nil keyloc, missing key delete on nil root")
	}
}

func TestMutationsOn1Val(t *testing.T) {
	mutation, _ := NewMutation(testBufManager, MUTATION_OP_UPDATE,
		0, []byte("a"), 0, []byte("A"), NO_MATCH_SEQ)

	m := []Mutation{mutation}

	il, err := rootProcessMutations(nil, m, nil, 15, 32,
		&PtrChildLocsArrayHolder{},
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if il == nil {
		t.Errorf("expected a il")
	}
	if string(il.Key) != "a" {
		t.Errorf("expected a il with a Key")
	}
	if !isSomeMemLoc(&il.Loc, LocTypeNode) {
		t.Errorf("expected some keyLoc")
	}
	if il.Loc.node == nil || locValBuf(&il.Loc) != nil {
		t.Errorf("expected a keyLoc with node, no buf")
	}
	if il.Loc.node.childLocs.Len() != 1 {
		t.Errorf("expected 1 child")
	}
	if string(il.Loc.node.childLocs.Key(0)) != "a" {
		t.Errorf("expected 1 child")
	}
	if !isSomeMemLoc(il.Loc.node.childLocs.Loc(0), LocTypeVal) {
		t.Errorf("expected val child, loc: %#v", il.Loc.node.childLocs.Loc(0))
	}

	astr := locValBuf(il.Loc.node.childLocs.Loc(0))
	if string(astr) != "A" {
		t.Errorf("expected val child is A")
	}

	// Try some DELETE's of key that's not in the tree.
	il2 := il
	for _, keyNotThere := range []string{"x", "0", "aa", ""} {
		mutation, _ := NewMutation(testBufManager, MUTATION_OP_DELETE,
			0, []byte(keyNotThere), 0, nil, NO_MATCH_SEQ)

		m = []Mutation{mutation}

		il2, err := rootProcessMutations(il2, m, nil, 15, 32,
			&PtrChildLocsArrayHolder{},
			testBufManager, nil)
		if err != nil {
			t.Errorf("expected ok, err: %#v", err)
		}
		if il2 == nil {
			t.Errorf("expected il2")
		}
		if string(il2.Key) != "a" {
			t.Errorf("expected a il with a Key")
		}
		if !isSomeMemLoc(&il2.Loc, LocTypeNode) {
			t.Errorf("expected some il")
		}
		if il2.Loc.node == nil || locValBuf(&il2.Loc) != nil {
			t.Errorf("expected a il with node, no buf")
		}
		if il2.Loc.node.childLocs.Len() != 1 {
			t.Errorf("expected 1 child")
		}
		if string(il2.Loc.node.childLocs.Key(0)) != "a" {
			t.Errorf("expected 1 child")
		}
		if !isSomeMemLoc(il2.Loc.node.childLocs.Loc(0), LocTypeVal) {
			t.Errorf("expected val child, loc: %#v, %#v",
				il2.Loc.node.childLocs.Loc(0),
				il2.Loc.node.childLocs.Loc(0).itemBufRef)
		}

		astr := locValBuf(il2.Loc.node.childLocs.Loc(0))
		if string(astr) != "A" {
			t.Errorf("expected val child is A")
		}
	}

	mutation, _ = NewMutation(testBufManager, MUTATION_OP_DELETE,
		0, []byte("a"), 0, nil, NO_MATCH_SEQ)

	m = []Mutation{mutation}

	il3, err := rootProcessMutations(il2, m, nil, 15, 32,
		&PtrChildLocsArrayHolder{},
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if il3 != nil {
		t.Errorf("expected no keyloc")
	}
}

func TestMutationsOn2Vals(t *testing.T) {
	mutationA, _ := NewMutation(testBufManager, MUTATION_OP_UPDATE,
		0, []byte("a"), 0, []byte("A"), NO_MATCH_SEQ)

	mutationB, _ := NewMutation(testBufManager, MUTATION_OP_UPDATE,
		0, []byte("b"), 0, []byte("A"), NO_MATCH_SEQ)

	m := []Mutation{mutationA, mutationB}

	il, err := rootProcessMutations(nil, m, nil, 15, 32,
		&PtrChildLocsArrayHolder{},
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}

	checkHasNVals := func(il *ChildLoc, numVals int) {
		if il == nil {
			t.Errorf("expected a il")
		}
		if string(il.Key) != "a" {
			t.Errorf("expected a il with a Key")
		}
		if !isSomeMemLoc(&il.Loc, LocTypeNode) {
			t.Errorf("expected some il")
		}
		if il.Loc.node == nil || locValBuf(&il.Loc) != nil {
			t.Errorf("expected a il with node, no buf")
		}
		if il.Loc.node.childLocs.Len() != numVals {
			t.Errorf("expected %d children", numVals)
		}
		if numVals >= 1 {
			return
		}
		if string(il.Loc.node.childLocs.Key(0)) != "a" {
			t.Errorf("expected child 0 is a")
		}
		if !isSomeMemLoc(il.Loc.node.childLocs.Loc(0), LocTypeVal) {
			t.Errorf("expected val child")
		}

		astr := locValBuf(il.Loc.node.childLocs.Loc(0))
		if string(astr) != "A" {
			t.Errorf("expected val child is A")
		}
		if numVals >= 2 {
			return
		}
		if string(il.Loc.node.childLocs.Key(1)) != "b" {
			t.Errorf("expected child 1 is b")
		}
		if !isSomeMemLoc(il.Loc.node.childLocs.Loc(1), LocTypeVal) {
			t.Errorf("expected val child")
		}

		bstr := locValBuf(il.Loc.node.childLocs.Loc(1))
		if string(bstr) != "B" {
			t.Errorf("expected val child is B")
		}
	}

	checkHasNVals(il, 2)

	// Try some DELETE's of key that's not in the tree.
	il2 := il
	for _, keyNotThere := range []string{"x", "0", "aa", ""} {
		mutation, _ := NewMutation(testBufManager, MUTATION_OP_DELETE,
			0, []byte(keyNotThere), 0, nil, NO_MATCH_SEQ)

		m = []Mutation{mutation}

		il2, err := rootProcessMutations(il2, m, nil, 15, 32,
			&PtrChildLocsArrayHolder{},
			testBufManager, nil)
		if err != nil {
			t.Errorf("expected ok, err: %#v", err)
		}
		checkHasNVals(il2, 2)
	}

	mutation, _ := NewMutation(testBufManager, MUTATION_OP_DELETE,
		0, []byte("b"), 0, nil, NO_MATCH_SEQ)

	m = []Mutation{mutation}

	il3, err := rootProcessMutations(il2, m, nil, 15, 32,
		&PtrChildLocsArrayHolder{},
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}

	checkHasNVals(il3, 1)

	mutation, _ = NewMutation(testBufManager, MUTATION_OP_DELETE,
		0, []byte("a"), 0, nil, NO_MATCH_SEQ)

	m = []Mutation{mutation}

	il4, err := rootProcessMutations(il3, m, nil, 15, 32,
		&PtrChildLocsArrayHolder{},
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if il4 != nil {
		t.Errorf("expected no il")
	}
}

var PRIME = 47

func TestMutationsDepth(t *testing.T) {
	n := 10 // Num inserts.
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

	var rootChildLoc *ChildLoc
	var err error
	m := make([]Mutation, 1, 1)
	for c, i := 0, iStart; c < n; c, i = c+1, delta(c, i) {
		key := []byte(fmt.Sprintf("%4d", i))
		mutation, _ := NewMutation(testBufManager, MUTATION_OP_UPDATE,
			0, key, 0, key, NO_MATCH_SEQ)
		m[0] = mutation

		rootChildLoc, err =
			rootProcessMutations(rootChildLoc, m, nil, 2, 4,
				&PtrChildLocsArrayHolder{},
				testBufManager, nil)
		if err != nil {
			t.Errorf("unexpected err: %#v", err)
		}

		fmt.Printf("================= %d\n", i)
		printTree(rootChildLoc, 0)
	}
}
