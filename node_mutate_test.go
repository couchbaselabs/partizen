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

func locBuf(loc *Loc) []byte {
	return FromBufRef(nil, loc.leafValBufRef, testBufManager)
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
				cil.Key, locBuf(&cil.Loc))
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
	if loc.Type == LocTypeVal &&
		loc.Size != uint32(len(locBuf(loc))) {
		return false
	}
	if locBuf(loc) != nil {
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

	m = []Mutation{
		Mutation{
			Key: []byte("x"),
			Op:  MUTATION_OP_DELETE,
		},
	}
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
	m := []Mutation{
		Mutation{
			Key:       []byte("a"),
			ValBufRef: testBufManager.Alloc(1, CopyToBufRef, []byte("A")),
			Op:        MUTATION_OP_UPDATE,
		},
	}
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
	if il.Loc.node == nil || locBuf(&il.Loc) != nil {
		t.Errorf("expected a keyLoc with node, no buf")
	}
	if il.Loc.node.childLocs.Len() != 1 {
		t.Errorf("expected 1 child")
	}
	if string(il.Loc.node.childLocs.Key(0)) != "a" {
		t.Errorf("expected 1 child")
	}
	if !isSomeMemLoc(il.Loc.node.childLocs.Loc(0), LocTypeVal) {
		t.Errorf("expected val child")
	}

	astr := locBuf(il.Loc.node.childLocs.Loc(0))
	if string(astr) != "A" {
		t.Errorf("expected val child is A")
	}

	// Try some DELETE's of key that's not in the tree.
	il2 := il
	for _, keyNotThere := range []string{"x", "0", "aa", ""} {
		m = []Mutation{
			Mutation{
				Key: []byte(keyNotThere),
				Op:  MUTATION_OP_DELETE,
			},
		}
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
		if il2.Loc.node == nil || locBuf(&il2.Loc) != nil {
			t.Errorf("expected a il with node, no buf")
		}
		if il2.Loc.node.childLocs.Len() != 1 {
			t.Errorf("expected 1 child")
		}
		if string(il2.Loc.node.childLocs.Key(0)) != "a" {
			t.Errorf("expected 1 child")
		}
		if !isSomeMemLoc(il2.Loc.node.childLocs.Loc(0),
			LocTypeVal) {
			t.Errorf("expected val child")
		}

		astr := locBuf(il2.Loc.node.childLocs.Loc(0))
		if string(astr) != "A" {
			t.Errorf("expected val child is A")
		}
	}

	m = []Mutation{ // Delete the only key.
		Mutation{
			Key: []byte("a"),
			Op:  MUTATION_OP_DELETE,
		},
	}
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
	m := []Mutation{
		Mutation{
			Key:       []byte("a"),
			ValBufRef: testBufManager.Alloc(1, CopyToBufRef, []byte("A")),
			Op:        MUTATION_OP_UPDATE,
		},
		Mutation{
			Key:       []byte("b"),
			ValBufRef: testBufManager.Alloc(1, CopyToBufRef, []byte("B")),
			Op:        MUTATION_OP_UPDATE,
		},
	}
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
		if il.Loc.node == nil || locBuf(&il.Loc) != nil {
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

		astr := locBuf(il.Loc.node.childLocs.Loc(0))
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

		bstr := locBuf(il.Loc.node.childLocs.Loc(1))
		if string(bstr) != "B" {
			t.Errorf("expected val child is B")
		}
	}

	checkHasNVals(il, 2)

	// Try some DELETE's of key that's not in the tree.
	il2 := il
	for _, keyNotThere := range []string{"x", "0", "aa", ""} {
		m = []Mutation{
			Mutation{
				Key: []byte(keyNotThere),
				Op:  MUTATION_OP_DELETE,
			},
		}
		il2, err := rootProcessMutations(il2, m, nil, 15, 32,
			&PtrChildLocsArrayHolder{},
			testBufManager, nil)
		if err != nil {
			t.Errorf("expected ok, err: %#v", err)
		}
		checkHasNVals(il2, 2)
	}

	m = []Mutation{ // Delete the key b.
		Mutation{
			Key: []byte("b"),
			Op:  MUTATION_OP_DELETE,
		},
	}
	il3, err := rootProcessMutations(il2, m, nil, 15, 32,
		&PtrChildLocsArrayHolder{},
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}

	checkHasNVals(il3, 1)

	m = []Mutation{ // Delete the key a.
		Mutation{
			Key: []byte("a"),
			Op:  MUTATION_OP_DELETE,
		},
	}
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
	m[0].Op = MUTATION_OP_UPDATE
	for c, i := 0, iStart; c < n; c, i = c+1, delta(c, i) {
		m[0].Key = []byte(fmt.Sprintf("%4d", i))
		m[0].ValBufRef = testBufManager.Alloc(len(m[0].Key),
			CopyToBufRef, m[0].Key)
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
