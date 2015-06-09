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
	return BufRefBytes(nil, loc.bufRef, testBufManager)
}

func printPrefix(n int) {
	for i := 0; i < n; i++ {
		fmt.Print(".")
	}
}

func printTree(ksl *ItemLoc, depth int) {
	a := ksl.Loc.node.GetItemLocs()
	n := a.Len()
	depth1 := depth + 1
	for i := 0; i < n; i++ {
		printPrefix(depth)
		cksl := a.ItemLoc(i)
		if cksl.Loc.Type == LocTypeVal {
			fmt.Printf("%s = %s\n",
				cksl.Key, locBuf(&cksl.Loc))
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
	ksl, err := rootProcessMutations(nil, nil, nil, 15, 32,
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if ksl != nil {
		t.Errorf("expected nil ksl on nil, nil")
	}

	rootItemLoc := &ItemLoc{
		Loc: Loc{
			Type: LocTypeNode,
			node: &NodeMem{},
		},
	}
	ksl, err = rootProcessMutations(rootItemLoc, nil, nil, 15, 32,
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if ksl != nil {
		t.Errorf("expected nil ksl")
	}

	m := []Mutation{}
	ksl, err = rootProcessMutations(nil, m, nil, 15, 32, testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if ksl != nil {
		t.Errorf("expected nil ksl on nil, nil")
	}

	m = []Mutation{
		Mutation{},
	} // Mutation.Op is unknown.
	ksl, err = rootProcessMutations(nil, m, nil, 15, 32, testBufManager, nil)
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
	ksl, err = rootProcessMutations(nil, m, nil, 15, 32, testBufManager, nil)
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
			Key:       []byte("a"),
			ValBufRef: testBufManager.Alloc(1, CopyToBufRef, []byte("A")),
			Op:        MUTATION_OP_UPDATE,
		},
	}
	ksl, err := rootProcessMutations(nil, m, nil, 15, 32, testBufManager, nil)
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
	if ksl.Loc.node == nil || locBuf(&ksl.Loc) != nil {
		t.Errorf("expected a keyLoc with node, no buf")
	}
	if ksl.Loc.node.(*NodeMem).ItemLocs.Len() != 1 {
		t.Errorf("expected 1 child")
	}
	if string(ksl.Loc.node.(*NodeMem).ItemLocs.Key(0)) != "a" {
		t.Errorf("expected 1 child")
	}
	if !isSomeMemLoc(ksl.Loc.node.(*NodeMem).ItemLocs.Loc(0), LocTypeVal) {
		t.Errorf("expected val child")
	}

	astr := locBuf(ksl.Loc.node.(*NodeMem).ItemLocs.Loc(0))
	if string(astr) != "A" {
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
		ksl2, err := rootProcessMutations(ksl2, m, nil, 15, 32,
			testBufManager, nil)
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
		if ksl2.Loc.node == nil || locBuf(&ksl2.Loc) != nil {
			t.Errorf("expected a ksl with node, no buf")
		}
		if ksl2.Loc.node.(*NodeMem).ItemLocs.Len() != 1 {
			t.Errorf("expected 1 child")
		}
		if string(ksl2.Loc.node.(*NodeMem).ItemLocs.Key(0)) != "a" {
			t.Errorf("expected 1 child")
		}
		if !isSomeMemLoc(ksl2.Loc.node.(*NodeMem).ItemLocs.Loc(0),
			LocTypeVal) {
			t.Errorf("expected val child")
		}

		astr := locBuf(ksl2.Loc.node.(*NodeMem).ItemLocs.Loc(0))
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
	ksl3, err := rootProcessMutations(ksl2, m, nil, 15, 32,
		testBufManager, nil)
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
	ksl, err := rootProcessMutations(nil, m, nil, 15, 32,
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}

	checkHasNVals := func(ksl *ItemLoc, numVals int) {
		if ksl == nil {
			t.Errorf("expected a ksl")
		}
		if string(ksl.Key) != "a" {
			t.Errorf("expected a ksl with a Key")
		}
		if !isSomeMemLoc(&ksl.Loc, LocTypeNode) {
			t.Errorf("expected some ksl")
		}
		if ksl.Loc.node == nil || locBuf(&ksl.Loc) != nil {
			t.Errorf("expected a ksl with node, no buf")
		}
		if ksl.Loc.node.(*NodeMem).ItemLocs.Len() != numVals {
			t.Errorf("expected %d children", numVals)
		}
		if numVals >= 1 {
			return
		}
		if string(ksl.Loc.node.(*NodeMem).ItemLocs.Key(0)) != "a" {
			t.Errorf("expected child 0 is a")
		}
		if !isSomeMemLoc(ksl.Loc.node.(*NodeMem).ItemLocs.Loc(0), LocTypeVal) {
			t.Errorf("expected val child")
		}

		astr := locBuf(ksl.Loc.node.(*NodeMem).ItemLocs.Loc(0))
		if string(astr) != "A" {
			t.Errorf("expected val child is A")
		}
		if numVals >= 2 {
			return
		}
		if string(ksl.Loc.node.(*NodeMem).ItemLocs.Key(1)) != "b" {
			t.Errorf("expected child 1 is b")
		}
		if !isSomeMemLoc(ksl.Loc.node.(*NodeMem).ItemLocs.Loc(1), LocTypeVal) {
			t.Errorf("expected val child")
		}

		bstr := locBuf(ksl.Loc.node.(*NodeMem).ItemLocs.Loc(1))
		if string(bstr) != "B" {
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
		ksl2, err := rootProcessMutations(ksl2, m, nil, 15, 32,
			testBufManager, nil)
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
	ksl3, err := rootProcessMutations(ksl2, m, nil, 15, 32,
		testBufManager, nil)
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
	ksl4, err := rootProcessMutations(ksl3, m, nil, 15, 32,
		testBufManager, nil)
	if err != nil {
		t.Errorf("expected ok, err: %#v", err)
	}
	if ksl4 != nil {
		t.Errorf("expected no ksl")
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

	var rootItemLoc *ItemLoc
	var err error
	m := make([]Mutation, 1, 1)
	m[0].Op = MUTATION_OP_UPDATE
	for c, i := 0, iStart; c < n; c, i = c+1, delta(c, i) {
		m[0].Key = []byte(fmt.Sprintf("%4d", i))
		m[0].ValBufRef = testBufManager.Alloc(len(m[0].Key),
			CopyToBufRef, m[0].Key)
		rootItemLoc, err =
			rootProcessMutations(rootItemLoc, m, nil, 2, 4,
				testBufManager, nil)
		if err != nil {
			t.Errorf("unexpected err: %#v", err)
		}

		fmt.Printf("================= %d\n", i)
		printTree(rootItemLoc, 0)
	}
}
