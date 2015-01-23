package partizen

import (
	"bytes"
	"fmt"
	"io"
)

// rootKeySeqLocProcessMutations is the entry function for applying a
// batch of copy-on-write mutations to a tree (rootKeySeqLoc).  The
// mutations must be ordered by ascending key order, and must also
// have no duplicates.  That is, if the application has a sequence of
// mutations on the same key, the caller must provide only the last
// mutation for any key.  Use nil for rootKeySeqLoc to start a brand
// new tree.
func rootKeySeqLocProcessMutations(rootKeySeqLoc *KeySeqLoc,
	mutations []Mutation, minFanOut, maxFanOut int, r io.ReaderAt) (
	*KeySeqLoc, error) {
	keySeqLocs, err := keySeqLocProcessMutations(rootKeySeqLoc,
		mutations, 0, len(mutations), minFanOut, maxFanOut, r)
	if err != nil {
		return nil, fmt.Errorf("rootKeySeqLocProcessMutations:"+
			" rootKeySeqLoc: %#v, err: %v", rootKeySeqLoc, err)
	}
	if keySeqLocs != nil {
		for keySeqLocs.Len() > 1 ||
			(keySeqLocs.Len() > 0 && keySeqLocs.Loc(0).Type == LocTypeVal) {
			keySeqLocs = groupKeySeqLocs(keySeqLocs, minFanOut, maxFanOut, nil)
		}
		if keySeqLocs.Len() > 0 {
			return keySeqLocs.KeySeqLoc(0), nil
		}
	}
	return nil, nil
}

// keySeqLocProcessMutations recursively applies the batch of
// mutations down the tree, building up copy-on-write new nodes.
func keySeqLocProcessMutations(keySeqLoc *KeySeqLoc,
	mutations []Mutation, mbeg, mend int, minFanOut, maxFanOut int,
	r io.ReaderAt) (KeySeqLocs, error) {
	var keySeqLocs KeySeqLocs

	if keySeqLoc != nil {
		if keySeqLoc.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&keySeqLoc.Loc, r)
			if err != nil {
				return nil, fmt.Errorf("keySeqLocProcessMutations:"+
					" keySeqLoc: %#v, err: %v", keySeqLoc, err)
			}
			if node != nil {
				keySeqLocs = node.GetKeySeqLocs()
			}
		} else if keySeqLoc.Loc.Type == LocTypeVal {
			keySeqLocs = PtrKeySeqLocsArray{keySeqLoc}
		} else {
			return nil, fmt.Errorf("keySeqLocProcessMutations:"+
				" unexpected keySeqLoc.Type, keySeqLoc: %#v", keySeqLoc)
		}
	}

	n := keySeqLocsLen(keySeqLocs)
	m := mend - mbeg

	var builder KeySeqLocsBuilder
	if n <= 0 || keySeqLocs.Loc(0).Type == LocTypeVal {
		builder = &ValsBuilder{s: make(PtrKeySeqLocsArray, 0, m)} // Mem mgmt.
	} else {
		builder = &NodesBuilder{NodeMutations: make([]NodeMutations, 0, m)}
	}

	processMutations(keySeqLocs, 0, n, mutations, mbeg, mend, builder)

	return builder.Done(mutations, minFanOut, maxFanOut, r)
}

// groupKeySeqLocs assigns a key-ordered sequence of children to new
// parent nodes, where the parent nodes will meet the given maxFanOut.
func groupKeySeqLocs(childKeySeqLocs KeySeqLocs, minFanOut, maxFanOut int,
	groupedKeySeqLocsStart KeySeqLocs) KeySeqLocs {
	groupedKeySeqLocs := groupedKeySeqLocsStart

	childKeySeqLocs = rebalanceNodes(childKeySeqLocs, minFanOut, maxFanOut)

	// TODO: A more optimal grouping approach would instead partition
	// the childKeySeqLocs more evenly, instead of the current approach
	// where the last group might be unfairly too small as it has only
	// the simple remainder of childKeySeqLocs.
	n := keySeqLocsLen(childKeySeqLocs)
	beg := 0
	for i := maxFanOut; i < n; i = i + maxFanOut {
		a, maxSeq := keySeqLocsSlice(childKeySeqLocs, beg, i)
		groupedKeySeqLocs = keySeqLocsAppend(groupedKeySeqLocs,
			a.Key(0), maxSeq, Loc{
				Type: LocTypeNode,
				node: &NodeMem{KeySeqLocs: a},
			})
		beg = i
	}
	if beg < n { // If there were leftovers...
		if beg <= 0 { // If there were only leftovers, group them...
			a, maxSeq := keySeqLocsSlice(childKeySeqLocs, beg, n)
			groupedKeySeqLocs = keySeqLocsAppend(groupedKeySeqLocs,
				a.Key(0), maxSeq, Loc{
					Type: LocTypeNode,
					node: &NodeMem{KeySeqLocs: a},
				})
		} else { // Pass the leftovers upwards.
			for i := beg; i < n; i++ {
				groupedKeySeqLocs =
					groupedKeySeqLocs.Append(*childKeySeqLocs.KeySeqLoc(i))
			}
		}
	}

	return groupedKeySeqLocs
}

func keySeqLocsLen(a KeySeqLocs) int {
	if a == nil {
		return 0
	}
	return a.Len()
}

func keySeqLocsSlice(a KeySeqLocs, from, to int) (KeySeqLocs, Seq) {
	kslArr := make(KeySeqLocsArray, to-from)
	maxSeq := Seq(0)

	lenKeys := 0
	for i := from; i < to; i++ {
		lenKeys = lenKeys + len(a.Key(i))
	}

	keys := make([]byte, 0, lenKeys)
	for i := from; i < to; i++ {
		key := a.Key(i)
		keys = append(keys, key...)
		key = keys[len(keys)-len(key):]

		seq := a.Seq(i)
		if maxSeq < seq {
			maxSeq = seq
		}

		kslArr[i-from] = KeySeqLoc{Key: key, Seq: seq, Loc: *(a.Loc(i))}
	}

	return kslArr, maxSeq
}

func keySeqLocsAppend(g KeySeqLocs, key Key, seq Seq, loc Loc) KeySeqLocs {
	if g == nil {
		return KeySeqLocsArray{KeySeqLoc{Key: key, Seq: seq, Loc: loc}}
	}
	return g.Append(KeySeqLoc{Key: key, Seq: seq, Loc: loc})
}

// processMutations merges or zippers together a key-ordered sequence
// of existing KeySeqLoc's with a key-ordered batch of mutations.
func processMutations(
	existings KeySeqLocs,
	ebeg, eend int, // Sub-range of existings[ebeg:eend] to process.
	mutations []Mutation,
	mbeg, mend int, // Sub-range of mutations[mbeg:mend] to process.
	builder KeySeqLocsBuilder) {
	existing, eok, ecur := nextKeySeqLoc(ebeg, eend, existings)
	mutation, mok, mcur := nextMutation(mbeg, mend, mutations)

	for eok && mok {
		// TODO: See if binary search to skip past keys here is faster?
		c := bytes.Compare(existing.Key, mutation.Key)
		if c < 0 {
			builder.AddExisting(existing)
			existing, eok, ecur = nextKeySeqLoc(ecur+1, eend, existings)
		} else {
			if c == 0 {
				builder.AddUpdate(existing, mutation, mcur)
				existing, eok, ecur = nextKeySeqLoc(ecur+1, eend, existings)
			} else {
				builder.AddNew(mutation, mcur)
			}
			mutation, mok, mcur = nextMutation(mcur+1, mend, mutations)
		}
	}
	for eok {
		builder.AddExisting(existing)
		existing, eok, ecur = nextKeySeqLoc(ecur+1, eend, existings)
	}
	for mok {
		builder.AddNew(mutation, mcur)
		mutation, mok, mcur = nextMutation(mcur+1, mend, mutations)
	}
}

func nextKeySeqLoc(idx, n int, keySeqLocs KeySeqLocs) (
	*KeySeqLoc, bool, int) {
	if idx < n {
		return keySeqLocs.KeySeqLoc(idx), true, idx
	}
	return &zeroKeySeqLoc, false, idx
}

func nextMutation(idx, n int, mutations []Mutation) (
	*Mutation, bool, int) {
	if idx < n {
		return &mutations[idx], true, idx
	}
	return &zeroMutation, false, idx
}

// --------------------------------------------------

type KeySeqLocsBuilder interface {
	AddExisting(existing *KeySeqLoc)
	AddUpdate(existing *KeySeqLoc, mutation *Mutation, mutationIdx int)
	AddNew(mutation *Mutation, mutationIdx int)
	Done(mutations []Mutation, minFanOut, maxFanOut int,
		r io.ReaderAt) (KeySeqLocs, error)
}

// --------------------------------------------------

// A ValsBuilder implements the KeySeqLocsBuilder interface to return an
// array of LocTypeVal KeySeqLoc's, which can be then used as input as
// the children to create new leaf Nodes.
type ValsBuilder struct {
	s PtrKeySeqLocsArray
}

func (b *ValsBuilder) AddExisting(existing *KeySeqLoc) {
	b.s = append(b.s, existing)
}

func (b *ValsBuilder) AddUpdate(existing *KeySeqLoc,
	mutation *Mutation, mutationIdx int) {
	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, mutationToValKeySeqLoc(mutation))
	}
}

func (b *ValsBuilder) AddNew(mutation *Mutation, mutationIdx int) {
	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, mutationToValKeySeqLoc(mutation))
	}
}

func (b *ValsBuilder) Done(mutations []Mutation, minFanOut, maxFanOut int,
	r io.ReaderAt) (KeySeqLocs, error) {
	return b.s, nil
}

func mutationToValKeySeqLoc(m *Mutation) *KeySeqLoc {
	return &KeySeqLoc{
		Key: m.Key, // NOTE: We copy key in groupKeySeqLocs/keySeqLocsSlice.
		Seq: m.Seq,
		Loc: Loc{
			Type: LocTypeVal,
			Size: uint32(len(m.Val)),
			buf:  append([]byte(nil), m.Val...), // TODO: Memory mgmt.
		},
	}
}

// --------------------------------------------------

// An NodesBuilder implements the KeySeqLocsBuilder interface to return an
// array of LocTypeNode KeySeqLoc's, which can be then used as input as
// the children to create new interior Nodes.
type NodesBuilder struct {
	NodeMutations []NodeMutations
}

type NodeMutations struct {
	BaseKeySeqLoc *KeySeqLoc
	MutationsBeg  int // Inclusive index into []Mutation.
	MutationsEnd  int // Exclusive index into []Mutation.
}

func (b *NodesBuilder) AddExisting(existing *KeySeqLoc) {
	b.NodeMutations = append(b.NodeMutations, NodeMutations{
		BaseKeySeqLoc: existing,
		MutationsBeg:  -1,
		MutationsEnd:  -1,
	})
}

func (b *NodesBuilder) AddUpdate(existing *KeySeqLoc,
	mutation *Mutation, mutationIdx int) {
	b.NodeMutations = append(b.NodeMutations, NodeMutations{
		BaseKeySeqLoc: existing,
		MutationsBeg:  mutationIdx,
		MutationsEnd:  mutationIdx + 1,
	})
}

func (b *NodesBuilder) AddNew(mutation *Mutation, mutationIdx int) {
	if len(b.NodeMutations) <= 0 {
		b.NodeMutations = append(b.NodeMutations, NodeMutations{
			MutationsBeg: mutationIdx,
			MutationsEnd: mutationIdx + 1,
		})
	} else {
		nm := &b.NodeMutations[len(b.NodeMutations)-1]
		if nm.MutationsBeg < 0 {
			nm.MutationsBeg = mutationIdx
		}
		nm.MutationsEnd = mutationIdx + 1
	}
}

func (b *NodesBuilder) Done(mutations []Mutation, minFanOut, maxFanOut int,
	r io.ReaderAt) (KeySeqLocs, error) {
	rv := PtrKeySeqLocsArray{}

	for _, nm := range b.NodeMutations {
		if nm.MutationsBeg >= nm.MutationsEnd {
			if nm.BaseKeySeqLoc != nil {
				rv = append(rv, nm.BaseKeySeqLoc)
			}
		} else {
			childKeySeqLocs, err :=
				keySeqLocProcessMutations(nm.BaseKeySeqLoc, mutations,
					nm.MutationsBeg, nm.MutationsEnd,
					minFanOut, maxFanOut, r)
			if err != nil {
				return nil, fmt.Errorf("NodesBuilder.Done:"+
					" BaseKeySeqLoc: %#v, err: %v", nm.BaseKeySeqLoc, err)
			}
			rv = groupKeySeqLocs(childKeySeqLocs, minFanOut, maxFanOut,
				rv).(PtrKeySeqLocsArray)
		}
	}

	return rv, nil
}

// --------------------------------------------------

func rebalanceNodes(keySeqLocs KeySeqLocs,
	minFanOut, maxFanOut int) KeySeqLocs {
	// If the keySeqLocs are all nodes, then some of those nodes might
	// be much smaller than others and might benefit from rebalancing.
	var rebalanced KeySeqLocs
	var rebalancing PtrKeySeqLocsArray

	// TODO: Knowing whether those child nodes are either in-memory
	// and/or are dirty would also be helpful hints as to whether to
	// attempt some rebalancing.
	n := keySeqLocsLen(keySeqLocs)
	for i := 0; i < n; i++ {
		loc := keySeqLocs.Loc(i)
		if loc.Type != LocTypeNode || loc.node == nil {
			return keySeqLocs // TODO: Mem mgmt.
		}
		kids := loc.node.GetKeySeqLocs()
		for j := 0; j < kids.Len(); j++ {
			rebalancing = keySeqLocsAppend(rebalancing,
				kids.Key(j), kids.Seq(j), *kids.Loc(j)).(PtrKeySeqLocsArray)
			if keySeqLocsLen(rebalancing) >= maxFanOut {
				a, maxSeq := keySeqLocsSlice(rebalancing, 0, rebalancing.Len())
				rebalanced = keySeqLocsAppend(rebalanced,
					a.Key(0), maxSeq, Loc{
						Type: LocTypeNode,
						node: &NodeMem{KeySeqLocs: a},
					})
				rebalancing = nil
			}
		}
	}
	if rebalancing != nil {
		a, maxSeq := keySeqLocsSlice(rebalancing, 0, rebalancing.Len())
		rebalanced = keySeqLocsAppend(rebalanced,
			a.Key(0), maxSeq, Loc{
				Type: LocTypeNode,
				node: &NodeMem{KeySeqLocs: a},
			})
	}
	if rebalanced != nil {
		return rebalanced
	}
	return keySeqLocs
}
