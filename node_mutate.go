package partizen

import (
	"bytes"
	"fmt"
	"io"
)

// rootProcessMutations is the entry function for applying a batch of
// copy-on-write mutations to a tree (rootItemLoc).  The mutations
// must be ascending key ordered, and must have no duplicates.  That
// is, if the application has multiple mutations on the same key, the
// caller must provide only the last mutation for any key.  Use nil
// for rootItemLoc to start a brand new tree.
func rootProcessMutations(rootItemLoc *ItemLoc, mutations []Mutation,
	cb MutationCallback, minFanOut, maxFanOut int, r io.ReaderAt) (
	*ItemLoc, error) {
	a, err := processMutations(rootItemLoc, mutations, 0, len(mutations),
		cb, minFanOut, maxFanOut, r)
	if err != nil {
		return nil, fmt.Errorf("rootProcessMutations:"+
			" rootItemLoc: %#v, err: %v", rootItemLoc, err)
	}
	if a != nil {
		for a.Len() > 1 || (a.Len() > 0 && a.Loc(0).Type == LocTypeVal) {
			a = groupItemLocs(a, minFanOut, maxFanOut, nil)
		}
		if a.Len() > 0 {
			return a.ItemLoc(0), nil
		}
	}
	return nil, nil
}

// processMutations recursively applies the batch of
// mutations down the tree, building up copy-on-write new nodes.
func processMutations(itemLoc *ItemLoc,
	mutations []Mutation, mbeg, mend int, cb MutationCallback,
	minFanOut, maxFanOut int, r io.ReaderAt) (ItemLocs, error) {
	var itemLocs ItemLocs

	if itemLoc != nil {
		if itemLoc.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&itemLoc.Loc, r)
			if err != nil {
				return nil, fmt.Errorf("processMutations:"+
					" itemLoc: %#v, err: %v", itemLoc, err)
			}
			if node != nil {
				itemLocs = node.GetItemLocs()
			}
		} else if itemLoc.Loc.Type == LocTypeVal {
			itemLocs = PtrItemLocsArray{itemLoc}
		} else {
			return nil, fmt.Errorf("processMutations:"+
				" unexpected itemLoc.Type, itemLoc: %#v", itemLoc)
		}
	}

	n := itemLocsLen(itemLocs)
	m := mend - mbeg

	var builder ItemLocsBuilder
	if n <= 0 || itemLocs.Loc(0).Type == LocTypeVal {
		// TODO: Mem mgmt / sync.Pool?
		builder = &ValsBuilder{s: make(PtrItemLocsArray, 0, m)}
	} else {
		builder = &NodesBuilder{NodeMutations: make([]NodeMutations, 0, m)}
	}

	if !mergeMutations(itemLocs, 0, n, mutations, mbeg, mend, cb, builder) {
		return nil, ErrMatchSeq
	}

	return builder.Done(mutations, cb, minFanOut, maxFanOut, r)
}

// groupItemLocs assigns a key-ordered sequence of children to new
// parent nodes, where the parent nodes will meet the given maxFanOut.
func groupItemLocs(childItemLocs ItemLocs,
	minFanOut, maxFanOut int, groupedItemLocsStart ItemLocs) ItemLocs {
	parents := groupedItemLocsStart
	children := rebalanceNodes(childItemLocs, minFanOut, maxFanOut)

	// TODO: A more optimal grouping approach would instead partition
	// the children more evenly, instead of the current approach where
	// the last group of children might be unfairly too small as it
	// has only the simple leftover remainder of children.
	n := itemLocsLen(children)
	beg := 0
	for i := maxFanOut; i < n; i = i + maxFanOut {
		a, maxSeq := itemLocsSlice(children, beg, i)
		parents = itemLocsAppend(parents,
			a.Key(0), maxSeq, Loc{
				Type: LocTypeNode,
				node: &NodeMem{ItemLocs: a},
			})
		beg = i
	}
	if beg < n { // If there were leftovers...
		if beg <= 0 { // If there were only leftovers, group them...
			a, maxSeq := itemLocsSlice(children, beg, n)
			parents = itemLocsAppend(parents,
				a.Key(0), maxSeq, Loc{
					Type: LocTypeNode,
					node: &NodeMem{ItemLocs: a},
				})
		} else { // Pass the leftovers upwards.
			for i := beg; i < n; i++ {
				parents = parents.Append(*children.ItemLoc(i))
			}
		}
	}

	return parents
}

func itemLocsLen(a ItemLocs) int {
	if a == nil {
		return 0
	}
	return a.Len()
}

func itemLocsSlice(a ItemLocs, from, to int) (ItemLocs, Seq) {
	kslArr := make(ItemLocsArray, to-from)
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

		kslArr[i-from] = ItemLoc{
			Key: key,
			Seq: seq,
			Loc: *(a.Loc(i)), // TODO: access of loc.node needs lock?
		}
	}

	return kslArr, maxSeq
}

func itemLocsAppend(g ItemLocs, key Key, seq Seq, loc Loc) ItemLocs {
	if g == nil {
		return ItemLocsArray{ItemLoc{Key: key, Seq: seq, Loc: loc}}
	}
	return g.Append(ItemLoc{Key: key, Seq: seq, Loc: loc})
}

// mergeMutations zippers together a key-ordered sequence of existing
// ItemLoc's with a key-ordered sequence of mutations.
func mergeMutations(
	existings ItemLocs,
	ebeg, eend int, // Sub-range of existings[ebeg:eend] to process.
	mutations []Mutation,
	mbeg, mend int, // Sub-range of mutations[mbeg:mend] to process.
	cb MutationCallback,
	builder ItemLocsBuilder) bool {
	existing, eok, ecur := nextItemLoc(ebeg, eend, existings)
	mutation, mok, mcur := nextMutation(mbeg, mend, mutations)

	for eok && mok {
		// TODO: See if binary search to skip past keys here is faster?
		c := bytes.Compare(existing.Key, mutation.Key)
		if c < 0 {
			builder.AddExisting(existing)
			existing, eok, ecur = nextItemLoc(ecur+1, eend, existings)
		} else {
			if c == 0 {
				if !builder.AddUpdate(existing, mutation, mcur, cb) {
					return false
				}
				existing, eok, ecur = nextItemLoc(ecur+1, eend, existings)
			} else {
				if !builder.AddNew(mutation, mcur, cb) {
					return false
				}
			}
			mutation, mok, mcur = nextMutation(mcur+1, mend, mutations)
		}
	}
	for eok {
		builder.AddExisting(existing)
		existing, eok, ecur = nextItemLoc(ecur+1, eend, existings)
	}
	for mok {
		if !builder.AddNew(mutation, mcur, cb) {
			return false
		}
		mutation, mok, mcur = nextMutation(mcur+1, mend, mutations)
	}
	return true
}

func nextItemLoc(idx, n int, itemLocs ItemLocs) (
	*ItemLoc, bool, int) {
	if idx < n {
		return itemLocs.ItemLoc(idx), true, idx
	}
	return &zeroItemLoc, false, idx
}

func nextMutation(idx, n int, mutations []Mutation) (
	*Mutation, bool, int) {
	if idx < n {
		return &mutations[idx], true, idx
	}
	return &zeroMutation, false, idx
}

// --------------------------------------------------

type ItemLocsBuilder interface {
	AddExisting(existing *ItemLoc)
	AddUpdate(existing *ItemLoc,
		mutation *Mutation, mutationIdx int,
		cb MutationCallback) bool
	AddNew(mutation *Mutation, mutationIdx int,
		cb MutationCallback) bool
	Done(mutations []Mutation, cb MutationCallback,
		minFanOut, maxFanOut int, r io.ReaderAt) (ItemLocs, error)
}

// --------------------------------------------------

// A ValsBuilder implements the ItemLocsBuilder interface to return an
// array of LocTypeVal ItemLoc's, which can be then used as input as
// the children to create new leaf Nodes.
type ValsBuilder struct {
	s PtrItemLocsArray
}

func (b *ValsBuilder) AddExisting(existing *ItemLoc) {
	b.s = append(b.s, existing)
}

func (b *ValsBuilder) AddUpdate(existing *ItemLoc,
	mutation *Mutation, mutationIdx int, cb MutationCallback) bool {
	if cb != nil && !cb(existing, true, mutation) {
		return false
	}
	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, mutationToValItemLoc(mutation))
	}
	return true
}

func (b *ValsBuilder) AddNew(
	mutation *Mutation, mutationIdx int, cb MutationCallback) bool {
	if cb != nil && !cb(nil, true, mutation) {
		return false
	}
	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, mutationToValItemLoc(mutation))
	}
	return true
}

func (b *ValsBuilder) Done(mutations []Mutation, cb MutationCallback,
	minFanOut, maxFanOut int, r io.ReaderAt) (ItemLocs, error) {
	return b.s, nil
}

func mutationToValItemLoc(m *Mutation) *ItemLoc {
	return &ItemLoc{
		Key: m.Key, // NOTE: We copy key in groupItemLocs/itemLocsSlice.
		Seq: m.Seq,
		Loc: Loc{
			Type: LocTypeVal,
			Size: uint32(len(m.Val)),

			// TODO: Memory mgmt.
			buf:         append([]byte(nil), m.Val...),
			partitionId: m.PartitionId,
		},
	}
}

// --------------------------------------------------

// An NodesBuilder implements the ItemLocsBuilder interface to return an
// array of LocTypeNode ItemLoc's, which can be then used as input as
// the children to create new interior Nodes.
type NodesBuilder struct {
	NodeMutations []NodeMutations
}

type NodeMutations struct {
	BaseItemLoc  *ItemLoc
	MutationsBeg int // Inclusive index into []Mutation.
	MutationsEnd int // Exclusive index into []Mutation.
}

func (b *NodesBuilder) AddExisting(existing *ItemLoc) {
	b.NodeMutations = append(b.NodeMutations, NodeMutations{
		BaseItemLoc:  existing,
		MutationsBeg: -1,
		MutationsEnd: -1,
	})
}

func (b *NodesBuilder) AddUpdate(existing *ItemLoc,
	mutation *Mutation, mutationIdx int, cb MutationCallback) bool {
	if cb != nil && !cb(existing, false, mutation) {
		return false
	}
	b.NodeMutations = append(b.NodeMutations, NodeMutations{
		BaseItemLoc:  existing,
		MutationsBeg: mutationIdx,
		MutationsEnd: mutationIdx + 1,
	})
	return true
}

func (b *NodesBuilder) AddNew(
	mutation *Mutation, mutationIdx int, cb MutationCallback) bool {
	if cb != nil && !cb(nil, false, mutation) {
		return false
	}
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
	return true
}

func (b *NodesBuilder) Done(mutations []Mutation, cb MutationCallback,
	minFanOut, maxFanOut int, r io.ReaderAt) (ItemLocs, error) {
	rv := PtrItemLocsArray{}

	for _, nm := range b.NodeMutations {
		if nm.MutationsBeg >= nm.MutationsEnd {
			if nm.BaseItemLoc != nil {
				rv = append(rv, nm.BaseItemLoc)
			}
		} else {
			children, err := processMutations(nm.BaseItemLoc,
				mutations, nm.MutationsBeg, nm.MutationsEnd,
				cb, minFanOut, maxFanOut, r)
			if err != nil {
				return nil, fmt.Errorf("NodesBuilder.Done:"+
					" BaseItemLoc: %#v, err: %v", nm.BaseItemLoc, err)
			}
			rv = groupItemLocs(children,
				minFanOut, maxFanOut, rv).(PtrItemLocsArray)
		}
	}

	return rv, nil
}

// --------------------------------------------------

func rebalanceNodes(itemLocs ItemLocs,
	minFanOut, maxFanOut int) ItemLocs {
	// If the itemLocs are all nodes, then some of those nodes might
	// be much smaller than others and might benefit from rebalancing.
	var rebalanced ItemLocs
	var rebalancing PtrItemLocsArray

	// TODO: Knowing whether those child nodes are either in-memory
	// and/or are dirty would also be helpful hints as to whether to
	// attempt some rebalancing.
	n := itemLocsLen(itemLocs)
	for i := 0; i < n; i++ {
		loc := itemLocs.Loc(i)
		if loc.Type != LocTypeNode || loc.node == nil {
			return itemLocs // TODO: Mem mgmt.
		}
		kids := loc.node.GetItemLocs()
		for j := 0; j < kids.Len(); j++ {
			rebalancing = itemLocsAppend(rebalancing,
				kids.Key(j), kids.Seq(j), *kids.Loc(j)).(PtrItemLocsArray)
			if itemLocsLen(rebalancing) >= maxFanOut {
				a, maxSeq := itemLocsSlice(rebalancing, 0, rebalancing.Len())
				rebalanced = itemLocsAppend(rebalanced,
					a.Key(0), maxSeq, Loc{
						Type: LocTypeNode,
						node: &NodeMem{ItemLocs: a},
					})
				rebalancing = nil
			}
		}
	}
	if rebalancing != nil {
		a, maxSeq := itemLocsSlice(rebalancing, 0, rebalancing.Len())
		rebalanced = itemLocsAppend(rebalanced,
			a.Key(0), maxSeq, Loc{
				Type: LocTypeNode,
				node: &NodeMem{ItemLocs: a},
			})
	}
	if rebalanced != nil {
		return rebalanced
	}
	return itemLocs
}
