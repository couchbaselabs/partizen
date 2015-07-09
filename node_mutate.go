package partizen

import (
	"fmt"
	"io"
	"sort"
)

// rootProcessMutations is the entry function for applying a batch of
// copy-on-write mutations to a tree (rootChildLoc).  The mutations
// must be ascending key ordered, and must have no duplicates.  That
// is, if the application has multiple mutations on the same key, the
// caller must provide only the last mutation for any key.  Use nil
// for rootChildLoc to start a brand new tree.
func rootProcessMutations(rootChildLoc *ChildLoc,
	mutations []Mutation, cb MutationCallback,
	minFanOut, maxFanOut int,
	reclaimables ReclaimableChildLocs,
	bufManager BufManager, r io.ReaderAt) (
	*ChildLoc, error) {
	a, err := processMutations(rootChildLoc, mutations, 0, len(mutations),
		cb, minFanOut, maxFanOut, reclaimables, bufManager, r)
	if err != nil {
		return nil, fmt.Errorf("rootProcessMutations:"+
			" rootChildLoc: %#v, err: %v", rootChildLoc, err)
	}
	if a != nil {
		// TODO: needs swizzle lock?
		for a.Len() > 1 || (a.Len() > 0 && a.Loc(0).Type == LocTypeVal) {
			a, err = groupChildLocs(a, minFanOut, maxFanOut, nil,
				bufManager, r)
			if err != nil {
				return nil, err
			}
		}
		if a.Len() > 0 {
			return a.ChildLoc(0), nil // TODO: swizzle lock?
		}
	}
	return nil, nil
}

// processMutations recursively applies the batch of mutations down
// the tree, building up copy-on-write new nodes.
func processMutations(childLoc *ChildLoc,
	mutations []Mutation,
	mbeg, mend int, // The subset [mbeg, mend) of mutations to process.
	cb MutationCallback,
	minFanOut, maxFanOut int,
	reclaimables ReclaimableChildLocs,
	bufManager BufManager, r io.ReaderAt) (
	ChildLocs, error) {
	var childLocs ChildLocs

	if childLoc != nil {
		if childLoc.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&childLoc.Loc, bufManager, r)
			if err != nil {
				return nil, fmt.Errorf("processMutations:"+
					" childLoc: %#v, err: %v", childLoc, err)
			}
			if node != nil {
				childLocs = node.GetChildLocs()
			}
		} else if childLoc.Loc.Type == LocTypeVal {
			childLocs = PtrChildLocsArray{childLoc}
		} else {
			return nil, fmt.Errorf("processMutations:"+
				" unexpected childLoc.Type, childLoc: %#v", childLoc)
		}
	}

	n := childLocsLen(childLocs)
	m := mend - mbeg

	var builder ChildLocsBuilder
	if n <= 0 || childLocs.Loc(0).Type == LocTypeVal {
		// TODO: swizzle lock?
		// TODO: mem mgmt / sync.Pool?
		builder = &ValsBuilder{
			bufManager:   bufManager,
			reclaimables: reclaimables,
			s:            make(PtrChildLocsArray, 0, m),
		}
	} else {
		builder = &NodesBuilder{
			bufManager:    bufManager,
			reclaimables:  reclaimables,
			NodeMutations: make([]NodeMutations, 0, m),
		}
	}

	if !mergeMutations(childLocs, 0, n, mutations, mbeg, mend,
		cb, bufManager, builder) {
		return nil, ErrMatchSeq
	}

	return builder.Done(mutations, cb, minFanOut, maxFanOut, bufManager, r)
}

// groupChildLocs assigns a key-ordered sequence of children to new
// parent nodes, where the parent nodes will meet the given
// minFanOut/maxFanOut.
func groupChildLocs(childChildLocs ChildLocs,
	minFanOut, maxFanOut int,
	groupedChildLocsStart ChildLocsAppendable,
	bufManager BufManager, r io.ReaderAt) (
	ChildLocs, error) {
	children, err :=
		rebalanceNodes(childChildLocs, minFanOut, maxFanOut, bufManager, r)
	if err != nil {
		return nil, err
	}

	parents := groupedChildLocsStart

	// TODO: A more optimal grouping approach would instead partition
	// the children more evenly, instead of the current approach where
	// the last group of children might be unfairly too small as it
	// has only the simple leftover remainder of children.
	n := childLocsLen(children)
	beg := 0
	for i := maxFanOut; i < n; i = i + maxFanOut {
		parents, err =
			childLocsGroupAppend(children, beg, i, parents, bufManager, r)
		if err != nil {
			return nil, err
		}

		beg = i
	}
	if beg < n { // If there were leftovers...
		if beg <= 0 { // If there were only leftovers, group them...
			parents, err =
				childLocsGroupAppend(children, beg, n, parents, bufManager, r)
			if err != nil {
				return nil, err
			}
		} else { // Pass the leftovers upwards.
			for i := beg; i < n; i++ { // TODO: swizzle lock?
				parents = parents.Append(*children.ChildLoc(i))
			}
		}
	}

	return parents, nil
}

func childLocsLen(a ChildLocs) int {
	if a == nil {
		return 0
	}
	return a.Len()
}

func childLocsSlice(a ChildLocs, from, to int) (ChildLocs, Seq) {
	ilArr := make(ChildLocsArray, to-from)
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

		ilArr[i-from] = ChildLoc{
			Key: key,
			Seq: seq,
			Loc: *(a.Loc(i)), // TODO: swizzle lock?
		}
	}

	return ilArr, maxSeq
}

func childLocsAppend(
	dst ChildLocsAppendable,
	key Key, seq Seq, loc Loc) ChildLocsAppendable {
	if dst == nil {
		// TODO: Use more capacity / sync.Pool?
		return ChildLocsArray{ChildLoc{Key: key, Seq: seq, Loc: loc}}
	}
	return dst.Append(ChildLoc{Key: key, Seq: seq, Loc: loc})
}

func childLocsGroupAppend(src ChildLocs, beg, end int, dst ChildLocsAppendable,
	bufManager BufManager, r io.ReaderAt) (
	ChildLocsAppendable, error) {
	a, maxSeq := childLocsSlice(src, beg, end)

	partitions, err := childLocsGroupByPartitionIds(a, bufManager, r)
	if err != nil {
		return nil, err
	}

	return childLocsAppend(dst,
		a.Key(0), maxSeq, Loc{
			Type: LocTypeNode,
			node: &Node{childLocs: a, partitions: partitions},
		}), nil
}

// mergeMutations zippers together a key-ordered sequence of existing
// ChildLoc's with a key-ordered sequence of mutations.
func mergeMutations(
	existings ChildLocs,
	ebeg, eend int, // Sub-range of existings[ebeg:eend] to process.
	mutations []Mutation,
	mbeg, mend int, // Sub-range of mutations[mbeg:mend] to process.
	cb MutationCallback,
	bufManager BufManager,
	builder ChildLocsBuilder) bool {
	existing, eok, ecur := nextChildLoc(ebeg, eend, existings)
	mutation, mok, mcur := nextMutation(mbeg, mend, mutations)

	for eok && mok {
		// TODO: See if binary search to skip past keys here is faster?
		c := CompareKeyItemBufRef(existing.Key, mutation.ItemBufRef, bufManager)
		if c < 0 {
			builder.AddExisting(existing)
			existing, eok, ecur = nextChildLoc(ecur+1, eend, existings)
		} else {
			if c == 0 {
				if !builder.AddUpdate(existing, mutation, mcur, cb, bufManager) {
					return false
				}
				existing, eok, ecur = nextChildLoc(ecur+1, eend, existings)
			} else {
				if !builder.AddNew(mutation, mcur, cb, bufManager) {
					return false
				}
			}
			mutation, mok, mcur = nextMutation(mcur+1, mend, mutations)
		}
	}
	for eok {
		builder.AddExisting(existing)
		existing, eok, ecur = nextChildLoc(ecur+1, eend, existings)
	}
	for mok {
		if !builder.AddNew(mutation, mcur, cb, bufManager) {
			return false
		}
		mutation, mok, mcur = nextMutation(mcur+1, mend, mutations)
	}
	return true
}

func nextChildLoc(idx, n int, childLocs ChildLocs) (
	*ChildLoc, bool, int) {
	if idx < n {
		return childLocs.ChildLoc(idx), true, idx // TODO: swizzle lock?
	}
	return &NilChildLoc, false, idx
}

func nextMutation(idx, n int, mutations []Mutation) (
	*Mutation, bool, int) {
	if idx < n {
		return &mutations[idx], true, idx
	}
	return &NilMutation, false, idx
}

// --------------------------------------------------

type ChildLocsBuilder interface {
	AddExisting(existing *ChildLoc)
	AddUpdate(existing *ChildLoc,
		mutation *Mutation, mutationIdx int,
		cb MutationCallback, bufManager BufManager) bool
	AddNew(mutation *Mutation, mutationIdx int,
		cb MutationCallback, bufManager BufManager) bool
	Done(mutations []Mutation, cb MutationCallback,
		minFanOut, maxFanOut int, bufManager BufManager, r io.ReaderAt) (
		ChildLocs, error)
}

// --------------------------------------------------

// A ValsBuilder implements the ChildLocsBuilder interface to return an
// array of LocTypeVal ChildLoc's, which can be then used as input as
// the children to create new leaf Nodes.
type ValsBuilder struct {
	bufManager   BufManager
	reclaimables ReclaimableChildLocs
	s            PtrChildLocsArray
}

func (b *ValsBuilder) AddExisting(existing *ChildLoc) {
	b.s = append(b.s, existing)
}

func (b *ValsBuilder) AddUpdate(existing *ChildLoc,
	mutation *Mutation, mutationIdx int,
	cb MutationCallback, bufManager BufManager) bool {
	if cb != nil && !cb(existing, true, mutation) {
		return false
	}

	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, mutationToValChildLoc(mutation, bufManager))

		b.reclaimables.Append(existing)
	}

	return true
}

func (b *ValsBuilder) AddNew(
	mutation *Mutation, mutationIdx int,
	cb MutationCallback, bufManager BufManager) bool {
	if cb != nil && !cb(nil, true, mutation) {
		return false
	}

	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, mutationToValChildLoc(mutation, bufManager))
	}

	return true
}

func (b *ValsBuilder) Done(mutations []Mutation, cb MutationCallback,
	minFanOut, maxFanOut int, bufManager BufManager, r io.ReaderAt) (
	ChildLocs, error) {
	return b.s, nil
}

func mutationToValChildLoc(m *Mutation, bm BufManager) *ChildLoc {
	m.ItemBufRef.AddRef(bm)

	key := FromItemBufRef(nil, true, m.ItemBufRef, bm) // TODO: mem-mgmt.

	seq := m.ItemBufRef.Seq(bm)

	size := m.ItemBufRef.Len(bm)

	return &ChildLoc{
		Key: key, // NOTE: We copy key in groupChildLocs/childLocsSlice.
		Seq: seq,
		Loc: Loc{
			Type:       LocTypeVal,
			Size:       uint32(size),
			itemBufRef: m.ItemBufRef,
		},
	}
}

// --------------------------------------------------

// An NodesBuilder implements the ChildLocsBuilder interface to return an
// array of LocTypeNode ChildLoc's, which can be then used as input as
// the children to create new interior Nodes.
type NodesBuilder struct {
	bufManager    BufManager
	reclaimables  ReclaimableChildLocs
	NodeMutations []NodeMutations
}

type NodeMutations struct {
	BaseChildLoc *ChildLoc
	MutationsBeg int // Inclusive index into []Mutation.
	MutationsEnd int // Exclusive index into []Mutation.
}

func (b *NodesBuilder) AddExisting(existing *ChildLoc) {
	b.NodeMutations = append(b.NodeMutations, NodeMutations{
		BaseChildLoc: existing,
		MutationsBeg: -1,
		MutationsEnd: -1,
	})
}

func (b *NodesBuilder) AddUpdate(existing *ChildLoc,
	mutation *Mutation, mutationIdx int,
	cb MutationCallback, bufManager BufManager) bool {
	if cb != nil && !cb(existing, false, mutation) {
		return false
	}

	b.NodeMutations = append(b.NodeMutations, NodeMutations{
		BaseChildLoc: existing,
		MutationsBeg: mutationIdx,
		MutationsEnd: mutationIdx + 1,
	})

	return true
}

func (b *NodesBuilder) AddNew(
	mutation *Mutation, mutationIdx int,
	cb MutationCallback, bufManager BufManager) bool {
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
	minFanOut, maxFanOut int, bufManager BufManager, r io.ReaderAt) (
	ChildLocs, error) {
	rv := PtrChildLocsArray{}

	for _, nm := range b.NodeMutations {
		if nm.MutationsBeg >= nm.MutationsEnd {
			if nm.BaseChildLoc != nil {
				rv = append(rv, nm.BaseChildLoc)
			}
		} else {
			children, err := processMutations(nm.BaseChildLoc,
				mutations, nm.MutationsBeg, nm.MutationsEnd,
				cb, minFanOut, maxFanOut, b.reclaimables, bufManager, r)
			if err != nil {
				return nil, fmt.Errorf("NodesBuilder.Done:"+
					" BaseChildLoc: %#v, err: %v", nm.BaseChildLoc, err)
			}

			rvx, err := groupChildLocs(children,
				minFanOut, maxFanOut, rv, bufManager, r)
			if err != nil {
				return nil, err
			}

			rv = rvx.(PtrChildLocsArray)

			b.reclaimables.Append(nm.BaseChildLoc)
		}
	}

	return rv, nil
}

// --------------------------------------------------

func rebalanceNodes(childLocs ChildLocs,
	minFanOut, maxFanOut int,
	bufManager BufManager, r io.ReaderAt) (rv ChildLocs, err error) {
	// If the childLocs are all nodes, then some of those nodes might
	// be much smaller than others and might benefit from rebalancing.
	var rebalanced ChildLocsAppendable
	var rebalancing PtrChildLocsArray

	// TODO: Knowing whether those child nodes are either in-memory
	// and/or are dirty would also be helpful hints as to whether to
	// attempt some rebalancing.
	n := childLocsLen(childLocs)
	for i := 0; i < n; i++ {
		loc := childLocs.Loc(i) // TODO: swizzle lock?
		if loc.Type != LocTypeNode || loc.node == nil {
			return childLocs, nil // TODO: Mem mgmt.
		}
		kids := loc.node.GetChildLocs()
		for j := 0; j < kids.Len(); j++ {
			// TODO: swizzle lock?
			rebalancing = childLocsAppend(rebalancing,
				kids.Key(j), kids.Seq(j), *kids.Loc(j)).(PtrChildLocsArray)
			if childLocsLen(rebalancing) >= maxFanOut {
				rebalanced, err =
					childLocsGroupAppend(rebalancing, 0, rebalancing.Len(),
						rebalanced, bufManager, r)
				if err != nil {
					return nil, err
				}

				rebalancing = nil
			}
		}
	}
	if rebalancing != nil {
		rebalanced, err =
			childLocsGroupAppend(rebalancing, 0, rebalancing.Len(),
				rebalanced, bufManager, r)
		if err != nil {
			return nil, err
		}
	}
	if rebalanced != nil {
		return rebalanced, nil
	}

	return childLocs, nil
}

// --------------------------------------------------

func childLocsGroupByPartitionIds(a ChildLocs,
	bufManager BufManager, r io.ReaderAt) (
	*Partitions, error) {
	partitionIds := make(PartitionIds, 0, a.Len())

	m := map[PartitionId][]KeyChildLoc{}
	n := a.Len()
	for i := 0; i < n; i++ {
		partitions, err := a.ChildLoc(i).GetPartitions(bufManager, r)
		if err != nil {
			return nil, err
		}

		for j, partitionId := range partitions.PartitionIds {
			prev := m[partitionId]
			if prev == nil {
				partitionIds = append(partitionIds, partitionId)
			}

			keyChildLoc := partitions.KeyChildLocs[j][0]

			m[partitionId] = append(prev, KeyChildLoc{
				Key:      keyChildLoc.Key,
				ChildLoc: keyChildLoc.ChildLoc,
			})
		}
	}

	sort.Sort(partitionIds)

	p := &Partitions{
		PartitionIds: partitionIds,
		KeyChildLocs: make([][]KeyChildLoc, len(partitionIds)),
	}

	for i := 0; i < len(partitionIds); i++ {
		p.KeyChildLocs[i] = m[partitionIds[i]]
	}

	return p, nil
}
