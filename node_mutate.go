package partizen

import (
	"bytes"
	"fmt"
	"io"
	"sort"
)

// rootProcessMutations is the entry function for applying a batch of
// copy-on-write mutations to a tree (rootKeySeqLoc).  The mutations
// must be ascending key ordered, and must have no duplicates.  That
// is, if the application has multiple mutations on the same key, the
// caller must provide only the last mutation for any key.  Use nil
// for rootKeySeqLoc to start a brand new tree.
func rootProcessMutations(rootKeySeqLoc *KeySeqLoc,
	mutations []Mutation, cb MutationCallback,
	minFanOut, maxFanOut int,
	reclaimables ReclaimableKeySeqLocs,
	bufManager BufManager, r io.ReaderAt) (
	*KeySeqLoc, error) {
	a, err := processMutations(rootKeySeqLoc, mutations, 0, len(mutations),
		cb, minFanOut, maxFanOut, reclaimables, bufManager, r)
	if err != nil {
		return nil, fmt.Errorf("rootProcessMutations:"+
			" rootKeySeqLoc: %#v, err: %v", rootKeySeqLoc, err)
	}
	if a != nil {
		// TODO: needs swizzle lock?
		for a.Len() > 1 || (a.Len() > 0 && a.Loc(0).Type == LocTypeVal) {
			a, err = groupKeySeqLocs(a, minFanOut, maxFanOut, nil,
				bufManager, r)
			if err != nil {
				return nil, err
			}
		}
		if a.Len() > 0 {
			return a.KeySeqLoc(0), nil // TODO: swizzle lock?
		}
	}
	return nil, nil
}

// processMutations recursively applies the batch of mutations down
// the tree, building up copy-on-write new nodes.
func processMutations(ksLoc *KeySeqLoc,
	mutations []Mutation,
	mbeg, mend int, // The subset [mbeg, mend) of mutations to process.
	cb MutationCallback,
	minFanOut, maxFanOut int,
	reclaimables ReclaimableKeySeqLocs,
	bufManager BufManager, r io.ReaderAt) (
	KeySeqLocs, error) {
	var ksLocs KeySeqLocs

	if ksLoc != nil {
		if ksLoc.Loc.Type == LocTypeNode {
			node, err := ReadLocNode(&ksLoc.Loc, bufManager, r)
			if err != nil {
				return nil, fmt.Errorf("processMutations:"+
					" ksLoc: %#v, err: %v", ksLoc, err)
			}
			if node != nil {
				ksLocs = node.GetKeySeqLocs()
			}
		} else if ksLoc.Loc.Type == LocTypeVal {
			ksLocs = PtrKeySeqLocsArray{ksLoc}
		} else {
			return nil, fmt.Errorf("processMutations:"+
				" unexpected ksLoc.Type, ksLoc: %#v", ksLoc)
		}
	}

	n := ksLocsLen(ksLocs)
	m := mend - mbeg

	var builder KeySeqLocsBuilder
	if n <= 0 || ksLocs.Loc(0).Type == LocTypeVal {
		// TODO: swizzle lock?
		// TODO: mem mgmt / sync.Pool?
		builder = &ValsBuilder{
			bufManager:   bufManager,
			reclaimables: reclaimables,
			s:            make(PtrKeySeqLocsArray, 0, m),
		}
	} else {
		builder = &NodesBuilder{
			bufManager:    bufManager,
			reclaimables:  reclaimables,
			NodeMutations: make([]NodeMutations, 0, m),
		}
	}

	if !mergeMutations(ksLocs, 0, n, mutations, mbeg, mend,
		cb, bufManager, builder) {
		return nil, ErrMatchSeq
	}

	return builder.Done(mutations, cb, minFanOut, maxFanOut, bufManager, r)
}

// groupKeySeqLocs assigns a key-ordered sequence of children to new
// parent nodes, where the parent nodes will meet the given maxFanOut.
func groupKeySeqLocs(childKeySeqLocs KeySeqLocs,
	minFanOut, maxFanOut int,
	groupedKeySeqLocsStart KeySeqLocsAppendable,
	bufManager BufManager, r io.ReaderAt) (
	KeySeqLocs, error) {
	children, err :=
		rebalanceNodes(childKeySeqLocs, minFanOut, maxFanOut, bufManager, r)
	if err != nil {
		return nil, err
	}

	parents := groupedKeySeqLocsStart

	// TODO: A more optimal grouping approach would instead partition
	// the children more evenly, instead of the current approach where
	// the last group of children might be unfairly too small as it
	// has only the simple leftover remainder of children.
	n := ksLocsLen(children)
	beg := 0
	for i := maxFanOut; i < n; i = i + maxFanOut {
		parents, err =
			ksLocsGroupAppend(children, beg, i, parents, bufManager, r)
		if err != nil {
			return nil, err
		}

		beg = i
	}
	if beg < n { // If there were leftovers...
		if beg <= 0 { // If there were only leftovers, group them...
			parents, err =
				ksLocsGroupAppend(children, beg, n, parents, bufManager, r)
			if err != nil {
				return nil, err
			}
		} else { // Pass the leftovers upwards.
			for i := beg; i < n; i++ { // TODO: swizzle lock?
				parents = parents.Append(*children.KeySeqLoc(i))
			}
		}
	}

	return parents, nil
}

func ksLocsLen(a KeySeqLocs) int {
	if a == nil {
		return 0
	}
	return a.Len()
}

func ksLocsSlice(a KeySeqLocs, from, to int) (KeySeqLocs, Seq) {
	ilArr := make(KeySeqLocsArray, to-from)
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

		ilArr[i-from] = KeySeqLoc{
			Key: key,
			Seq: seq,
			Loc: *(a.Loc(i)), // TODO: swizzle lock?
		}
	}

	return ilArr, maxSeq
}

func ksLocsAppend(
	dst KeySeqLocsAppendable,
	key Key, seq Seq, loc Loc) KeySeqLocsAppendable {
	if dst == nil {
		return KeySeqLocsArray{KeySeqLoc{Key: key, Seq: seq, Loc: loc}}
	}
	return dst.Append(KeySeqLoc{Key: key, Seq: seq, Loc: loc})
}

func ksLocsGroupAppend(src KeySeqLocs, beg, end int, dst KeySeqLocsAppendable,
	bufManager BufManager, r io.ReaderAt) (
	KeySeqLocsAppendable, error) {
	a, maxSeq := ksLocsSlice(src, beg, end)

	partitions, err := ksLocsGroupByPartitionIds(a, bufManager, r)
	if err != nil {
		return nil, err
	}

	return ksLocsAppend(dst,
		a.Key(0), maxSeq, Loc{
			Type: LocTypeNode,
			node: &Node{ksLocs: a, partitions: partitions},
		}), nil
}

// mergeMutations zippers together a key-ordered sequence of existing
// KeySeqLoc's with a key-ordered sequence of mutations.
func mergeMutations(
	existings KeySeqLocs,
	ebeg, eend int, // Sub-range of existings[ebeg:eend] to process.
	mutations []Mutation,
	mbeg, mend int, // Sub-range of mutations[mbeg:mend] to process.
	cb MutationCallback,
	bufManager BufManager,
	builder KeySeqLocsBuilder) bool {
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
				if !builder.AddUpdate(existing, mutation, mcur, cb, bufManager) {
					return false
				}
				existing, eok, ecur = nextKeySeqLoc(ecur+1, eend, existings)
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
		existing, eok, ecur = nextKeySeqLoc(ecur+1, eend, existings)
	}
	for mok {
		if !builder.AddNew(mutation, mcur, cb, bufManager) {
			return false
		}
		mutation, mok, mcur = nextMutation(mcur+1, mend, mutations)
	}
	return true
}

func nextKeySeqLoc(idx, n int, ksLocs KeySeqLocs) (
	*KeySeqLoc, bool, int) {
	if idx < n {
		return ksLocs.KeySeqLoc(idx), true, idx // TODO: swizzle lock?
	}
	return &NilKeySeqLoc, false, idx
}

func nextMutation(idx, n int, mutations []Mutation) (
	*Mutation, bool, int) {
	if idx < n {
		return &mutations[idx], true, idx
	}
	return &NilMutation, false, idx
}

// --------------------------------------------------

type KeySeqLocsBuilder interface {
	AddExisting(existing *KeySeqLoc)
	AddUpdate(existing *KeySeqLoc,
		mutation *Mutation, mutationIdx int,
		cb MutationCallback, bufManager BufManager) bool
	AddNew(mutation *Mutation, mutationIdx int,
		cb MutationCallback, bufManager BufManager) bool
	Done(mutations []Mutation, cb MutationCallback,
		minFanOut, maxFanOut int, bufManager BufManager, r io.ReaderAt) (
		KeySeqLocs, error)
}

// --------------------------------------------------

// A ValsBuilder implements the KeySeqLocsBuilder interface to return an
// array of LocTypeVal KeySeqLoc's, which can be then used as input as
// the children to create new leaf Nodes.
type ValsBuilder struct {
	bufManager   BufManager
	reclaimables ReclaimableKeySeqLocs
	s            PtrKeySeqLocsArray
}

func (b *ValsBuilder) AddExisting(existing *KeySeqLoc) {
	b.s = append(b.s, existing)
}

func (b *ValsBuilder) AddUpdate(existing *KeySeqLoc,
	mutation *Mutation, mutationIdx int,
	cb MutationCallback, bufManager BufManager) bool {
	if cb != nil && !cb(existing, true, mutation) {
		return false
	}

	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, mutationToValKeySeqLoc(mutation, bufManager))

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
		b.s = append(b.s, mutationToValKeySeqLoc(mutation, bufManager))
	}

	return true
}

func (b *ValsBuilder) Done(mutations []Mutation, cb MutationCallback,
	minFanOut, maxFanOut int, bufManager BufManager, r io.ReaderAt) (
	KeySeqLocs, error) {
	return b.s, nil
}

func mutationToValKeySeqLoc(m *Mutation, bufManager BufManager) *KeySeqLoc {
	m.ValBufRef.AddRef(bufManager)

	bufLen := m.ValBufRef.Len(bufManager)

	return &KeySeqLoc{
		Key: m.Key, // NOTE: We copy key in groupKeySeqLocs/ksLocsSlice.
		Seq: m.Seq,
		Loc: Loc{
			Type:            LocTypeVal,
			Size:            uint32(bufLen),
			leafValBufRef:   m.ValBufRef,
			leafPartitionId: m.PartitionId,
		},
	}
}

// --------------------------------------------------

// An NodesBuilder implements the KeySeqLocsBuilder interface to return an
// array of LocTypeNode KeySeqLoc's, which can be then used as input as
// the children to create new interior Nodes.
type NodesBuilder struct {
	bufManager    BufManager
	reclaimables  ReclaimableKeySeqLocs
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
	mutation *Mutation, mutationIdx int,
	cb MutationCallback, bufManager BufManager) bool {
	if cb != nil && !cb(existing, false, mutation) {
		return false
	}

	b.NodeMutations = append(b.NodeMutations, NodeMutations{
		BaseKeySeqLoc: existing,
		MutationsBeg:  mutationIdx,
		MutationsEnd:  mutationIdx + 1,
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
	KeySeqLocs, error) {
	rv := PtrKeySeqLocsArray{}

	for _, nm := range b.NodeMutations {
		if nm.MutationsBeg >= nm.MutationsEnd {
			if nm.BaseKeySeqLoc != nil {
				rv = append(rv, nm.BaseKeySeqLoc)
			}
		} else {
			children, err := processMutations(nm.BaseKeySeqLoc,
				mutations, nm.MutationsBeg, nm.MutationsEnd,
				cb, minFanOut, maxFanOut, b.reclaimables, bufManager, r)
			if err != nil {
				return nil, fmt.Errorf("NodesBuilder.Done:"+
					" BaseKeySeqLoc: %#v, err: %v", nm.BaseKeySeqLoc, err)
			}

			rvx, err := groupKeySeqLocs(children,
				minFanOut, maxFanOut, rv, bufManager, r)
			if err != nil {
				return nil, err
			}

			rv = rvx.(PtrKeySeqLocsArray)

			b.reclaimables.Append(nm.BaseKeySeqLoc)
		}
	}

	return rv, nil
}

// --------------------------------------------------

func rebalanceNodes(ksLocs KeySeqLocs,
	minFanOut, maxFanOut int,
	bufManager BufManager, r io.ReaderAt) (rv KeySeqLocs, err error) {
	// If the ksLocs are all nodes, then some of those nodes might
	// be much smaller than others and might benefit from rebalancing.
	var rebalanced KeySeqLocsAppendable
	var rebalancing PtrKeySeqLocsArray

	// TODO: Knowing whether those child nodes are either in-memory
	// and/or are dirty would also be helpful hints as to whether to
	// attempt some rebalancing.
	n := ksLocsLen(ksLocs)
	for i := 0; i < n; i++ {
		loc := ksLocs.Loc(i) // TODO: swizzle lock?
		if loc.Type != LocTypeNode || loc.node == nil {
			return ksLocs, nil // TODO: Mem mgmt.
		}
		kids := loc.node.GetKeySeqLocs()
		for j := 0; j < kids.Len(); j++ {
			// TODO: swizzle lock?
			rebalancing = ksLocsAppend(rebalancing,
				kids.Key(j), kids.Seq(j), *kids.Loc(j)).(PtrKeySeqLocsArray)
			if ksLocsLen(rebalancing) >= maxFanOut {
				rebalanced, err =
					ksLocsGroupAppend(rebalancing, 0, rebalancing.Len(),
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
			ksLocsGroupAppend(rebalancing, 0, rebalancing.Len(),
				rebalanced, bufManager, r)
		if err != nil {
			return nil, err
		}
	}
	if rebalanced != nil {
		return rebalanced, nil
	}

	return ksLocs, nil
}

// --------------------------------------------------

func ksLocsGroupByPartitionIds(a KeySeqLocs,
	bufManager BufManager, r io.ReaderAt) (
	*Partitions, error) {
	partitionIds := make(PartitionIds, 0, a.Len())

	m := map[PartitionId][]KeyKeySeqLoc{}
	n := a.Len()
	for i := 0; i < n; i++ {
		partitions, err := a.KeySeqLoc(i).GetPartitions(bufManager, r)
		if err != nil {
			return nil, err
		}

		for j, partitionId := range partitions.PartitionIds {
			prev := m[partitionId]
			if prev == nil {
				partitionIds = append(partitionIds, partitionId)
			}

			keyKeySeqLoc := partitions.KeyKeySeqLocs[j][0]

			m[partitionId] = append(prev, KeyKeySeqLoc{
				Key:       keyKeySeqLoc.Key,
				KeySeqLoc: keyKeySeqLoc.KeySeqLoc,
			})
		}
	}

	sort.Sort(partitionIds)

	p := &Partitions{
		PartitionIds:  partitionIds,
		KeyKeySeqLocs: make([][]KeyKeySeqLoc, len(partitionIds)),
	}

	for i := 0; i < len(partitionIds); i++ {
		p.KeyKeySeqLocs[i] = m[partitionIds[i]]
	}

	return p, nil
}
