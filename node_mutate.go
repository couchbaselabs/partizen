package partizen

import (
	"bytes"
	"fmt"
	"io"
)

// rootNodeLocProcessMutations is the entry function for applying a
// batch of copy-on-write mutations to a tree (rootNodeLoc).  The
// mutations must be ordered by ascending key order, and must also
// have no duplicates.  That is, if the application has a sequence of
// mutations on the same key, the caller must provide only the last
// mutation for any key.  The rootNodeLoc may be nil to start off a
// brand new tree.
func rootNodeLocProcessMutations(rootNodeLoc *Loc, mutations []Mutation,
	maxFanOut int, r io.ReaderAt) (*KeySeqLoc, error) {
	keySeqLocs, err := nodeLocProcessMutations(rootNodeLoc, mutations,
		0, len(mutations), maxFanOut, r)
	if err != nil {
		return nil, fmt.Errorf("rootLocProcessMutations:"+
			" rootNodeLoc: %#v, err: %v", rootNodeLoc, err)
	}
	for len(keySeqLocs) > 1 ||
		(len(keySeqLocs) > 0 && keySeqLocs[0].Loc.Type == LocTypeVal) {
		keySeqLocs = groupKeySeqLocs(keySeqLocs, maxFanOut, nil)
	}
	if len(keySeqLocs) > 0 {
		return keySeqLocs[0], nil
	}
	return nil, nil
}

// nodeLocProcessMutations recursively applies the batch of mutations
// down the tree, building up copy-on-write new nodes.
func nodeLocProcessMutations(nodeLoc *Loc, mutations []Mutation,
	mbeg, mend int, maxFanOut int, r io.ReaderAt) (KeySeqLocs, error) {
	node, err := ReadLocNode(nodeLoc, r)
	if err != nil {
		return nil, fmt.Errorf("nodeLocProcessMutations:"+
			" nodeLoc: %#v, err: %v", nodeLoc, err)
	}

	var builder KeySeqLocsBuilder
	if node == nil || node.IsLeaf() || node.NumChildren() <= 0 {
		builder = &ValsBuilder{}
	} else {
		builder = &NodesBuilder{}
	}

	var keySeqLocs KeySeqLocs
	if node != nil {
		keySeqLocs = node.GetKeySeqLocs()
	}

	processMutations(keySeqLocs, 0, len(keySeqLocs),
		mutations, mbeg, mend, builder)

	return builder.Done(mutations, maxFanOut, r)
}

// groupKeySeqLocs assigns a key-ordered sequence of children to new
// parent nodes, where the parent nodes will meet the given maxFanOut.
func groupKeySeqLocs(childKeySeqLocs KeySeqLocs, maxFanOut int,
	groupedKeySeqLocsStart KeySeqLocs) KeySeqLocs {
	// TODO: A more optimal grouping approach would instead partition
	// the childKeySeqLocs more evenly, instead of the current approach
	// where the last group might be unfairly too small as it has only
	// the simple remainder of childKeySeqLocs.
	groupedKeySeqLocs := groupedKeySeqLocsStart
	beg := 0
	for i := maxFanOut; i < len(childKeySeqLocs); i = i + maxFanOut {
		groupedKeySeqLocs = append(groupedKeySeqLocs, &KeySeqLoc{
			Key: childKeySeqLocs[beg].Key,
			Loc: Loc{
				Type: LocTypeNode,
				node: &NodeMem{KeySeqLocs: childKeySeqLocs[beg:i]},
			},
		})
		beg = i
	}
	if beg < len(childKeySeqLocs) {
		groupedKeySeqLocs = append(groupedKeySeqLocs, &KeySeqLoc{
			Key: childKeySeqLocs[beg].Key,
			Loc: Loc{
				Type: LocTypeNode,
				node: &NodeMem{KeySeqLocs: childKeySeqLocs[beg:]},
			},
		})
	}
	return groupedKeySeqLocs
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
		return keySeqLocs[idx], true, idx
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
	Done(mutations []Mutation, maxFanOut int, r io.ReaderAt) (KeySeqLocs, error)
}

// --------------------------------------------------

// A ValsBuilder implements the KeySeqLocsBuilder interface to return an
// array of LocTypeVal KeySeqLoc's, which can be then used as input as
// the children to create new leaf Nodes.
type ValsBuilder struct {
	s KeySeqLocs
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

func (b *ValsBuilder) Done(mutations []Mutation, maxFanOut int,
	r io.ReaderAt) (KeySeqLocs, error) {
	return b.s, nil
}

func mutationToValKeySeqLoc(m *Mutation) *KeySeqLoc {
	return &KeySeqLoc{
		Key: m.Key,
		Loc: Loc{
			Type: LocTypeVal,
			Size: uint32(len(m.Val)),
			buf:  m.Val,
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
	NodeKeySeqLoc *KeySeqLoc
	MutationsBeg  int // Inclusive index into keyValOps.
	MutationsEnd  int // Exclusive index into keyValOps.
}

func (b *NodesBuilder) AddExisting(existing *KeySeqLoc) {
	b.NodeMutations = append(b.NodeMutations, NodeMutations{
		NodeKeySeqLoc: existing,
		MutationsBeg:  -1,
		MutationsEnd:  -1,
	})
}

func (b *NodesBuilder) AddUpdate(existing *KeySeqLoc,
	mutation *Mutation, mutationIdx int) {
	b.NodeMutations = append(b.NodeMutations, NodeMutations{
		NodeKeySeqLoc: existing,
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

func (b *NodesBuilder) Done(mutations []Mutation, maxFanOut int,
	r io.ReaderAt) (KeySeqLocs, error) {
	var rv KeySeqLocs

	for _, nm := range b.NodeMutations {
		if nm.MutationsBeg >= nm.MutationsEnd {
			if nm.NodeKeySeqLoc != nil {
				rv = append(rv, nm.NodeKeySeqLoc)
			}
		} else {
			childKeySeqLocs, err :=
				nodeLocProcessMutations(&nm.NodeKeySeqLoc.Loc, mutations,
					nm.MutationsBeg, nm.MutationsEnd, maxFanOut, r)
			if err != nil {
				return nil, fmt.Errorf("NodesBuilder.Done:"+
					" NodeKeySeqLoc: %#v, err: %v", nm.NodeKeySeqLoc, err)
			}
			rv = groupKeySeqLocs(childKeySeqLocs, maxFanOut, rv)
		}
	}

	return rv, nil
}
