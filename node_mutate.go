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
	maxFanOut int, r io.ReaderAt) (*KeyLoc, error) {
	keyLocs, err := nodeLocProcessMutations(rootNodeLoc, mutations,
		0, len(mutations), maxFanOut, r)
	if err != nil {
		return nil, fmt.Errorf("rootLocProcessMutations:"+
			" rootNodeLoc: %#v, err: %v", rootNodeLoc, err)
	}
	for len(keyLocs) > 1 ||
		(len(keyLocs) > 0 && keyLocs[0].Loc.Type == LocTypeVal) {
		keyLocs = groupKeyLocs(keyLocs, maxFanOut, nil)
	}
	if len(keyLocs) > 0 {
		return keyLocs[0], nil
	}
	return nil, nil
}

// nodeLocProcessMutations recursively applies the batch of mutations
// down the tree, building up copy-on-write new nodes.
func nodeLocProcessMutations(nodeLoc *Loc, mutations []Mutation,
	mbeg, mend int, maxFanOut int, r io.ReaderAt) (KeyLocs, error) {
	node, err := ReadLocNode(nodeLoc, r)
	if err != nil {
		return nil, fmt.Errorf("nodeLocProcessMutations:"+
			" nodeLoc: %#v, err: %v", nodeLoc, err)
	}

	var builder KeyLocsBuilder
	if node == nil || node.IsLeaf() || node.NumChildren() <= 0 {
		builder = &ValsBuilder{}
	} else {
		builder = &NodesBuilder{}
	}

	var keyLocs KeyLocs
	if node != nil {
		keyLocs = node.GetKeyLocs()
	}

	processMutations(keyLocs, 0, len(keyLocs),
		mutations, mbeg, mend, builder)

	return builder.Done(mutations, maxFanOut, r)
}

// groupKeyLocs assigns a key-ordered sequence of children to new
// parent nodes, where the parent nodes will meet the given maxFanOut.
func groupKeyLocs(childKeyLocs KeyLocs, maxFanOut int,
	groupedKeyLocsStart KeyLocs) KeyLocs {
	// TODO: A more optimal grouping approach would instead partition
	// the childKeyLocs more evenly, instead of the current approach
	// where the last group might be unfairly too small as it has only
	// the simple remainder of childKeyLocs.
	groupedKeyLocs := groupedKeyLocsStart
	beg := 0
	for i := maxFanOut; i < len(childKeyLocs); i = i + maxFanOut {
		groupedKeyLocs = append(groupedKeyLocs, &KeyLoc{
			Key: childKeyLocs[beg].Key,
			Loc: Loc{
				Type: LocTypeNode,
				node: &NodeMem{KeyLocs: childKeyLocs[beg:i]},
			},
		})
		beg = i
	}
	if beg < len(childKeyLocs) {
		groupedKeyLocs = append(groupedKeyLocs, &KeyLoc{
			Key: childKeyLocs[beg].Key,
			Loc: Loc{
				Type: LocTypeNode,
				node: &NodeMem{KeyLocs: childKeyLocs[beg:]},
			},
		})
	}
	return groupedKeyLocs
}

// processMutations merges or zippers together a key-ordered sequence
// of existing KeyLoc's with a key-ordered batch of mutations.
func processMutations(
	existings KeyLocs,
	ebeg, eend int, // Sub-range of existings[ebeg:eend] to process.
	mutations []Mutation,
	mbeg, mend int, // Sub-range of mutations[mbeg:mend] to process.
	builder KeyLocsBuilder) {
	existing, eok, ecur := nextKeyLoc(ebeg, eend, existings)
	mutation, mok, mcur := nextMutation(mbeg, mend, mutations)

	for eok && mok {
		c := bytes.Compare(existing.Key, mutation.Key)
		if c < 0 {
			builder.AddExisting(existing)
			existing, eok, ecur = nextKeyLoc(ecur+1, eend, existings)
		} else {
			if c == 0 {
				builder.AddUpdate(existing, mutation, mcur)
				existing, eok, ecur = nextKeyLoc(ecur+1, eend, existings)
			} else {
				builder.AddNew(mutation, mcur)
			}
			mutation, mok, mcur = nextMutation(mcur+1, mend, mutations)
		}
	}
	for eok {
		builder.AddExisting(existing)
		existing, eok, ecur = nextKeyLoc(ecur+1, eend, existings)
	}
	for mok {
		builder.AddNew(mutation, mcur)
		mutation, mok, mcur = nextMutation(mcur+1, mend, mutations)
	}
}

func nextKeyLoc(idx, n int, keyLocs KeyLocs) (
	*KeyLoc, bool, int) {
	if idx < n {
		return keyLocs[idx], true, idx
	}
	return &zeroKeyLoc, false, idx
}

func nextMutation(idx, n int, mutations []Mutation) (
	*Mutation, bool, int) {
	if idx < n {
		return &mutations[idx], true, idx
	}
	return &zeroMutation, false, idx
}

// --------------------------------------------------

type KeyLocsBuilder interface {
	AddExisting(existing *KeyLoc)
	AddUpdate(existing *KeyLoc, mutation *Mutation, mutationIdx int)
	AddNew(mutation *Mutation, mutationIdx int)
	Done(mutations []Mutation, maxFanOut int, r io.ReaderAt) (KeyLocs, error)
}

// --------------------------------------------------

// A ValsBuilder implements the KeyLocsBuilder interface to return an
// array of LocTypeVal KeyLoc's, which can be then used as input as
// the children to create new leaf Nodes.
type ValsBuilder struct {
	s KeyLocs
}

func (b *ValsBuilder) AddExisting(existing *KeyLoc) {
	b.s = append(b.s, existing)
}

func (b *ValsBuilder) AddUpdate(existing *KeyLoc,
	mutation *Mutation, mutationIdx int) {
	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, mutationToValKeyLoc(mutation))
	}
}

func (b *ValsBuilder) AddNew(mutation *Mutation, mutationIdx int) {
	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, mutationToValKeyLoc(mutation))
	}
}

func (b *ValsBuilder) Done(mutations []Mutation, maxFanOut int,
	r io.ReaderAt) (KeyLocs, error) {
	return b.s, nil
}

func mutationToValKeyLoc(m *Mutation) *KeyLoc {
	return &KeyLoc{
		Key: m.Key,
		Loc: Loc{
			Type: LocTypeVal,
			Size: uint32(len(m.Val)),
			buf:  m.Val,
		},
	}
}

// --------------------------------------------------

// An NodesBuilder implements the KeyLocsBuilder interface to return an
// array of LocTypeNode KeyLoc's, which can be then used as input as
// the children to create new interior Nodes.
type NodesBuilder struct {
	NodeMutations []NodeMutations
}

type NodeMutations struct {
	NodeKeyLoc   *KeyLoc
	MutationsBeg int // Inclusive index into keyValOps.
	MutationsEnd int // Exclusive index into keyValOps.
}

func (b *NodesBuilder) AddExisting(existing *KeyLoc) {
	b.NodeMutations = append(b.NodeMutations, NodeMutations{
		NodeKeyLoc:   existing,
		MutationsBeg: -1,
		MutationsEnd: -1,
	})
}

func (b *NodesBuilder) AddUpdate(existing *KeyLoc,
	mutation *Mutation, mutationIdx int) {
	b.NodeMutations = append(b.NodeMutations, NodeMutations{
		NodeKeyLoc:   existing,
		MutationsBeg: mutationIdx,
		MutationsEnd: mutationIdx + 1,
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
	r io.ReaderAt) (KeyLocs, error) {
	var rv KeyLocs

	for _, nm := range b.NodeMutations {
		if nm.MutationsBeg >= nm.MutationsEnd {
			if nm.NodeKeyLoc != nil {
				rv = append(rv, nm.NodeKeyLoc)
			}
		} else {
			childKeyLocs, err :=
				nodeLocProcessMutations(&nm.NodeKeyLoc.Loc, mutations,
					nm.MutationsBeg, nm.MutationsEnd, maxFanOut, r)
			if err != nil {
				return nil, fmt.Errorf("NodesBuilder.Done:"+
					" NodeKeyLoc: %#v, err: %v", nm.NodeKeyLoc, err)
			}
			rv = groupKeyLocs(childKeyLocs, maxFanOut, rv)
		}
	}

	return rv, nil
}
