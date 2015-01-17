package partizen

import (
	"bytes"
	"fmt"
	"io"
)

func rootNodeLocProcessMutations(rootNodeLoc *Loc, mutations []Mutation,
	degree int, r io.ReaderAt) (*KeyLoc, error) {
	keyLocs, err := nodeLocProcessMutations(rootNodeLoc, mutations,
		0, len(mutations), degree, r)
	if err != nil {
		return nil, fmt.Errorf("rootLocProcessMutations:"+
			" rootNodeLoc: %#v, err: %v", rootNodeLoc, err)
	}
	for len(keyLocs) > 1 ||
		(len(keyLocs) > 0 && keyLocs[0].Loc.Type == LocTypeVal) {
		keyLocs = groupKeyLocs(keyLocs, degree, nil)
	}
	if len(keyLocs) > 0 {
		return keyLocs[0], nil
	}
	return nil, nil
}

func nodeLocProcessMutations(nodeLoc *Loc, mutations []Mutation,
	mbeg, mend int, degree int, r io.ReaderAt) ([]*KeyLoc, error) {
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

	var keyLocs []*KeyLoc
	if node != nil {
		keyLocs = node.GetKeyLocs()
	}

	processMutations(keyLocs, 0, len(keyLocs),
		mutations, mbeg, mend, builder)

	return builder.Done(mutations, degree, r)
}

func groupKeyLocs(childKeyLocs []*KeyLoc, degree int,
	groupedKeyLocsStart []*KeyLoc) []*KeyLoc {
	// TODO: A more optimal grouping approach would instead partition
	// the childKeyLocs more evenly, instead of the current approach
	// where the last group might be unfairly too small as it has only
	// the simple remainder of childKeyLocs.
	groupedKeyLocs := groupedKeyLocsStart
	beg := 0
	for i := degree; i < len(childKeyLocs); i = i + degree {
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

func processMutations(
	existings []*KeyLoc,
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

func nextKeyLoc(idx, n int, keyLocs []*KeyLoc) (
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
	Done(mutations []Mutation, degree int, r io.ReaderAt) ([]*KeyLoc, error)
}

// --------------------------------------------------

// A ValsBuilder implements the KeyLocsBuilder interface to return an
// array of LocTypeVal KeyLoc's, which can be then used as input to
// create a leaf Node.
type ValsBuilder struct {
	s []*KeyLoc
}

func (b *ValsBuilder) AddExisting(existing *KeyLoc) {
	b.s = append(b.s, existing)
}

func (b *ValsBuilder) AddUpdate(existing *KeyLoc,
	mutation *Mutation, mutationIdx int) {
	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, MutationToValKeyLoc(mutation))
	}
}

func (b *ValsBuilder) AddNew(mutation *Mutation, mutationIdx int) {
	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, MutationToValKeyLoc(mutation))
	}
}

func (b *ValsBuilder) Done(mutations []Mutation, degree int,
	r io.ReaderAt) ([]*KeyLoc, error) {
	return b.s, nil
}

// --------------------------------------------------

// An NodesBuilder implements the KeyLocsBuilder interface to return an
// array of LocTypeNode KeyLoc's, which can be then used as input to
// create an interior Node.
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

func (b *NodesBuilder) Done(mutations []Mutation, degree int,
	r io.ReaderAt) ([]*KeyLoc, error) {
	var rv []*KeyLoc

	for _, nm := range b.NodeMutations {
		if nm.MutationsBeg >= nm.MutationsEnd {
			if nm.NodeKeyLoc != nil {
				rv = append(rv, nm.NodeKeyLoc)
			}
		} else {
			childKeyLocs, err :=
				nodeLocProcessMutations(&nm.NodeKeyLoc.Loc, mutations,
					nm.MutationsBeg, nm.MutationsEnd, degree, r)
			if err != nil {
				return nil, fmt.Errorf("NodesBuilder.Done:"+
					" NodeKeyLoc: %#v, err: %v", nm.NodeKeyLoc, err)
			}
			rv = groupKeyLocs(childKeyLocs, degree, rv)
		}
	}

	return rv, nil
}

// --------------------------------------------------

func MutationToValKeyLoc(m *Mutation) *KeyLoc {
	return &KeyLoc{
		Key: m.Key,
		Loc: Loc{
			Type: LocTypeVal,
			Size: uint32(len(m.Val)),
			buf:  m.Val,
		},
	}
}
