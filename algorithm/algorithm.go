package algorithm

import (
	"bytes"
	"fmt"
	"io"
)

type Key []byte
type Val []byte
type PartitionId uint16
type Seq uint64

type Loc struct {
	Type   LocType
	Offset uint64 // Zero Offset means not yet persisted.
	Size   uint32 // Zero Size means not yet prepared for persistence.

	// The immutable, uncompressed bytes that were or will be
	// persisted.  When buf is nil, Loc is not yet loaded from storage
	// or not yet prepared for persistence.
	buf []byte
}

type LocType uint8

const (
	LOC_TYPE_UNKNOWN LocType = 0
	LOC_TYPE_NODE    LocType = 1
	LOC_TYPE_VAL     LocType = 2
)

type KeyLoc struct {
	Key Key
	Loc Loc

	// When LOC_TYPE_NODE, this is the in-memory Node representation
	// of the Loc, where the node might share memory from Loc.buf.
	node *Node
}

type Node struct {
	KeyLocs []*KeyLoc
}

// Mutation represents a mutation request.
type Mutation struct {
	Key []byte
	Val []byte
	Op  MutationOp
}

type MutationOp uint8

const (
	MUTATION_OP_NONE   MutationOp = 0
	MUTATION_OP_UPDATE MutationOp = 1
	MUTATION_OP_DELETE MutationOp = 2

	// FUTURE MutationOp's might include merging, visiting, etc.
)

func (m *Mutation) ToValKeyLoc() *KeyLoc {
	return nil // TODO.
}

var zeroKeyLoc KeyLoc
var zeroMutation Mutation

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

func nextMutation(idx, n int, keyValOps []Mutation) (
	*Mutation, bool, int) {
	if idx < n {
		return &keyValOps[idx], true, idx
	}
	return &zeroMutation, false, idx
}

// --------------------------------------------------

type KeyLocsBuilder interface {
	AddExisting(existing *KeyLoc)
	AddUpdate(existing *KeyLoc, mutation *Mutation, mutationIdx int)
	AddNew(mutation *Mutation, mutationIdx int)
	Done(mutations []Mutation, r io.ReaderAt) ([]*KeyLoc, error)
}

// --------------------------------------------------

// A ValsBuilder implements the KeyLocsBuilder interface to return an
// array of LOC_TYPE_VAL KeyLoc's, which can be then used as input to
// create a leaf Node.
type ValsBuilder struct {
	Vals []*KeyLoc
}

func (b *ValsBuilder) AddExisting(existing *KeyLoc) {
	b.Vals = append(b.Vals, existing)
}

func (b *ValsBuilder) AddUpdate(existing *KeyLoc,
	mutation *Mutation, mutationIdx int) {
	if mutation.Op == MUTATION_OP_UPDATE {
		b.Vals = append(b.Vals, mutation.ToValKeyLoc())
	}
}

func (b *ValsBuilder) AddNew(mutation *Mutation, mutationIdx int) {
	if mutation.Op == MUTATION_OP_UPDATE {
		b.Vals = append(b.Vals, mutation.ToValKeyLoc())
	}
}

func (b *ValsBuilder) Done(mutations []Mutation, r io.ReaderAt) (
	[]*KeyLoc, error) {
	return b.Vals, nil
}

// --------------------------------------------------

// An NodesBuilder implements the KeyLocsBuilder interface to return an
// array of LOC_TYPE_NODE KeyLoc's, which can be then used as input to
// create an interior Node.
type NodesBuilder struct {
	Degree        int
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

func (b *NodesBuilder) Done(mutations []Mutation, r io.ReaderAt) (
	[]*KeyLoc, error) {
	var rv []*KeyLoc

	for _, nm := range b.NodeMutations {
		if nm.MutationsBeg >= nm.MutationsEnd {
			rv = append(rv, nm.NodeKeyLoc)
		} else {
			node, err := ReadNode(r, nm.NodeKeyLoc)
			if err != nil {
				return nil, fmt.Errorf("NodesBuilder.Done,"+
					" NodeKeyLoc: %#v, err: %v",
					nm.NodeKeyLoc, err)
			}
			if node == nil {
				return nil, fmt.Errorf("NodesBuilder.Done,"+
					" missing node, NodeKeyLoc: %#v, err: %v",
					nm.NodeKeyLoc, err)
			}
			childKeyLocs, err := nodeProcessMutations(b.Degree, node,
				mutations, nm.MutationsBeg, nm.MutationsEnd, r)
			if err != nil {
				return nil, fmt.Errorf("NodesBuilder.Done,"+
					" node: %#v, NodeKeyLoc: %#v, err: %v",
					node, nm.NodeKeyLoc, err)
			}
			rv = formParentKeyLocs(b.Degree, childKeyLocs, rv)
		}
	}

	return rv, nil
}

// --------------------------------------------------

func nodeProcessMutations(degree int, node *Node,
	mutations []Mutation, mbeg, mend int,
	r io.ReaderAt) ([]*KeyLoc, error) {
	var builder KeyLocsBuilder
	if len(node.KeyLocs) <= 0 ||
		node.KeyLocs[0].Loc.Type != LOC_TYPE_NODE {
		builder = &ValsBuilder{}
	} else {
		builder = &NodesBuilder{Degree: degree}
	}
	processMutations(node.KeyLocs, 0, len(node.KeyLocs),
		mutations, mbeg, mend, builder)
	return builder.Done(mutations, r)
}

func formParentKeyLocs(degree int, childKeyLocs []*KeyLoc,
	parentKeyLocs []*KeyLoc) []*KeyLoc {
	beg := 0
	for i := degree; i < len(childKeyLocs); i = i + degree {
		parentKeyLocs = append(parentKeyLocs, &KeyLoc{
			Key:  childKeyLocs[beg].Key,
			Loc:  Loc{Type: LOC_TYPE_NODE},
			node: &Node{KeyLocs: childKeyLocs[beg:i]},
		})
		beg = i
	}
	parentKeyLocs = append(parentKeyLocs, &KeyLoc{
		Key:  childKeyLocs[beg].Key,
		Loc:  Loc{Type: LOC_TYPE_NODE},
		node: &Node{KeyLocs: childKeyLocs[beg:]},
	})
	return parentKeyLocs
}

// --------------------------------------------------

func ReadNode(r io.ReaderAt, kl *KeyLoc) (*Node, error) {
	return nil, fmt.Errorf("unimpl")
}
