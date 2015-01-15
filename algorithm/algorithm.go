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

// A KeyLoc associates a Key with a Loc.
type KeyLoc struct {
	Key Key
	Loc Loc
}

// A Loc represents the location of a byte range persisted or
// soon-to-be-persisted to the storage file.  Persisted field sizes
// are carefully chosen to add up to 128 bits.
type Loc struct {
	// Offset is relative to start of file.  Offset of 0 means the
	// pointed-to bytes buf are not persisted yet.
	Offset   uint64
	Size     uint32
	Type     LocType
	Flags    uint8
	CheckSum uint16 // An optional checksum of the bytes buf.

	// Transient; non-nil when the Loc is read into memory or when the
	// bytes of the Loc are prepared for writing.  The len(Loc.buf)
	// should equal Loc.Size.
	buf []byte

	// Transient; only used when Type is LocTypeNode.  If nil, runtime
	// representation hasn't been loaded yet.  The node might share
	// memory with Loc.buf.
	node *Node
}

type LocType uint8

const (
	LOC_TYPE_UNKNOWN LocType = 0
	LOC_TYPE_NODE    LocType = 1
	LOC_TYPE_VAL     LocType = 2
)

// A Node has KeyLoc children.  All the KeyLoc children of a Node must
// be of the same Loc.Type; e.g., either all val's or all nodes.
type Node struct {
	KeyLocs []*KeyLoc
}

// A Mutation represents a mutation request on a key.
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
	return &KeyLoc{
		Key: m.Key,
		Loc: Loc{
			Type: LOC_TYPE_VAL,
			Size: uint32(len(m.Val)),
			buf:  m.Val,
		},
	}
}

var zeroKeyLoc KeyLoc
var zeroMutation Mutation

// --------------------------------------------------

func rootLocProcessMutations(degree int, rootLoc *Loc,
	mutations []Mutation, r io.ReaderAt) (*KeyLoc, error) {
	rv, err := nodeLocProcessMutations(degree, rootLoc,
		mutations, 0, len(mutations), r)
	if err != nil {
		return nil, fmt.Errorf("rootLocProcessMutations:"+
			" rootLoc: %#v, err: %v", rootLoc, err)
	}
	for len(rv) > 1 {
		rv = formParentKeyLocs(degree, rv, nil)
	}
	if len(rv) > 0 {
		return rv[0], nil
	}
	return nil, nil
}

func nodeLocProcessMutations(degree int, nodeLoc *Loc,
	mutations []Mutation, mbeg, mend int,
	r io.ReaderAt) ([]*KeyLoc, error) {
	node, err := ReadLocNode(nodeLoc, r)
	if err != nil {
		return nil, fmt.Errorf("nodeLocProcessMutations:"+
			" nodeLoc: %#v, err: %v", nodeLoc, err)
	}

	var builder KeyLocsBuilder
	if node == nil || len(node.KeyLocs) <= 0 ||
		node.KeyLocs[0].Loc.Type != LOC_TYPE_NODE {
		builder = &ValsBuilder{}
	} else {
		builder = &NodesBuilder{}
	}

	var keyLocs []*KeyLoc
	if node != nil {
		keyLocs = node.KeyLocs
	}

	processMutations(keyLocs, 0, len(keyLocs),
		mutations, mbeg, mend,
		builder)

	return builder.Done(mutations, degree, r)
}

func formParentKeyLocs(degree int, childKeyLocs []*KeyLoc,
	parentKeyLocs []*KeyLoc) []*KeyLoc {
	beg := 0
	for i := degree; i < len(childKeyLocs); i = i + degree {
		parentKeyLocs = append(parentKeyLocs, &KeyLoc{
			Key: childKeyLocs[beg].Key,
			Loc: Loc{
				Type: LOC_TYPE_NODE,
				node: &Node{KeyLocs: childKeyLocs[beg:i]},
			},
		})
		beg = i
	}
	if beg < len(childKeyLocs) {
		parentKeyLocs = append(parentKeyLocs, &KeyLoc{
			Key: childKeyLocs[beg].Key,
			Loc: Loc{
				Type: LOC_TYPE_NODE,
				node: &Node{KeyLocs: childKeyLocs[beg:]},
			},
		})
	}
	return parentKeyLocs
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
	Done(mutations []Mutation, degree int, r io.ReaderAt) ([]*KeyLoc, error)
}

// --------------------------------------------------

// A ValsBuilder implements the KeyLocsBuilder interface to return an
// array of LOC_TYPE_VAL KeyLoc's, which can be then used as input to
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
		b.s = append(b.s, mutation.ToValKeyLoc())
	}
}

func (b *ValsBuilder) AddNew(mutation *Mutation, mutationIdx int) {
	if mutation.Op == MUTATION_OP_UPDATE {
		b.s = append(b.s, mutation.ToValKeyLoc())
	}
}

func (b *ValsBuilder) Done(mutations []Mutation, degree int,
	r io.ReaderAt) ([]*KeyLoc, error) {
	return b.s, nil
}

// --------------------------------------------------

// An NodesBuilder implements the KeyLocsBuilder interface to return an
// array of LOC_TYPE_NODE KeyLoc's, which can be then used as input to
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
				nodeLocProcessMutations(degree, &nm.NodeKeyLoc.Loc,
					mutations, nm.MutationsBeg, nm.MutationsEnd, r)
			if err != nil {
				return nil, fmt.Errorf("NodesBuilder.Done:"+
					" NodeKeyLoc: %#v, err: %v", nm.NodeKeyLoc, err)
			}
			rv = formParentKeyLocs(degree, childKeyLocs, rv)
		}
	}

	return rv, nil
}

// --------------------------------------------------

func ReadLocNode(loc *Loc, r io.ReaderAt) (*Node, error) {
	if loc == nil {
		return nil, nil
	}
	if loc.Type != LOC_TYPE_NODE {
		return nil, fmt.Errorf("ReadLocNode: not a node, loc: %#v", loc)
	}
	if loc.node != nil {
		return loc.node, nil
	}
	return nil, fmt.Errorf("ReadLocNode: failed")
}
