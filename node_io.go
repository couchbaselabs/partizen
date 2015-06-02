package partizen

import (
	"fmt"
	"io"
)

func (loc *Loc) Read(r io.ReaderAt) (*Loc, error) {
	// TODO: Need locking for swizzling.

	if loc.Type == LocTypeNode {
		if loc.node == nil {
			return nil, fmt.Errorf("node_io: Read loc type node TODO")
		}
		return loc, nil
	}

	if loc.Type == LocTypeVal {
		if loc.buf == nil {
			return nil, fmt.Errorf("node_io: Read loc type val TODO")
		}
		return loc, nil
	}

	return nil, fmt.Errorf("node_io: Read unreadable type, loc: %v", loc)
}

func ReadLocNode(loc *Loc, r io.ReaderAt) (Node, error) {
	loc, err := loc.Read(r)
	if err != nil {
		return nil, err
	}

	if loc.node == nil {
		return nil, fmt.Errorf("node_io: ReadLocNode, no node")
	}

	return loc.node, nil
}
