package partizen

import (
	"fmt"
	"io"
)

func ReadLocNode(loc *Loc, r io.ReaderAt) (Node, error) {
	if loc == nil {
		return nil, nil
	}
	if loc.Type != LocTypeNode {
		return nil, fmt.Errorf("ReadLocNode: not a node, loc: %#v", loc)
	}

	// TODO: Need locking before accessing loc.node?
	if loc.node != nil {
		return loc.node, nil
	}

	return nil, fmt.Errorf("ReadLocNode: TODO")
}
