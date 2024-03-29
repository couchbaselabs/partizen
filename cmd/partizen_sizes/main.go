package main

import (
	"fmt"
	"unsafe"

	"github.com/couchbaselabs/go-slab"
	"github.com/couchbaselabs/partizen"
)

func main() {
	var PartitionId partizen.PartitionId
	fmt.Printf("partizen.PartitionId: %d\n", unsafe.Sizeof(PartitionId))

	var PartitionIds partizen.PartitionIds
	fmt.Printf("partizen.PartitionIds: %d\n", unsafe.Sizeof(PartitionIds))

	var Key partizen.Key
	fmt.Printf("partizen.Key: %d\n", unsafe.Sizeof(Key))

	var Seq partizen.Seq
	fmt.Printf("partizen.Seq: %d\n", unsafe.Sizeof(Seq))

	var Val partizen.Val
	fmt.Printf("partizen.Val: %d\n", unsafe.Sizeof(Val))

	var Mutation partizen.Mutation
	fmt.Printf("partizen.Mutation: %d\n", unsafe.Sizeof(Mutation))

	var Header partizen.Header
	fmt.Printf("partizen.Header: %d\n", unsafe.Sizeof(Header))

	var Footer partizen.Footer
	fmt.Printf("partizen.Footer: %d\n", unsafe.Sizeof(Footer))

	var ChildLocRef partizen.ChildLocRef
	fmt.Printf("partizen.ChildLocRef: %d\n", unsafe.Sizeof(ChildLocRef))

	var ChildLoc partizen.ChildLoc
	fmt.Printf("partizen.ChildLoc: %d\n", unsafe.Sizeof(ChildLoc))

	var Loc partizen.Loc
	fmt.Printf("partizen.Loc: %d\n", unsafe.Sizeof(Loc))

	var BufRef partizen.BufRef
	fmt.Printf("partizen.BufRef: %d\n", unsafe.Sizeof(BufRef))

	var DefaultBufRef partizen.DefaultBufRef
	fmt.Printf("partizen.DefaultBufRef: %d\n", unsafe.Sizeof(DefaultBufRef))

	var Node partizen.Node
	fmt.Printf("partizen.Node: %d\n", unsafe.Sizeof(Node))

	var Partitions partizen.Partitions
	fmt.Printf("partizen.Partitions: %d\n", unsafe.Sizeof(Partitions))

	var KeyChildLoc partizen.KeyChildLoc
	fmt.Printf("partizen.KeyChildLoc: %d\n", unsafe.Sizeof(KeyChildLoc))

	var slabLoc slab.Loc
	fmt.Printf("slab.Loc: %d\n", unsafe.Sizeof(slabLoc))
}
