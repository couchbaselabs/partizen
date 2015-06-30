package main

import (
	"fmt"
	"unsafe"

	"github.com/couchbaselabs/partizen"
	"github.com/couchbaselabs/go-slab"

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

	var ItemLocRef partizen.ItemLocRef
	fmt.Printf("partizen.ItemLocRef: %d\n", unsafe.Sizeof(ItemLocRef))

	var ItemLoc partizen.ItemLoc
	fmt.Printf("partizen.ItemLoc: %d\n", unsafe.Sizeof(ItemLoc))

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

	var KeyItemLoc partizen.KeyItemLoc
	fmt.Printf("partizen.KeyItemLoc: %d\n", unsafe.Sizeof(KeyItemLoc))

	var slabLoc slab.Loc
	fmt.Printf("slab.Loc: %d\n", unsafe.Sizeof(slabLoc))
}
