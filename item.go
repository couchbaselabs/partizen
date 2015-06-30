package partizen

// NewItemBufRef helper function allocates an ItemBufRef to represent
// the persisted bytes for an item, including its Key, Val and other
// metadata.
func NewItemBufRef(bufManager BufManager, partitionId PartitionId,
	keyLen, seq Seq, valLen int) ItemBufRef {
	return nil // TODO.
}
