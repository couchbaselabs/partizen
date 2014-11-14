partizen

A potential copy-on-write btree design partition and sequence number
awareness.  Partitions can be independently rolled back to a previous
sequence number in O(1) time.

Status: thinking about the design

------------------------------------------------------------
ADT...

    Open(StoreFile..., options) (Store, error)

    StoreOptions...
      CompareFuncs map[string]CompareFunc

      BufAlloc
      BufAddRef
      BufDecRef

    Store...
      CommitChanges() (CommitStats, error)
      AbortChanges() error

      CopyTo(StoreFile, keepCommits) error

      Snapshot() (Store, error)

      SnapshotPreviousCommit(numCommits) => (Store, error)

      CollectionNames() ([]string, error)
      GetCollection(collName) (Collection, error)
      AddCollection(collName, compareFuncName) (Collection, error)
      RemoveCollection(collName) error

      Stats(dest) error

    Collection...
      CompareFunc() CompareFunc

      Get(partition PartitionID, key []byte, withValue bool) (seq Seq, value []byte, error)
      Set(partition PartitionID, key []byte, seq Seq, value []byte) error
      Merge(partition PartitionID, key[]byte, seq Seq, MergeFunc) error
      Del(partition PartitionID, key []byte, seq Seq) error

      Min(withValue bool) (partition PartitionID, key []byte, seq Seq, value []byte, error)
      Max(withValue bool) (partition PartitionID, key []byte, seq Seq, value []byte, error)

      Scan(onlyPartitions []PartitionID, // Use nil for all partitions.
           fromKeyInclusive []byte,
           toKeyExclusive []byte,
           withValue bool,
           reverse bool, // When true, fromKey should be > toKey.
           visitorFunc(partition PartitionID,
                       key []byte, seq Seq, value []byte) bool) error

      ChangesSince(partition PartitionID,
                   fromSeqExclusive Seq,
                   withValue bool,
                   visitorFunc(partition PartitionID,
                               key []byte, seq Seq, value []byte) bool) error

      Rewind(partition PartitionID, seq Seq, exact bool) error

------------------------------------------------------------
Sketch of persisted representation...

    type Header struct {
         Magic      uint64
         UUID       uint64
         VersionLen uint32
         VersionVal []byte
         ExtrasLen  uint32
         ExtrasVal  []byte
    }

    type Store struct {
         Magic               uint64 // Same as Header.Magic.
         UUID                uint64 // Same as Header.UUID.
         StoreManifestLoc    Loc    // Pointer to StoreManifest.
         CollectionRootNodes []Loc  // Length is StoreManifest.NumCollections.
    }

    type StoreManifest struct {
         Flags          uint32 // Reserved.
         ExtrasLen      uint32
         ExtrasVal      []byte
         NumCollections uint32
         Collections    []Collection // Length is NumCollections.
    }

    type Collection struct {
         NameLen            uint32
         Name               []byte
         CompareFuncNameLen uint32
         CompareFuncName    []byte
         ExtrasLen          uint32
         ExtrasVal          []byte
    }

    // A Node of a partizen btree has its child pointers first ordered by
    // PartitionID, then secondarily ordered by Key.
    type Node struct {
         // These Partition arrays all have the same length of NumPartitions.
         // For example PartitionIdxs[4] and Partitions[4] are both about
         // PartitionIdxs[4].PartitionID.  They are ordered by PartitionID.
         NumPartitions  uint16
         PartitionIdxs  []NodePartitionIdx
         Partitions     []NodePartition

         // Locs are ordered by Loc.Offset and are kept separate
         // as multiple NodePartitions might be pointing to the same Loc.
         NumLocs uint8 // Max fan-out of 256.
         Locs    []Loc
    }

    type NodePartitionIdx struct {
         PartitionID PartitionID
         Offset      uint32 // Start byte offset of corresponding NodePartition
                            // in Node.Partitions array, starting from the 0th
                            // Node.Partition[0] byte position.
    }

    type NodePartition {
         TotKeys     uint64 // TotKeys - TotVals equals number of deletions.
         TotVals     uint64
         TotKeyBytes uint64
         TotValBytes uint64

         NumKeySeqIdxs uint8 // Max fan-out of 256.
         KeySeqIdxs    []KeySeqIdx

         // FUTURE: Aggregates might be maintained here per NodePartition.
    }

    type KeySeqIdx struct {
         KeyLen uint16
         Key    Key
         Seq    Seq   // If this points to a Node, this is the Node's max Seq for a Partition.
         Idx    uint8 // An index into Node.Locs; an Idx of uint8(0xff) means a deleted item.
    }

    type Key         []byte
    type Val         []byte
    type PartitionID uint16
    type Seq         uint64

    type Loc struct {
         Type     uint8
         Flags    uint8
         CheckSum uint16
         Size     uint32
         Offset   uint64 // 0 offset means not persisted yet.

         buf []byte
    }

    LocTypeCollections
    LocTypeNode
    LocTypeVal

    Loc.Flags are reserved; perhaps a future LocFlagsCompressed?

------------------------------------------------------------
More ideas / TODO...

- B+tree, except for perhaps the root node, which might have data
  pointers, perhaps to help with time-series data.

- Possible for insertion of min key or max key to be O(1) until the
  root node needs to split.

- Per-partition aggregates should be supportable.

------------------------------------------------------------
Some ascii notation/digrams...

An item looks like {[a-z]}{seq-number}, like...

    a0
    b0
    c0

For the purposes of this simplified explanation, let's say our
partitioning "hash" function is whether a key starts with a consonant
or a vowel.  For example, "a0" hashes to the vowel partition; and,
"b0" & "c0" hash to the consonant partition.  In real life, you'd
instead have some real hash algorithm to calculate the partition
number for a key.

Nodes look like "n.#[pointers to lower nodes or items]", where the
pointers to lower nodes are grouped by their partition (consonants
first, then vowels).  For example, drawing the partizen btree
sideways, with leaf items on the left hand side and the root node on
the right hand side...

    a0   n.0[b0 c0; a0]
    b0
    c0

Above node n.0 has b0 and c0 in its consonants partition and a0 in its
vowels partition.

A two level partizen btree might look like..

    a0   n.0[b0; a0]  n.2[b-n.0 c-n.1; a-n.0]
    b0
    c0   n.1[c0;]

So n.2 is the root node of the tree, and n.0 & n.1 are interior nodes.

Here's a more complex tree with more leaf items...

    a0   n.0[b0 c0 d0; a0]n.0   n.3[b-n.0 f-n.1 h-n.2; a-n.0 e-n.1 i-n.2]
    b0
    c0
    d0
    e0   n.1[f0 g0; e0]
    f0
    g0
    h0   n.2[h0; i0]
    i0

If we update b0 to b1, then we append these records to the log, where
n.5 becomes the most current root node...

    b1   n.4[b1 c0 d0; a0]   n.5[b-n.4 f-n.1 h-n.2; a-n.4 e-n.1 i-n.2]

If we update d0 to d2, then we append these records, where n.7 becomes
the most current root node...

    d2   n.6[b1 c0 d1; a0]   n.7[b-n.6 f-n.1 h-n.2; a-n.6 e-n.1 i-n.2]

Let's do one more update of b1 to b2...

    b3   n.8[b3 c0 d1; a0]   n.9[b-n.8 f-n.1 h-n.2; a-n.8 e-n.1 i-n.2]

So, n.9 is the most current root node.

Next, let's rollback just the consonants partition back to where it
was at the commit of root node n.5.  So, we append a new root node to
the log...

                             n.10[b-n.4 f-n.1 h-n.2; a-n.8 e-n.1 i-n.2]

So, that's quick rollback of an individual partition (a subset of the
tree) with no major data reoganizations.

Scans and partition/key lookups of the data structure might also
ignore partitions during interior node visits, which may be able to
increase performance if candidate tree descendents are able to be
pruned away early.

