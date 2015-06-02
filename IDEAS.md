partizen

A potential copy-on-write b+tree design with partition and sequence
number awareness.  Partitions can be independently rolled back to a
previous sequence number in O(1) time.  But, key data will still be
stored in lexigraphic order in b-tree fashion, so that range scans
will find items lexigraphically "close" to each other.  (e.g., all the
entries of folks who live in "Mountain View" will be next to each
other, even if they come from different vbuckets)

Status: thinking about the design / draft.

------------------------------------------------------------
Some ascii notation/digrams...

An item looks like {[a-z]}{seq-number}.  Here are 3 sample items...

    a0
    b0
    c0

For the purposes of this simplified explanation, let's say our
partitioning "hash" function is whether a key starts with a consonant
or a vowel.  For example, "a0" hashes to the vowel partition; and,
"b0" & "c0" hash to the consonant partition.  In real life, you'd
instead have a real hash function to calculate a real partition number
for a key.

Nodes look like "n.{number}[pointers to lower nodes or items]", where the
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

The descendent pointers in node n.2 is recorded as...

  [b-n.0 c-n.1; a-n.0]

This means that in the consonants partition, you can find key range
["b"..."c") at node n.0.  And, you can find key range ["c"...TOP) at
node n.1.  Then, in the vowels partitions (after the ";" separator
character), you can find key range ["a"...TOP) at node n.0.

Here's a more complex tree with more leaf items...

    a0   n.0[b0 c0 d0; a0]   n.3[b-n.0 f-n.1 h-n.2; a-n.0 e-n.1 i-n.2]
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

    d2   n.6[b1 c0 d2; a0]   n.7[b-n.6 f-n.1 h-n.2; a-n.6 e-n.1 i-n.2]

Let's do one more update of b1 to b3...

    b3   n.8[b3 c0 d2; a0]   n.9[b-n.8 f-n.1 h-n.2; a-n.8 e-n.1 i-n.2]

So, n.9 is the most current root node.

Next, let's rollback just the consonants partition back to where it
was at the commit of root node n.5.  So, we append a new root node to
the log...

                             n.10[b-n.4 f-n.1 h-n.2; a-n.8 e-n.1 i-n.2]

That new n.10 root node copies the old consonants partition from n.5,
and the vowels partitions from n.9.

So, that's quick rollback of an individual partition (a subset of the
tree) with no major data reoganizations, as an O(1) step.

Partition-based key lookups of the partizen btree might also be able
to prune away non-matching partitions during interior node visits,
which may be able to increase performance.

------------------------------------------------------------
More ideas / TODO...

- B+tree, except for perhaps the root node, which might have data
  pointers, perhaps to help with time-series data.

- Possible for insertion of min key or max key to be O(1) until the
  root node needs to split.

- Per-partition aggregates should be supportable.

