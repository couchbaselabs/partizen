package partizen

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/tv42/benchmark-ordered-map/fixture"
)

func noop() {}

func TestInsertBatchSize1Items200(t *testing.T) {
	benchmarkInsertBatchSizeN(noop,
		1, 1, directBatchChoice,
		// TODO: Theory is that unbalanced tree leads this
		// to be too slow for full datasize.
		fixture.SortedTestData[:200])
}

func TestInsertBatchSize100(t *testing.T) {
	benchmarkInsertBatchSizeN(noop,
		1, 100, directBatchChoice,
		fixture.SortedTestData[:])
}

func TestInsertBatchSize1Items200Reverse(t *testing.T) {
	benchmarkInsertBatchSizeN(noop,
		1, 1, reverseBatchChoice,
		// TODO: Theory is that unbalanced tree leads this
		// to be too slow for full datasize.
		fixture.SortedTestData[:200])
}

func TestInsertBatchSize100Reverse(t *testing.T) {
	benchmarkInsertBatchSizeN(noop,
		1, 100, reverseBatchChoice,
		fixture.SortedTestData[:])
}

// ------------------------------------

func BenchmarkInsertBatchSize1Items200(b *testing.B) {
	benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 1, directBatchChoice,
		// TODO: Theory is that unbalanced tree leads this
		// to be too slow for full datasize.
		fixture.SortedTestData[:200])
}

func BenchmarkInsertBatchSize10(b *testing.B) {
	benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 10, directBatchChoice,
		fixture.SortedTestData[:])
}

func BenchmarkInsertBatchSize100(b *testing.B) {
	benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 100, directBatchChoice,
		fixture.SortedTestData[:])
}

func BenchmarkInsertBatchSize1000(b *testing.B) {
	benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 1000, directBatchChoice,
		fixture.SortedTestData[:])
}

// ------------------------------------

func BenchmarkInsertBatchSize1Items200Reverse(b *testing.B) {
	err := benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 1, reverseBatchChoice,
		// TODO: Theory is that unbalanced tree leads this
		// to be too slow for full datasize.
		fixture.SortedTestData[:200])
	if err != nil {
		b.Errorf("err: %v", err)
	}
}

func BenchmarkInsertBatchSize10Reverse(b *testing.B) {
	err := benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 10, reverseBatchChoice,
		fixture.SortedTestData[:])
	if err != nil {
		b.Errorf("err: %v", err)
	}
}

func BenchmarkInsertBatcheSize100Reverse(b *testing.B) {
	err := benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 100, reverseBatchChoice,
		fixture.SortedTestData[:])
	if err != nil {
		b.Errorf("err: %v", err)
	}
}

func BenchmarkInsertBatchSize1000Reverse(b *testing.B) {
	err := benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 1000, reverseBatchChoice,
		fixture.SortedTestData[:])
	if err != nil {
		b.Errorf("err: %v", err)
	}
}

// ------------------------------------

func benchmarkInsertBatchSizeN(markStart func(), numRuns,
	batchSize int,
	batchChoice func(mm [][]Mutation, j int) []Mutation,
	data []fixture.Item) error {
	val := []byte("hello")
	var mm [][]Mutation
	t := 0
	for t < len(data) {
		m := make([]Mutation, 0, batchSize)
		i := 0
		for i < batchSize && t < len(data) {
			key := []byte(strconv.Itoa(int(fixture.SortedTestData[t].Key)))
			mutation, _ := NewMutation(testBufManager, MUTATION_OP_UPDATE,
				0, key, 0, val, NO_MATCH_SEQ)
			m = append(m, mutation)
			i++
			t++
		}
		mm = append(mm, m)
	}

	markStart()

	for i := 0; i < numRuns; i++ {
		var rootChildLoc *ChildLoc
		var err error
		for j := 0; j < len(mm); j++ {
			rootChildLoc, err =
				rootProcessMutations(rootChildLoc,
					batchChoice(mm, j), nil, 15, 32,
					&PtrChildLocsArrayHolder{},
					testBufManager, nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// ------------------------------------

func directBatchChoice(mm [][]Mutation, j int) []Mutation {
	return mm[j]
}

func reverseBatchChoice(mm [][]Mutation, j int) []Mutation {
	return mm[len(mm)-1-j]
}

// ------------------------------------

func TODO_BenchmarkSortedInsert_ReplaceOrInsert(b *testing.B) {
	// for i := 0; i < b.N; i++ {
	// 		tree := btree.New(btreeDegree)
	// 		for i := 0; i < len(fixture.SortedTestData); i++ {
	// 			tree.ReplaceOrInsert(googItem(fixture.SortedTestData[i]))
	// 		}
	// 	}
}

func BenchmarkIterate(b *testing.B) {
	so := &StoreOptions{
		BufManager: testBufManager,
	}

	s, _ := StoreOpen(nil, so)
	c, _ := s.AddCollection("x", "")

	batchSize := 10000
	data := fixture.SortedTestData
	val := []byte("hello")
	m := make([]Mutation, 0, batchSize)
	t := 0
	for t < len(data) {
		m = m[0:0]
		i := 0
		for i < cap(m) && t < len(data) {
			key := []byte(strconv.Itoa(int(data[t].Key)))
			mutation, _ := NewMutation(testBufManager, MUTATION_OP_UPDATE,
				0, key, 0, val, NO_MATCH_SEQ)
			m = append(m, mutation)
			i++
			t++
		}
		err := c.Batch(m)
		if err != nil {
			b.Errorf("batch err: %#v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, _ := c.Scan([]byte(nil), true, nil, true, 0)
		key := []byte{}
		for key != nil {
			_, key, _, _, _ = cur.Next()
		}
	}
}

func BenchmarkItemBufRefSets(b *testing.B) {
	kvs := make([][]byte, 300)
	for i := 0; i < len(kvs); i++ {
		kvs[i] = []byte(fmt.Sprintf("%d", rand.Int()))
	}

	bm := NewDefaultBufManager(128, 1024*1024, 1.5, nil)

	so := &StoreOptions{
		BufManager: bm,
	}

	s, _ := StoreOpen(nil, so)
	if s.(*store).bufManager != bm {
		panic("wrong bm")
	}

	c, _ := s.AddCollection("x", "")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		kv := kvs[i%len(kvs)]

		ibr, _ := NewItemBufRef(bm, 0, kv, Seq(i), kv)
		c.SetItemBufRef(NO_MATCH_SEQ, ibr)
		ibr.DecRef(bm)
	}

	fmt.Printf("b.N: %d, arena: %#v\n", b.N, bm.arena)
}
