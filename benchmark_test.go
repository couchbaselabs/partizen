package partizen

import (
	"strconv"
	"testing"

	"github.com/tv42/benchmark-ordered-map/fixture"
)

func noop() {}

func TestInsertBatchSize1(t *testing.T) {
	benchmarkInsertBatchSizeN(noop,
		1, 1, fixture.SortedTestData[:])
}

func TestInsertBatchSize100(t *testing.T) {
	benchmarkInsertBatchSizeN(noop,
		1, 100, fixture.SortedTestData[:])
}

// ------------------------------------

func BenchmarkInsertBatchSize1(b *testing.B) {
	benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 1, fixture.SortedTestData[:])
}

func BenchmarkInsertBatchSize10(b *testing.B) {
	benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 10, fixture.SortedTestData[:])
}

func BenchmarkInsertBatchSize100(b *testing.B) {
	benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 100, fixture.SortedTestData[:])
}

func BenchmarkInsertBatchSize1000(b *testing.B) {
	benchmarkInsertBatchSizeN(func() { b.ResetTimer() },
		b.N, 1000, fixture.SortedTestData[:])
}

func benchmarkInsertBatchSizeN(markStart func(), numRuns, batchSize int,
	data []fixture.Item) error {
	var mm [][]Mutation
	t := 0
	for t < len(data) {
		m := make([]Mutation, 0, batchSize)
		i := 0
		for i < batchSize && t < len(data) {
			m = append(m, Mutation{
				Key: []byte(strconv.Itoa(int(fixture.SortedTestData[t].Key))),
				Op:  MUTATION_OP_UPDATE,
			})
			i++
			t++
		}
		mm = append(mm, m)
	}

	markStart()

	for i := 0; i < numRuns; i++ {
		var rootNodeLoc *Loc
		for j := 0; j < len(mm); j++ {
			ksl, err := rootNodeLocProcessMutations(rootNodeLoc, mm[j], 32, nil)
			if err != nil {
				return err
			}
			rootNodeLoc = &ksl.Loc
		}
	}

	return nil
}

func TODO_BenchmarkSortedInsert_ReplaceOrInsert(b *testing.B) {
	// for i := 0; i < b.N; i++ {
	// 		tree := btree.New(btreeDegree)
	// 		for i := 0; i < len(fixture.SortedTestData); i++ {
	// 			tree.ReplaceOrInsert(googItem(fixture.SortedTestData[i]))
	// 		}
	// 	}
}

func TODO_BenchmarkIterate(b *testing.B) {
	// 	tree := btree.New(btreeDegree)
	// 	for i := 0; i < len(fixture.TestData); i++ {
	// 		tree.ReplaceOrInsert(googItem(fixture.TestData[i]))
	// 	}
	// 	b.ResetTimer()
	//
	// 	for i := 0; i < b.N; i++ {
	//		tree.Ascend(func(i btree.Item) bool {
	// 			_ = i.(googItem).Key
	// 			_ = i.(googItem).Value
	// 			return true
	// 		})
	// 	}
}