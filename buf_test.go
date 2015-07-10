package partizen

import (
	"testing"
)

func TestCompareKeyItemBufRef(t *testing.T) {
	testCompareKeyItemBufRef(t, "a-1.5", NewDefaultBufManager(32, 1024*1024, 1.5, nil))
	testCompareKeyItemBufRef(t, "1-1.1", NewDefaultBufManager(1, 1024, 1.1, nil))
}

func testCompareKeyItemBufRef(t *testing.T, label string, testBufManager BufManager) {
	partitionId := PartitionId(0)
	seq := Seq(0)

	tests := []struct {
		k   string
		s   string
		exp int
	}{
		{"d", "b", 1},
		{"c", "b", 1},
		{"b", "b", 0},
		{"a", "b", -1},
		{"aa", "b", -1},
		{"ba", "b", 1},
		{"baa", "b", 1},

		{"d", "bbb", 1},
		{"c", "bbb", 1},
		{"bbb", "bbb", 0},
		{"a", "bbb", -1},
		{"bbba", "bbb", 1},
		{"bbb", "bbba", -1},
	}

	for testi, test := range tests {
		ibr, err := NewItemBufRef(testBufManager, partitionId, []byte(test.s), seq, nil)
		if err != nil || ibr == nil {
			t.Errorf("expected no err")
		}

		c := CompareKeyItemBufRef([]byte(test.k), ibr, testBufManager)
		if c != test.exp {
			t.Errorf("%s - testi %d, k: %s, s: %s, exp: %d, got: %d",
				label, testi, test.k, test.s, test.exp, c)
		}
	}
}
