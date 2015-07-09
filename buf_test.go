package partizen

import (
	"testing"
)

func TestCompareKeyItemBufRef(t *testing.T) {
	partitionId := PartitionId(0)
	seq := Seq(0)

	ibr, err := NewItemBufRef(testBufManager, partitionId, []byte("b"), seq, nil)
	if err != nil || ibr == nil {
		t.Errorf("expected no err")
	}

	tests := []struct{
		s   string
		exp int
	}{
		{"d", 1},
		{"c", 1},
		{"b", 0},
		{"a", -1},
		{"aa", -1},
		{"ba", 1},
		{"baa", 1},
	}

	for testi, test := range tests {
		c := CompareKeyItemBufRef([]byte(test.s), ibr, testBufManager)
		if c != test.exp {
			t.Errorf("testi %d, s: %s, exp: %d, got: %d",
				testi, test.s, test.exp, c)
		}
	}
}
