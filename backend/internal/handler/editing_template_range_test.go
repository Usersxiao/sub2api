package handler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEditingTemplateByteRange(t *testing.T) {
	tests := []struct {
		value      string
		size       int64
		start, end int64
		partial    bool
		valid      bool
	}{
		{"", 100, 0, 99, false, true},
		{"bytes=10-19", 100, 10, 19, true, true},
		{"bytes=90-", 100, 90, 99, true, true},
		{"bytes=-12", 100, 88, 99, true, true},
		{"bytes=90-200", 100, 90, 99, true, true},
		{"bytes=100-", 100, 0, 0, false, false},
		{"bytes=1-2,4-5", 100, 0, 0, false, false},
	}
	for _, test := range tests {
		start, end, partial, valid := editingTemplateByteRange(test.value, test.size)
		require.Equal(t, test.start, start, test.value)
		require.Equal(t, test.end, end, test.value)
		require.Equal(t, test.partial, partial, test.value)
		require.Equal(t, test.valid, valid, test.value)
	}
}
