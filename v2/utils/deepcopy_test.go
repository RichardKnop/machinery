package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeepCopy(t *testing.T) {
	t.Parallel()

	type s struct {
		A float64
		B int
		C []int
		D *int
		E map[string]int
	}
	var d = 3
	var dst = new(s)
	var src = s{1.0, 1, []int{1, 2, 3}, &d, map[string]int{"a": 1}}

	err := DeepCopy(dst, &src)
	src.A = 2

	assert.NoError(t, err)
	assert.Equal(t, 1.0, dst.A)
	assert.Equal(t, 1, dst.B)
	assert.Equal(t, []int{1, 2, 3}, dst.C)
	assert.Equal(t, &d, dst.D)
	assert.Equal(t, map[string]int{"a": 1}, dst.E)
}
