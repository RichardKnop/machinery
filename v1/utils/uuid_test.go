package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetPureUUID(t *testing.T) {
	t.Parallel()
	assert.Len(t, GetPureUUID(), 32)
}
