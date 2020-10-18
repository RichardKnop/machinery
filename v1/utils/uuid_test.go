package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPureUUID(t *testing.T) {
	t.Parallel()

	assert.Len(t, GetPureUUID(), 32)
}
