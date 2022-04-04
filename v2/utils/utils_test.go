package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLockName(t *testing.T) {
	t.Parallel()

	lockName := GetLockName("test", "*/3 * * *")
	assert.Equal(t, "machinery_lock_utils.testtest*/3 * * *", lockName)
}
