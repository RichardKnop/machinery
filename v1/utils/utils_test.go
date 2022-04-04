package utils

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLockName(t *testing.T) {
	t.Parallel()

	lockName := GetLockName("test", "*/3 * * *")
	input := []string{LockKeyPrefix, filepath.Base(os.Args[0]), "test", "*/3 * * *"}
	assert.Equal(t, strings.Join(input, ""), lockName)
}
