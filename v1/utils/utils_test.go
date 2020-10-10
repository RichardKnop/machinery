package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetLockName(t *testing.T) {
	lockName := GetLockName("test", "*/3 * * *")
	assert.Equal(t, "machinery_lock____TestGetLockName_in_github_com_RichardKnop_machinery_v1_utilstest*/3 * * *", lockName)
}
