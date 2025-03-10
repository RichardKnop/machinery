package utils

import (
	"os"
	"path/filepath"
	"strings"
)

const (
	LockKeyPrefix = "machinery_lock_"
)

func GetLockName(name, spec string) string {
	cmd := filepath.Base(os.Args[0])
	return strings.Join([]string{LockKeyPrefix, cmd, name, spec}, "")
}
