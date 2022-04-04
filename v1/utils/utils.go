package utils

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	LockKeyPrefix = "machinery_lock_"
	windows       = "windows"
)

func GetLockName(name, spec string) string {
	cmd := filepath.Base(os.Args[0])
	if runtime.GOOS == windows {
		cmd = strings.ReplaceAll(cmd, ".exe", "")
	}

	return strings.Join([]string{LockKeyPrefix, cmd, name, spec}, "")
}
