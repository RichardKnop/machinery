package utils

const LockKeyPrefix = "machinery_lock_"

func GetLockName(name, spec string) string {
	return LockKeyPrefix + name + spec
}
