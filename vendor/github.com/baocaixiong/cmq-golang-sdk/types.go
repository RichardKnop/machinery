package cmq

func IntPtr(v int) *int {
	return &v
}

func Int64Ptr(v int64) *int64 {
	return &v
}

func UintPtr(v uint) *uint {
	return &v
}

func Uint64Ptr(v uint64) *uint64 {
	return &v
}

func Float64Ptr(v float64) *float64 {
	return &v
}

func StringPtr(v string) *string {
	return &v
}

func SliceStringPtr(v []string) *[]string {
	return &v
}

func SliceStringValue(v *[]string) []string {
	if v != nil {
		return *v
	}

	return nil
}
