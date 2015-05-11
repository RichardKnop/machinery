package exampletasks

// AddTask ...
func AddTask(args ...float64) (float64, error) {
	sum := 0.0
	for _, arg := range args {
		sum += arg
	}
	return sum, nil
}

// MultiplyTask ...
func MultiplyTask(args ...float64) (float64, error) {
	sum := 1.0
	for _, arg := range args {
		sum *= arg
	}
	return sum, nil
}
