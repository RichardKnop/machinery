package exampletasks

// Add ...
func Add(args ...float64) (float64, error) {
	sum := 0.0
	for _, arg := range args {
		sum += arg
	}
	return sum, nil
}

// Multiply ...
func Multiply(args ...float64) (float64, error) {
	sum := 1.0
	for _, arg := range args {
		sum *= arg
	}
	return sum, nil
}
