package utils

// Fibonacci returns successive Fibonacci numbers starting from 1
func Fibonacci() func() int {
	a, b := 0, 1
	return func() int {
		a, b = b, a+b
		return a
	}
}
