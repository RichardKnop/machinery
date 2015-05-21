package utils

import (
	"fmt"
	"log"
	"time"
)

// A useful closure we can use when there is a problem connecting to the broker
// It uses Fibonacci sequence to space out retry attempts
var RetryClosure = func() func() {
	retryIn := 0
	fibonacci := Fibonacci()
	return func() {
		if retryIn > 0 {
			durationString := fmt.Sprintf("%vs", retryIn)
			duration, _ := time.ParseDuration(durationString)

			log.Printf("Retrying in %v seconds", retryIn)
			time.Sleep(duration)
		}
		retryIn = fibonacci()
	}
}
