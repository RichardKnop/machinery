package machinery

import (
	"errors"
	"fmt"
)

// ParseNumberArgs - parses []interface{} into []float64
func ParseNumberArgs(args []interface{}) ([]float64, error) {
	var err error
	var parsedArgs = make([]float64, len(args))

	for n, arg := range args {
		num, ok := arg.(float64)
		if !ok {
			errMsg := fmt.Sprintf(
				"%v. arg is not a valid floating point number: %v",
				n+1,
				num,
			)
			err = errors.New(errMsg)
			return nil, err
		}
		parsedArgs[n] = num
	}

	return parsedArgs, nil
}
