[![Codeship Status for RichardKnop/logging](https://codeship.com/projects/3844b520-3c97-0134-92d6-6a89aa757e0d/status?branch=master)](https://codeship.com/projects/166987)

[![GoDoc](https://godoc.org/github.com/nathany/looper?status.svg)](http://godoc.org/github.com/RichardKnop/logging)
[![Travis Status for RichardKnop/logging](https://travis-ci.org/RichardKnop/logging.svg?branch=master)](https://travis-ci.org/RichardKnop/logging)
[![Donate Bitcoin](https://img.shields.io/badge/donate-bitcoin-orange.svg)](https://richardknop.github.io/donate/)

# Logging

A simple leveled logging library with coloured output.

Log levels:
* `INFO` (blue)
* `WARNING` (pink)
* `ERROR` (red)
* `FATAL` (red)

Formatters:
* `DefaultFormatter`
* `ColouredFormatter`

Example usage:

```go
package main

import (
	"github.com/RichardKnop/logging"
)

var (
	plainLogger    logging.Logger
	colouredLogger logging.Logger
)

func init() {
	plainLogger = logging.New(nil, nil, nil)
	colouredLogger = logging.New(nil, nil, new(logging.ColouredFormatter))
}

func main() {
	plainLogger[logging.INFO].Print("log message")
	plainLogger[logging.INFO].Printf("formatted %s %s", "log", "message")
	colouredLogger[logging.INFO].Print("log message")
	colouredLogger[logging.INFO].Printf("formatted %s %s", "log", "message")
}
```
