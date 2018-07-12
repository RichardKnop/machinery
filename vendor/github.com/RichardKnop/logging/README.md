## Logging

A simple leveled logging library with coloured output.

[![Travis Status for RichardKnop/logging](https://travis-ci.org/RichardKnop/logging.svg?branch=master&label=linux+build)](https://travis-ci.org/RichardKnop/logging)
[![godoc for RichardKnop/logging](https://godoc.org/github.com/nathany/looper?status.svg)](http://godoc.org/github.com/RichardKnop/logging)
[![codecov for RichardKnop/logging](https://codecov.io/gh/RichardKnop/logging/branch/master/graph/badge.svg)](https://codecov.io/gh/RichardKnop/logging)

---

Log levels:

- `INFO` (blue)
- `WARNING` (pink)
- `ERROR` (red)
- `FATAL` (red)

Formatters:

- `DefaultFormatter`
- `ColouredFormatter`

Example usage. Create a new package `log` in your app such that:

```go
package log

import (
	"github.com/RichardKnop/logging"
)

var (
	logger = logging.New(nil, nil, new(logging.ColouredFormatter))

	// INFO ...
	INFO = logger[logging.INFO]
	// WARNING ...
	WARNING = logger[logging.WARNING]
	// ERROR ...
	ERROR = logger[logging.ERROR]
	// FATAL ...
	FATAL = logger[logging.FATAL]
)
```

Then from your app you could do:

```go
package main

import (
	"github.com/yourusername/yourapp/log"
)

func main() {
	log.INFO.Print("log message")
}
```
