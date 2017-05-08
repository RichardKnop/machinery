package logging

// LoggerInterface will accept stdlib logger and a custom logger.
// There's no standard interface, this is the closest we get, unfortunately.
type LoggerInterface interface {
	Print(...interface{})
	Printf(string, ...interface{})
	Println(...interface{})

	Fatal(...interface{})
	Fatalf(string, ...interface{})
	Fatalln(...interface{})

	Panic(...interface{})
	Panicf(string, ...interface{})
	Panicln(...interface{})
}
