package logging

// DefaultFormatter adds filename and line number before the log message
type DefaultFormatter struct {
}

// GetPrefix returns ""
func (f *DefaultFormatter) GetPrefix(lvl level) string {
	return ""
}

// GetSuffix returns ""
func (f *DefaultFormatter) GetSuffix(lvl level) string {
	return ""
}

// Format adds filename and line number before the log message
func (f *DefaultFormatter) Format(lvl level, v ...interface{}) []interface{} {
	return append([]interface{}{header()}, v...)
}
