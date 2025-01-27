package prom

// warning represents an unexpected result that occurs but doesn't halt processing
type warning interface {
	Message() string
}

// defaultWarning is a simple implementation for warning
type defaultWarning struct {
	message string
}

// Message returns the message for the warning
func (dw *defaultWarning) Message() string {
	return dw.message
}

// Stringer implementation
func (dw *defaultWarning) String() string {
	return dw.message
}

// Creates a warning for the prom package. NOTE: We can make this less prom-centric if desirable.
func newWarning(msg string) warning {
	return &defaultWarning{msg}
}
