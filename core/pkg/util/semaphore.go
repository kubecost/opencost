package util

// Semaphore implements a non-weighted semaphore for restricting
// concurrent access to a limited number of processes.
type Semaphore struct {
	s chan bool
}

// Acquire blocks until access can be granted to the caller
func (s *Semaphore) Acquire() {
	s.s <- true
}

// Return releases access from the caller, opening it for acquisition
func (s *Semaphore) Return() {
	<-s.s
}

// NewSemaphore creates a new Semaphore that allows max number of
// concurrent access
func NewSemaphore(max int) *Semaphore {
	return &Semaphore{
		s: make(chan bool, max),
	}
}
