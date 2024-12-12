package util

type stackNode[T any] struct {
	value    T
	previous *stackNode[T]
}

type Stack[T any] struct {
	top *stackNode[T]

	length int
}

// NewStack creates a new Stack[T]
func NewStack[T any]() *Stack[T] {
	return &Stack[T]{
		top:    nil,
		length: 0,
	}
}

// Push adds a value to the top of the stack.
func (s *Stack[T]) Push(value T) {
	n := &stackNode[T]{
		value:    value,
		previous: s.top,
	}

	s.top = n
	s.length++
}

// Pop the top item of the stack and return it
func (s *Stack[T]) Pop() T {
	if s.length == 0 {
		return defaultFor[T]()
	}

	n := s.top
	s.top = n.previous
	s.length--

	return n.value
}

// Top returns the item on the top of the stack
func (s *Stack[T]) Top() T {
	if s.length == 0 {
		return defaultFor[T]()
	}

	return s.top.value
}

// Length returns the total number of elements on the stack.
func (s *Stack[T]) Length() int {
	return s.length
}

func defaultFor[T any]() T {
	var t T
	return t
}
