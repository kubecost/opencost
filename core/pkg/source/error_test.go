package source

import (
	"errors"
	"fmt"
	"testing"
)

func newCommError() error {
	return NewCommError("Test Communication Error")
}

func newErrorCollection() error {
	qc := &QueryErrorCollector{}

	qc.Report("test_query1", nil, NewCommError("Failed to connect"), nil)
	qc.Report("test_query2", nil, NewCommError("Failed to connect"), errors.New("Parsing error"))
	qc.Report("test_query3", nil, nil, errors.New("Failed to parse field 'foo'"))

	return qc
}

func newNestedError() error {
	comErr := NewCommError("Communication Error")
	e1 := fmt.Errorf("Wrap Error #1: %w", comErr)
	e2 := fmt.Errorf("Wrap Error #2: %w", e1)
	return e2
}

func TestErrorCollectionCheck(t *testing.T) {
	err := newErrorCollection()

	if !IsErrorCollection(err) {
		t.Fatalf("IsErrorCollection() returned false, expected true")
		return
	}
}

func TestNestedErrorAs(t *testing.T) {
	err := newNestedError()

	var commErr CommError
	if !errors.As(err, &commErr) {
		t.Fatalf("Expected there to exist a CommError, but failed.")
		return
	}
}

func TestErrorCollectionErrorAs(t *testing.T) {
	err := newErrorCollection()

	var commErr CommError
	if !errors.As(err, &commErr) {
		t.Fatalf("Expected there to exist a CommError, but failed.")
		return
	}
}

func TestCommErrorAs(t *testing.T) {
	err := newCommError()

	var commErr CommError
	if !errors.As(err, &commErr) {
		t.Fatalf("Expected there to exist a CommError, but failed.")
		return
	}
}

func TestAllErrorsFor(t *testing.T) {
	err := newErrorCollection()
	if !IsErrorCollection(err) {
		t.Fatalf("Error is not ErrorCollection")
		return
	}
	collection := err.(QueryErrorCollection)
	allErrors := AllErrorsFor(collection)

	// Expected Errors Length
	const expected = 4

	if len(allErrors) != expected {
		t.Fatalf("All Errors Length was: %d, Expected %d", len(allErrors), expected)
		return
	}
}
