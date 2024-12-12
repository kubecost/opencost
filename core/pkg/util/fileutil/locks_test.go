package fileutil

import (
	"os"
	"path/filepath"
	"testing"
)

// Make sure read works on file created without locking logic
func TestReadLocked(t *testing.T) {
	toWrite := "hello world"
	dir := t.TempDir()
	filename := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(filename, []byte(toWrite), 0600); err != nil {
		t.Fatalf("failed to write test data: %s", err)
	}

	read, err := ReadLocked(filename)
	if err != nil {
		t.Fatalf("Failed to read: %s", err)
	}
	sread := string(read)

	if toWrite != sread {
		t.Errorf("Expected read data to be '%s' but was '%s'", toWrite, sread)
	}
}

// Does not test concurrency, just makes sure the basic read write functionality
// works
func TestRWLocked(t *testing.T) {
	toWrite := "hello world"
	dir := t.TempDir()
	filename := filepath.Join(dir, "test.txt")

	if _, err := WriteLocked(filename, []byte(toWrite)); err != nil {
		t.Fatalf("Failed to write: %s", err)
	}

	read, err := ReadLocked(filename)
	if err != nil {
		t.Fatalf("Failed to read: %s", err)
	}
	sread := string(read)

	if toWrite != sread {
		t.Errorf("Expected read data to be '%s' but was '%s'", toWrite, sread)
	}
}

func TestReadLockedFDMiddlePosition(t *testing.T) {
	toWrite := "hello world"
	dir := t.TempDir()
	filename := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(filename, []byte(toWrite), 0600); err != nil {
		t.Fatalf("failed to write test data: %s", err)
	}

	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("opening after write: %s", err)
	}
	if _, err := f.Seek(3, 0); err != nil {
		t.Fatalf("seeking: %s", err)
	}

	read, err := ReadLockedFD(f)
	if err != nil {
		t.Fatalf("Failed to read: %s", err)
	}
	sread := string(read)

	if toWrite != sread {
		t.Errorf("Expected read data to be '%s' but was '%s'", toWrite, sread)
	}
}

func TestWriteLockedFDMiddlePosition(t *testing.T) {
	toWrite := "hello world"
	toWriteOver := "goodbye land"
	dir := t.TempDir()
	filename := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(filename, []byte(toWrite), 0600); err != nil {
		t.Fatalf("failed to write test data: %s", err)
	}

	f, err := os.OpenFile(filename, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("opening after write: %s", err)
	}
	if _, err := f.Seek(3, 0); err != nil {
		t.Fatalf("seeking: %s", err)
	}

	if _, err := WriteLockedFD(f, []byte(toWriteOver)); err != nil {
		t.Fatalf("writing over: %s", err)
	}

	read, err := ReadLockedFD(f)
	if err != nil {
		t.Fatalf("Failed to read: %s", err)
	}
	sread := string(read)

	if toWriteOver != sread {
		t.Errorf("Expected read data to be '%s' but was '%s'", toWriteOver, sread)
	}
}
