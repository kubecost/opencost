//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd

// The above platforms support flock()

package fileutil

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/opencost/opencost/core/pkg/log"
)

// WriteLockedFD uses the flock() syscall to safely write to an open file as
// long as other users of the file are also using flock()-based access.
//
// WriteLocked will block until it gets lock access.
//
// The file will be truncated before writing and at the end of writing the
// FD will be reset to position 0.
//
// For the reasons outlined best in https://lwn.net/Articles/586904/ this uses
// flock() instead of fcntl(). The ability to lock byte ranges is not necessary
// and flock() has better behavior.
func WriteLockedFD(f *os.File, data []byte) (int, error) {
	// For the reasons outlined best in https://lwn.net/Articles/586904/ we're
	// going to use flock() instead of fcntl() because we want a whole-file lock.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return 0, fmt.Errorf("unexpected error flock()-ing with EX: %w", err)
	}
	defer func() {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
			log.Errorf("unexpected error flock()-ing FD %d with UN after writing: %s", f.Fd(), err)
		}
	}()

	if err := f.Truncate(0); err != nil {
		return 0, fmt.Errorf("truncating: %w", err)
	}

	if _, err := f.Seek(0, 0); err != nil {
		return 0, fmt.Errorf("seeking to 0 before write: %w", err)
	}
	defer func() {
		if _, err := f.Seek(0, 0); err != nil {
			log.Errorf("unexpected error seeking to 0 after write on FD %d: %s", f.Fd(), err)
		}
	}()

	n, err := f.Write(data)
	if err != nil {
		return n, fmt.Errorf("writing data: %w", err)
	}

	return n, nil
}

// WriteLocked opens the file and then calls WriteLockedFD.
func WriteLocked(filename string, data []byte) (int, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return 0, fmt.Errorf("opening %s: %w", filename, err)
	}
	defer file.Close()

	return WriteLockedFD(file, data)
}

// ReadLockedFD uses the flock() syscall to safely read from an open file as
// long as other users of the file are also using flock()-based access.
//
// ReadLockedFD will block until it gets lock access.
//
// This will read the file in full, from 0, regardless of the current
// position and then reset the position to 0.
//
// For the reasons outlined best in https://lwn.net/Articles/586904/ this uses
// flock() instead of fcntl(). The ability to lock byte ranges is not necessary
// and flock() has better behavior.
func ReadLockedFD(f *os.File) ([]byte, error) {
	// For the reasons outlined best in https://lwn.net/Articles/586904/ we're
	// going to use flock() instead of fcntl() because we want a whole-file lock.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		return nil, fmt.Errorf("unexpected error flock()-ing with SH: %w", err)
	}
	defer func() {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
			log.Errorf("unexpected error flock()-ing FD %d with UN reading: %s", f.Fd(), err)
		}
	}()

	if _, err := f.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("seeking to 0 before read: %w", err)
	}
	defer func() {
		if _, err := f.Seek(0, 0); err != nil {
			log.Errorf("unexpected error seeking to 0 after read on FD %d: %s", f.Fd(), err)
		}
	}()

	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, f); err != nil {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
			log.Errorf("unexpected error flock()-ing with UN after error reading: %s", err)
		}
		return nil, fmt.Errorf("copying data out of file: %w", err)
	}

	return buf.Bytes(), nil
}

// ReadLocked opens the given file and then calls ReadLockedFD.
func ReadLocked(filename string) ([]byte, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", filename, err)
	}
	defer file.Close()

	return ReadLockedFD(file)
}
