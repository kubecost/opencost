//go:build windows

package fileutil

import (
	"fmt"
	"os"
)

func WriteLockedFD(f *os.File, data []byte) (int, error) {
	return 0, fmt.Errorf("WriteLockedFD is not implemented on Windows. Please open an issue.")
}

func WriteLocked(filename string, data []byte) (int, error) {
	return 0, fmt.Errorf("WriteLocked is not implemented on Windows. Please open an issue.")
}

func ReadLockedFD(f *os.File) ([]byte, error) {
	return nil, fmt.Errorf("ReadLockedFD is not implemented on Windows. Please open an issue.")
}

func ReadLocked(filename string) ([]byte, error) {
	return nil, fmt.Errorf("ReadLocked is not implemented on Windows. Please open an issue.")
}
