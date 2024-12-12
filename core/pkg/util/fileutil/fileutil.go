package fileutil

import (
	"os"
)

// File exists has three different return cases that should be handled:
//  1. File exists and is not a directory (true, nil)
//  2. File does not exist (false, nil)
//  3. File may or may not exist. Error occurred during stat (false, error)
//
// The third case represents the scenario where the stat returns an error,
// but the error isn't relevant to the path. This can happen when the current
// user doesn't have permission to access the file.
func FileExists(filename string) (bool, error) {
	info, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	return !info.IsDir(), nil
}
