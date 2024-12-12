package storage

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/opencost/opencost/core/pkg/util/json"
)

// This suite of integration tests is meant to validate if an implementation of Storage that relies on a could
// bucket service properly implements the interface. To run these tests the env variable "TEST_BUCKET_CONFIG"
// must be set with the path to a valid bucket config as defined in the NewBucketStorage() function.

type testFileContent struct {
	Field1 int    `json:"field_1"`
	Field2 string `json:"field_2"`
}

var tfc = testFileContent{
	Field1: 101,
	Field2: "TEST_FILE_CONTENT",
}

const testpath = "opencost/storage/"

func createStorage(configPath string) (Storage, error) {

	bytes, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	store, err := NewBucketStorage(bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	return store, nil
}

func createFiles(files []string, testName string, store Storage) error {
	b, err := json.Marshal(tfc)
	if err != nil {
		return fmt.Errorf("failed to marshal file content: %w", err)
	}

	for _, fileName := range files {
		filePath := path.Join(testpath, testName, fileName)
		err = store.Write(filePath, b)
		if err != nil {
			return fmt.Errorf("failed to write file '%s': %w ", filePath, err)
		}
	}

	return nil
}

func cleanupFiles(files []string, testName string, store Storage) error {
	for _, fileName := range files {
		filePath := path.Join(testpath, testName, fileName)
		err := store.Remove(filePath)
		if err != nil {
			return fmt.Errorf("failed to remove file '%s': %w ", filePath, err)
		}
	}

	return nil
}

func TestBucketStorage_List(t *testing.T) {
	configPath := os.Getenv("TEST_BUCKET_CONFIG")
	if configPath == "" {
		t.Skip("skipping integration test, set environment variable TEST_BUCKET_CONFIG")
	}
	store, err := createStorage(configPath)
	if err != nil {
		t.Errorf("failed to create storage: %s", err.Error())
		return
	}

	testName := "list"

	fileNames := []string{
		"/file0.json",
		"/file1.json",
		"/dir0/file2.json",
		"/dir0/file3.json",
	}

	err = createFiles(fileNames, testName, store)
	if err != nil {
		t.Errorf("failed to create files: %s", err)
	}

	defer func() {
		err = cleanupFiles(fileNames, testName, store)
		if err != nil {
			t.Errorf("failed to clean up files: %s", err)
		}
	}()

	testCases := map[string]struct {
		path      string
		expected  []string
		expectErr bool
	}{
		"base dir files": {
			path: path.Join(testpath, testName),
			expected: []string{
				"file0.json",
				"file1.json",
			},
			expectErr: false,
		},
		"single nested dir files": {
			path: path.Join(testpath, testName, "dir0"),
			expected: []string{
				"file2.json",
				"file3.json",
			},
			expectErr: false,
		},
		"nonexistent dir files": {
			path:      path.Join(testpath, testName, "dir1"),
			expected:  []string{},
			expectErr: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			fileList, err := store.List(tc.path)
			if tc.expectErr == (err == nil) {
				if tc.expectErr {
					t.Errorf("expected error was not thrown")
					return
				}
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			if len(fileList) != len(tc.expected) {
				t.Errorf("file list length does not match expected length, actual: %d, expected: %d", len(fileList), len(tc.expected))
			}

			expectedSet := map[string]struct{}{}
			for _, expName := range tc.expected {
				expectedSet[expName] = struct{}{}
			}

			for _, file := range fileList {
				_, ok := expectedSet[file.Name]
				if !ok {
					t.Errorf("unexpect file in list %s", file.Name)
				}

				if file.Size == 0 {
					t.Errorf("file size is not set")
				}

				if file.ModTime.IsZero() {
					t.Errorf("file mod time is not set")
				}
			}
		})
	}
}

func TestBucketStorage_ListDirectories(t *testing.T) {
	configPath := os.Getenv("TEST_BUCKET_CONFIG")
	if configPath == "" {
		t.Skip("skipping integration test, set environment variable TEST_BUCKET_CONFIG")
	}
	store, err := createStorage(configPath)
	if err != nil {
		t.Errorf("failed to create storage: %s", err.Error())
		return
	}

	testName := "list_directories"

	fileNames := []string{
		"/file0.json",
		"/dir0/file2.json",
		"/dir0/file3.json",
		"/dir0/dir1/file4.json",
		"/dir0/dir2/file5.json",
	}

	err = createFiles(fileNames, testName, store)
	if err != nil {
		t.Errorf("failed to create files: %s", err)
	}

	defer func() {
		err = cleanupFiles(fileNames, testName, store)
		if err != nil {
			t.Errorf("failed to clean up files: %s", err)
		}
	}()

	testCases := map[string]struct {
		path      string
		expected  []string
		expectErr bool
	}{
		"base dir dir": {
			path: path.Join(testpath, testName),
			expected: []string{
				path.Join(testpath, testName, "dir0") + "/",
			},
			expectErr: false,
		},
		"single nested dir files": {
			path: path.Join(testpath, testName, "dir0"),
			expected: []string{
				path.Join(testpath, testName, "dir0", "dir1") + "/",
				path.Join(testpath, testName, "dir0", "dir2") + "/",
			},
			expectErr: false,
		},
		"dir with no sub dirs": {
			path:      path.Join(testpath, testName, "dir0/dir1"),
			expected:  []string{},
			expectErr: false,
		},
		"non-existent dir": {
			path:      path.Join(testpath, testName, "dir1"),
			expected:  []string{},
			expectErr: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			dirList, err := store.ListDirectories(tc.path)
			if tc.expectErr == (err == nil) {
				if tc.expectErr {
					t.Errorf("expected error was not thrown")
					return
				}
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			if len(dirList) != len(tc.expected) {
				t.Errorf("dir list length does not match expected length, actual: %d, expected: %d", len(dirList), len(tc.expected))
			}

			expectedSet := map[string]struct{}{}
			for _, expName := range tc.expected {
				expectedSet[expName] = struct{}{}
			}

			for _, dir := range dirList {
				_, ok := expectedSet[dir.Name]
				if !ok {
					t.Errorf("unexpect dir in list %s", dir.Name)
				}
			}
		})
	}
}

func TestBucketStorage_Exists(t *testing.T) {
	configPath := os.Getenv("TEST_BUCKET_CONFIG")
	if configPath == "" {
		t.Skip("skipping integration test, set environment variable TEST_BUCKET_CONFIG")
	}
	store, err := createStorage(configPath)
	if err != nil {
		t.Errorf("failed to create storage: %s", err.Error())
		return
	}

	testName := "exists"

	fileNames := []string{
		"/file0.json",
	}

	err = createFiles(fileNames, testName, store)
	if err != nil {
		t.Errorf("failed to create files: %s", err)
	}

	defer func() {
		err = cleanupFiles(fileNames, testName, store)
		if err != nil {
			t.Errorf("failed to clean up files: %s", err)
		}
	}()

	testCases := map[string]struct {
		path      string
		expected  bool
		expectErr bool
	}{
		"file exists": {
			path:      path.Join(testpath, testName, "file0.json"),
			expected:  true,
			expectErr: false,
		},
		"file does not exist": {
			path:      path.Join(testpath, testName, "file1.json"),
			expected:  false,
			expectErr: false,
		},
		"dir does not exist": {
			path:      path.Join(testpath, testName, "dir0/file.json"),
			expected:  false,
			expectErr: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			exists, err := store.Exists(tc.path)
			if tc.expectErr == (err == nil) {
				if tc.expectErr {
					t.Errorf("expected error was not thrown")
					return
				}
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			if exists != tc.expected {
				t.Errorf("file exists output did not match expected")
			}
		})
	}
}

func TestBucketStorage_Read(t *testing.T) {
	configPath := os.Getenv("TEST_BUCKET_CONFIG")
	if configPath == "" {
		t.Skip("skipping integration test, set environment variable TEST_BUCKET_CONFIG")
	}
	store, err := createStorage(configPath)
	if err != nil {
		t.Errorf("failed to create storage: %s", err.Error())
		return
	}

	testName := "read"

	fileNames := []string{
		"/file0.json",
	}

	err = createFiles(fileNames, testName, store)
	if err != nil {
		t.Errorf("failed to create files: %s", err)
	}

	defer func() {
		err = cleanupFiles(fileNames, testName, store)
		if err != nil {
			t.Errorf("failed to clean up files: %s", err)
		}
	}()

	testCases := map[string]struct {
		path      string
		expectErr bool
	}{
		"file exists": {
			path:      path.Join(testpath, testName, "file0.json"),
			expectErr: false,
		},
		"file does not exist": {
			path:      path.Join(testpath, testName, "file1.json"),
			expectErr: true,
		},
		"dir does not exist": {
			path:      path.Join(testpath, testName, "dir0/file.json"),
			expectErr: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			b, err := store.Read(tc.path)
			if tc.expectErr && err != nil {
				return
			}
			if tc.expectErr == (err == nil) {
				if tc.expectErr {
					t.Errorf("expected error was not thrown")
					return
				}
				t.Errorf("unexpected error: %s", err.Error())
				return
			}
			var content testFileContent
			err = json.Unmarshal(b, &content)
			if err != nil {
				t.Errorf("could not unmarshal file content")
				return
			}

			if content != tfc {
				t.Errorf("file content did not match writen value")
			}
		})
	}
}

func TestBucketStorage_Stat(t *testing.T) {
	configPath := os.Getenv("TEST_BUCKET_CONFIG")
	if configPath == "" {
		t.Skip("skipping integration test, set environment variable TEST_BUCKET_CONFIG")
	}
	store, err := createStorage(configPath)
	if err != nil {
		t.Errorf("failed to create storage: %s", err.Error())
		return
	}

	testName := "stat"

	fileNames := []string{
		"/file0.json",
	}

	err = createFiles(fileNames, testName, store)
	if err != nil {
		t.Errorf("failed to create files: %s", err)
	}

	defer func() {
		err = cleanupFiles(fileNames, testName, store)
		if err != nil {
			t.Errorf("failed to clean up files: %s", err)
		}
	}()

	testCases := map[string]struct {
		path      string
		expected  *StorageInfo
		expectErr bool
	}{
		"base dir": {
			path: path.Join(testpath, testName, "file0.json"),
			expected: &StorageInfo{
				Name: "file0.json",
				Size: 45,
			},
			expectErr: false,
		},
		"file does not exist": {
			path:      path.Join(testpath, testName, "file1.json"),
			expected:  nil,
			expectErr: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			status, err := store.Stat(tc.path)
			if tc.expectErr && err != nil {
				return
			}
			if tc.expectErr == (err == nil) {
				if tc.expectErr {
					t.Errorf("expected error was not thrown")
					return
				}
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			if status.Name != tc.expected.Name {
				t.Errorf("status name did name match expected, actual: %s, expected: %s", status.Name, tc.expected.Name)
			}

			if status.Size != tc.expected.Size {
				t.Errorf("status name did size match expected, actual: %d, expected: %d", status.Size, tc.expected.Size)
			}

			if status.ModTime.IsZero() {
				t.Errorf("status mod time is not set")
			}

		})
	}
}
