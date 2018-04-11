package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
)

var testDir = "../testData/storage"

func TestMain(m *testing.M) {
	testPath := testDir + "/path/to/folder"
	if err := os.MkdirAll(testPath, 0700); err != nil {
		log.Fatalf("unable to create dir %q: %s", testDir, err)
	}
	retCode := m.Run()
	if err := os.RemoveAll(testDir); err != nil {
		log.Fatalf("cannot remove %q: %s", testDir, err)
	}
	os.Exit(retCode)
}

func TestPath_Positive(t *testing.T) {
	if err := Init(testDir); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	tempDir := fmt.Sprintf("%s/path/to/folder", testDir)
	if err := os.MkdirAll(tempDir, 0700); err != nil {
		t.Fatalf("unable to create dir %q: %s", tempDir, err)
	}
	testCases := []string{
		"storage://path/to/folder",
		"storage://path/to/folder/",
		"storage://path/to/folder//",
		"storage://path/to///folder//",
		"marketplace://path/to/folder",
		"marketplace://path/to///folder//",
	}
	path, err := filepath.Abs(testDir)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	expectedAbs := path + "/path/to/folder"
	expectedRelative := "path/to/folder"
	for _, src := range testCases {
		pi, err := Path(src)
		if err != nil {
			t.Fatalf("unexpected err: %s", err)
		}
		if pi.Origin() != src {
			t.Fatalf("got origin %q; expected %q", pi.Origin(), src)
		}
		if pi.Abs() != expectedAbs {
			t.Fatalf("got abs %q; expected %q", pi.Abs(), expectedAbs)
		}
		if pi.Relative() != expectedRelative {
			t.Fatalf("got relative %q; expected %q", pi.Relative(), expectedRelative)
		}
	}
}

func TestPath_Negative(t *testing.T) {
	if err := Init(testDir); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	testCases := []string{
		"storage:/path/to/folder",
		"stor://path/to/folder/",
		"marketPlace://path/to/folder/",
		"mrktpkace://path/to/folder/",
		"path/to/folder",
	}
	for _, src := range testCases {
		_, err := Path(src)
		if err == nil {
			t.Fatalf("expected to get err; got nil instead")
		}
	}
}
