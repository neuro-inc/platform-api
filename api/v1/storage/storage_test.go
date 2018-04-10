package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
)

var testDir = "./testData"

func TestMain(m *testing.M) {
	if err := os.MkdirAll(testDir, 0700); err != nil {
		log.Fatalf("unable to create dir %q: %s", testDir, err)
	}
	retCode := m.Run()
	if err := os.RemoveAll(testDir); err != nil {
		log.Fatalf("cannot remove %q: %s", testDir, err)
	}
	os.Exit(retCode)
}

func TestPath_Positive(t *testing.T) {
	if err := Init("./testData"); err != nil {
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
	}
	path, err := filepath.Abs(testDir)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	expected := path + "/path/to/folder"
	for _, src := range testCases {
		path, err := Path(src)
		if err != nil {
			t.Fatalf("unexpected err: %s", err)
		}
		if path != expected {
			t.Fatalf("got %q; expected %q", path, expected)
		}
	}
}

func TestPath_Negative(t *testing.T) {
	if err := Init("./testData"); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	testCases := []string{
		"storage:/path/to/folder",
		"stor://path/to/folder/",
		"path/to/folder",
	}
	for _, src := range testCases {
		_, err := Path(src)
		if err == nil {
			t.Fatalf("expected to get err; got nil instead")
		}
	}
}
