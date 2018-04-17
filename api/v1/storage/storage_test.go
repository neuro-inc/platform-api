package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
)

var testDir = "../testdata/storage"

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
	path, err := filepath.Abs(testDir)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	testCases := []struct {
		src, abs, rel, orig string
	}{
		{
			src:  "storage://path/to/folder",
			abs:  path + "/path/to/folder",
			rel:  "path/to/folder",
			orig: "storage://path/to/folder",
		},
		{
			src:  testDir + "/path/to/folder",
			abs:  path + "/path/to/folder",
			rel:  "path/to/folder",
			orig: testDir + "/path/to/folder",
		},
		{
			src:  "storage://path",
			abs:  path + "/path",
			rel:  "path",
			orig: "storage://path",
		},
	}
	for _, tc := range testCases {
		pi, err := Path(tc.src)
		if err != nil {
			t.Fatalf("unexpected err: %s", err)
		}
		if pi.Origin() != tc.orig {
			t.Fatalf("got origin %q; expected %q", pi.Origin(), tc.orig)
		}
		if pi.Abs() != tc.abs {
			t.Fatalf("got abs %q; expected %q", pi.Abs(), tc.abs)
		}
		if pi.Relative() != tc.rel {
			t.Fatalf("got relative %q; expected %q", pi.Relative(), tc.rel)
		}
	}
}

func TestPath_Negative(t *testing.T) {
	if err := Init(testDir); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	testCases := []string{
		"storage:/path/to/folder",
		"storage:/path/to/folder/",
		"storage://path/to/folder/",
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
