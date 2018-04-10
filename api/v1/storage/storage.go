package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var basePath string

// Init checks passed path and set it as base
func Init(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("unable to access %q: %s", path, err)
	}
	basePath, err = filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("unable to find abs path %q: %s", path, err)
	}
	return nil
}

var pathRegexp = regexp.MustCompile("^(storage://)(.+)(/[^/ ]*)+/?$")

// Path check passed addr and returns converted
func Path(src string) (string, error) {
	match := pathRegexp.FindAllStringSubmatch(src, -1)
	if match == nil {
		return "", fmt.Errorf("passed path %q has wrong format", src)
	}
	slice := match[0]
	slice[1] = basePath
	slice = slice[1:]
	path := strings.Join(slice, "/")
	path = filepath.Clean(path)
	_, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("unable to access %q: %s", path, err)
	}
	return filepath.Clean(path), nil
}
