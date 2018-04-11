package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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

var pathRegexp = regexp.MustCompile(`^(storage|marketplace):/((\/[\w]+)+)$`)

// PathInfo contains path data from passed storage binding
type PathInfo struct {
	// absolute path
	abs string
	// relative path
	relative string
	// origin path
	origin string
}

// Abs return absolute path
func (pi PathInfo) Abs() string { return pi.abs }

// Relative return relative path
func (pi PathInfo) Relative() string { return pi.relative }

// Origin return origin path
func (pi PathInfo) Origin() string { return pi.origin }

// Path check passed addr and returns converted
func Path(src string) (*PathInfo, error) {
	match := pathRegexp.FindAllStringSubmatch(src, -1)
	if match == nil {
		return nil, fmt.Errorf("passed path %q has wrong format", src)
	}
	// get only sufficient values
	path := match[0][2]
	pi := &PathInfo{
		abs:      filepath.Clean(fmt.Sprintf("%s/%s", basePath, path)),
		relative: filepath.Clean(path),
		origin:   src,
	}
	_, err := os.Stat(pi.Abs())
	if err != nil {
		return nil, fmt.Errorf("unable to access %q: %s", pi.abs, err)
	}
	return pi, nil
}
