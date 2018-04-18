package storage

import (
	"fmt"
	"github.com/neuromation/platform-api/log"
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

var pathRegexp = regexp.MustCompile(`^(storage):/((\/[\w]+)+)$`)

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
	if len(basePath) == 0 {
		log.Fatalf("BUG: forgot to call Init ?")
	}

	var (
		abs  string
		path string
		err  error
	)

	match := pathRegexp.FindAllStringSubmatch(src, -1)
	if match != nil {
		path = match[0][2]
		// skip first slash
		path = path[1:]
		abs = filepath.Clean(fmt.Sprintf("%s/%s", basePath, path))
	} else {
		abs, err = filepath.Abs(src)
		if err != nil {
			return nil, err
		}
		path, err = filepath.Rel(basePath, abs)
		if err != nil {
			return nil, err
		}
	}

	if _, err = os.Stat(abs); err != nil {
		return nil, fmt.Errorf("unable to access %q: %s", abs, err)
	}

	return &PathInfo{
		abs:      abs,
		relative: filepath.Clean(path),
		origin:   src,
	}, nil
}
