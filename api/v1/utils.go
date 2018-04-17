package v1

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/storage"
)

func respondWithError(rw http.ResponseWriter, err error) {
	respondWith(rw, http.StatusBadRequest, fmt.Sprintf("error occured: %s", err))
}

func respondWith(rw http.ResponseWriter, sc int, msg string) {
	rw.WriteHeader(sc)
	fmt.Fprint(rw, msg)
}

func decodeInto(rc io.ReadCloser, v interface{}) error {
	decoder := json.NewDecoder(rc)
	err := decoder.Decode(v)
	if err != nil {
		return fmt.Errorf("error while decoding into struct: %s", err)
	}
	rc.Close()
	return nil
}

func newROVolume(from, to string) (*container.Volume, error) {
	return newVolume(from, to, "RO")
}
func newRWVolume(from, to string) (*container.Volume, error) {
	return newVolume(from, to, "RW")
}
func newVolume(from, to, mode string) (*container.Volume, error) {
	pi, err := storage.Path(from)
	if err != nil {
		return nil, fmt.Errorf("invalid path %q: %s", from, err)
	}
	return &container.Volume{
		From: pi.Abs(),
		To:   to + pi.Relative(),
		Mode: mode,
	}, nil
}
