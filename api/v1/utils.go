package v1

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
		return err
	}
	rc.Close()
	return nil
}

func requiredError(field string) error {
	return fmt.Errorf("field %q required to be set", field)
}
