package v1

import (
	"fmt"
	"net/http"
)

func respondWithSuccess(rw http.ResponseWriter) {
	respondWith(rw, http.StatusOK, "")
}

func respondWithError(rw http.ResponseWriter, err error) {
	respondWith(rw, http.StatusBadRequest, fmt.Sprintf("error occured: %s", err))
}

func respondWith(rw http.ResponseWriter, sc int, msg string) {
	rw.WriteHeader(sc)
	fmt.Fprint(rw, msg)
}
