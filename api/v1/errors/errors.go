package errors

import (
	"fmt"
	"net/http"

	"github.com/neuromation/platform-api/log"
)

const errResponse = `{"error": %q}`

func Respond(rw http.ResponseWriter, sc int, message string, err error) {
	rw.WriteHeader(sc)
	rw.Header().Set("Content-Type", "application/json")
	log.Errorf("%s", err)
	msg := fmt.Sprintf(errResponse, message)
	fmt.Fprint(rw, msg)
}
