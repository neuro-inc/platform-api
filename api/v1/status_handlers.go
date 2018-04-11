package v1

import (
	"net/http"

	"github.com/julienschmidt/httprouter"

	// "github.com/neuromation/platform-api/api/v1/status"
)

func ViewStatus(statusService StatusService) httprouter.Handle {
	return func(rw http.ResponseWriter, _ *http.Request, params httprouter.Params) {
		_, err := statusService.Get(params.ByName("id"))
		if err != nil {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		// check if it is required to query the status from Singularity
		// query the status
		// update the status
		// generate a payload and return
	}
}
