package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"

	"github.com/neuromation/platform-api/api/v1/orchestrator"
	"github.com/neuromation/platform-api/api/v1/status"
)

// by given status id, obtain existing old status object
// if it is PENDING, get the corresponding jobId and retrieve the current
// status
// create a new Status object
// save via the StatusService
// return the corresponding payload
func ViewStatus(jobClient orchestrator.Client, statusService status.StatusService) httprouter.Handle {
	return func(rw http.ResponseWriter, _ *http.Request, params httprouter.Params) {
		status, err := statusService.Get(params.ByName("id"))
		if err != nil {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		payload, err := json.Marshal(status)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		if status.IsSucceeded() && status.IsRedirectionSupported() {
			rw.WriteHeader(http.StatusSeeOther)
			// TODO: generate a correct URL
			rw.Header().Set("Location", "http://domain.com")
		} else {
			rw.WriteHeader(http.StatusOK)
		}
		fmt.Fprint(rw, payload)


		// TODO: pattern matching etc
		// check if it is required to query the status from Singularity
		// query the status
		// update the status
		// generate a payload and return
	}
}
