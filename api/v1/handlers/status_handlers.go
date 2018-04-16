package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/julienschmidt/httprouter"

	"github.com/neuromation/platform-api/api/v1/orchestrator"
	"github.com/neuromation/platform-api/api/v1/status"
)

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
	}
}

func GenerateStatusURLFromRequest(req *http.Request, statusId string) url.URL {
	// TODO (A Danshyn 04/15): SSL/TLS is ignored for now
	return url.URL{
		Scheme: "http",
		Host: req.Host,
		Path: fmt.Sprintf("/statuses/%s", statusId),
	}
}
