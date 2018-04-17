package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/julienschmidt/httprouter"

	"github.com/neuromation/platform-api/api/v1/status"
)

func ViewStatus(statusService status.StatusService) httprouter.Handle {
	return func(rw http.ResponseWriter, req *http.Request, params httprouter.Params) {
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

		if status.IsSucceeded() && status.IsHttpRedirectSupported() {
			rw.Header().Set("Location", status.HttpRedirectUrl())
			rw.WriteHeader(http.StatusSeeOther)
		} else {
			rw.WriteHeader(http.StatusOK)
		}
		rw.Write(payload)
	}
}

func GenerateStatusURLFromRequest(req *http.Request, statusId string) url.URL {
	// TODO (A Danshyn 04/15): SSL/TLS is ignored for now
	return url.URL{
		Scheme: "http",
		Host:   req.Host,
		Path:   fmt.Sprintf("/statuses/%s", statusId),
	}
}
