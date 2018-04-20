package handlers

import (
	"fmt"
	"net/http"
	"net/url"
)

func GenerateBatchInferenceURLFromRequest(req *http.Request, biID string) url.URL {
	// TODO (A Danshyn 04/16): SSL/TLS is ignored for now
	return url.URL{
		Scheme: "http",
		Host:   req.Host,
		Path:   fmt.Sprintf("/batchInference/%s", biID),
	}
}
