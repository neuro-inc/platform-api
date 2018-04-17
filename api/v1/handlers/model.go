package handlers

import (
	"fmt"
	"net/http"
	"net/url"
)

func GenerateModelURLFromRequest(req *http.Request, modelId string) url.URL {
	// TODO (A Danshyn 04/16): SSL/TLS is ignored for now
	return url.URL{
		Scheme: "http",
		Host: req.Host,
		Path: fmt.Sprintf("/models/%s", modelId),
	}
}
