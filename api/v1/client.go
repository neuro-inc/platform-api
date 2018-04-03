package v1

import (
	"fmt"
	"github.com/neuromation/platform-api/log"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type singularityClient struct {
	c    http.Client
	addr *url.URL
}

var client *singularityClient

func Init(addr string, timeout time.Duration) {
	u, err := url.Parse(addr)
	if err != nil {
		log.Fatalf("cannot parse singularity addr %q: %s", addr, err)
	}

	client = &singularityClient{
		c: http.Client{
			Timeout: timeout,
		},
		addr: u,
	}
}

func (c *singularityClient) Deploy(j *Job) error {
	// TODO: must be replaced with smthng rly unique
	reqID := fmt.Sprintf("platform_request_%d", time.Now().Nanosecond())
	if err := c.registerRequest(reqID); err != nil {
		return fmt.Errorf("error while registering singularity request: %s", err)
	}

	j.Deploy.RequestID = reqID
	if err := c.registerDeploy(reqID, j); err != nil {
		return fmt.Errorf("error while registering singularity deploy: %s", err)
	}

	return nil
}

var requestTpl = `
{
  "id": "%s",
  "owners": [
    "you@example.com"
  ],
  "requestType": "RUN_ONCE",
  "rackSensitive": false,
  "loadBalanced": false
}
`

func (c *singularityClient) registerRequest(id string) error {
	body := fmt.Sprintf(requestTpl, id)
	r := strings.NewReader(body)
	addr := fmt.Sprintf("%s/singularity/api/requests", c.addr.String())
	req, err := http.NewRequest("POST", addr, r)
	if err != nil {
		return fmt.Errorf("err while creating singularity request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.c.Do(req)
	if err != nil {
		return fmt.Errorf("error while registering request %q at %q: %s", body, c.addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		responseBody, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code returned from %q: %d. Response body: %q",
			addr, resp.StatusCode, responseBody)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("cannot read response body: %s", err)
	}
	log.Infof("request response: %s", string(respBody))
	return nil
}

func (c *singularityClient) registerDeploy(reqID string, j *Job) error {
	body := fmt.Sprintf("%s", j)
	log.Infof("%s", body)

	r := strings.NewReader(body)
	addr := fmt.Sprintf("%s/singularity/api/deploys", c.addr.String())
	req, err := http.NewRequest("POST", addr, r)
	if err != nil {
		return fmt.Errorf("err while creating singularity deploy: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.c.Do(req)
	if err != nil {
		return fmt.Errorf("error while registering deploy %q at %q: %s", body, c.addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		responseBody, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code returned from %q: %d. Response body: %q",
			addr, resp.StatusCode, responseBody)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("cannot read response body: %s", err)
	}
	log.Infof("deploy response: %s", string(respBody))
	return nil
}
