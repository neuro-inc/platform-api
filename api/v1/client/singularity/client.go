package singularity

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/orchestrator"
)

type singularityClient struct {
	c    http.Client
	addr *url.URL
}

// NewClient creates new orchestrator.Client from given config
func NewClient(addr string, timeout time.Duration) (orchestrator.Client, error) {
	uri, err := url.ParseRequestURI(addr)
	if err != nil {
		return nil, err
	}
	client := &singularityClient{
		c: http.Client{
			Timeout: timeout,
		},
		addr: uri,
	}
	return client, nil
}

// TODO: NewJob is the method of singularityClient
// but actually Job is something different from client.
func (c *singularityClient) NewJob(container container.Container, res container.Resources) orchestrator.Job {
	id := fmt.Sprintf("platform_deploy_%d", time.Now().Nanosecond())
	j := &singularityJob{
		client: c,
		Deploy: deploy{
			ID: id,
			ContainerInfo: containerInfo{
				Type: "DOCKER",
				Docker: dockerContainer{
					Image: container.Image,
				},
				Volumes: container.Volumes,
			},
			Resources:                  res,
			DeployHealthTimeoutSeconds: 300,
			Env: container.Env,
		},
	}
	return j
}

func (c *singularityClient) GetJob() orchestrator.Job {
	panic("implement me")
}

func (c *singularityClient) SearchJobs() []orchestrator.Job {
	panic("implement me")
}

type singularityJob struct {
	client *singularityClient
	Deploy deploy `json:"deploy"`
}

// String implements the Stringer interface
func (j singularityJob) String() string {
	b, err := json.Marshal(j)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (j *singularityJob) Start() error {
	// TODO: must be replaced with smthng rly unique
	reqID := fmt.Sprintf("platform_request_%d", time.Now().Nanosecond())
	if err := j.client.registerRequest(reqID); err != nil {
		return fmt.Errorf("error while registering singularity request: %s", err)
	}

	j.Deploy.RequestID = reqID
	if err := j.client.registerDeploy(reqID, j); err != nil {
		return fmt.Errorf("error while registering singularity deploy: %s", err)
	}

	return nil
}

func (j *singularityJob) Stop() error {
	panic("implement me")
}

func (j *singularityJob) Delete() error {
	panic("implement me")
}

func (j *singularityJob) Status() (string, error) {
	panic("implement me")
}

func (j *singularityJob) GetID() string {
	return j.Deploy.RequestID
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
	_, err := c.post("requests", body)
	if err != nil {
		return fmt.Errorf("error while registering request %q at %q: %s", body, c.addr, err)
	}
	return nil
}

func (c *singularityClient) registerDeploy(reqID string, job *singularityJob) error {
	body := job.String()
	_, err := c.post("deploys", body)
	if err != nil {
		return fmt.Errorf("error while registering deploy %q at %q: %s", body, c.addr, err)
	}
	return nil
}

func (c *singularityClient) post(addr, body string) (*http.Response, error) {
	r := strings.NewReader(body)
	uri := fmt.Sprintf("%s/singularity/api/%s", c.addr.String(), addr)
	req, err := http.NewRequest("POST", uri, r)
	if err != nil {
		return nil, fmt.Errorf("err while creating singularity post request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req)
}

func (c *singularityClient) get(addr string) (*http.Response, error) {
	uri := fmt.Sprintf("%s/singularity/api/%s", c.addr.String(), addr)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, fmt.Errorf("err while creating singularity GET request: %s", err)
	}
	return c.do(req)
}

func (c *singularityClient) do(req *http.Request) (*http.Response, error) {
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unexpected error returned: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		responseBody, _ := ioutil.ReadAll(resp.Body)
		return resp, fmt.Errorf("unexpected status code returned from %q: %d. Response body: %q",
			resp.Request.URL, resp.StatusCode, responseBody)
	}
	return resp, nil
}
