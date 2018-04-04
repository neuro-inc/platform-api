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
	"github.com/neuromation/platform-api/log"
)

type singularityClient struct {
	c    http.Client
	addr *url.URL
}

// NewClient creates new orchestrator.Client from given config
func NewClient(addr string, timeout time.Duration) (orchestrator.Client, error) {
	uri, err := url.Parse(addr)
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
	return j.Deploy.ID
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

func (c *singularityClient) registerDeploy(reqID string, job *singularityJob) error {
	body := fmt.Sprintf("%s", job)
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
