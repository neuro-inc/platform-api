package singularity

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/orchestrator"
	"github.com/neuromation/platform-api/log"
)

type singularityClient struct {
	c    http.Client
	addr *url.URL
}

var registry = &jobRegistry{
	r: make(map[string]*singularityJob),
}

type jobRegistry struct {
	sync.RWMutex
	r map[string]*singularityJob
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

func (sc *singularityClient) Ping(maxWait time.Duration) error {
	done := time.Now().Add(maxWait)
	retryTimeout := time.Second
	var err error
	for time.Now().Before(done) {
		err = sc.ready()
		if err == nil {
			return nil
		}
		log.Errorf("%s. Retrying after %v...", err, retryTimeout)
		time.Sleep(retryTimeout)
	}
	return err
}

func (sc *singularityClient) ready() error {
	resp, err := sc.get("state")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code returned from %q: %d", resp.Request.URL, resp.StatusCode)
	}

	state := &struct {
		ActiveSlaves int `json:"activeSlaves"`
	}{}
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(state); err != nil {
		return fmt.Errorf("response parse error: %s", err)
	}
	if state.ActiveSlaves < 1 {
		return fmt.Errorf("singularity has no active slaves")
	}
	return nil
}

// TODO: NewJob is the method of singularityClient
// but actually Job is something different from client.
func (sc *singularityClient) NewJob(container container.Container, res container.Resources) orchestrator.Job {
	id := fmt.Sprintf("platform_deploy_%d", time.Now().Nanosecond())
	var volumes []volume
	for _, v := range container.Volumes {
		v := volume{
			HostPath:      v.From,
			ContainerPath: v.To,
			Mode:          v.Mode,
		}
		volumes = append(volumes, v)
	}

	j := &singularityJob{
		client: sc,
		Deploy: deploy{
			ID: id,
			ContainerInfo: containerInfo{
				Type: "DOCKER",
				Docker: dockerContainer{
					Image: container.Image,
				},
				Volumes: volumes,
			},
			Resources:                  res,
			DeployHealthTimeoutSeconds: 300,
			Env: container.Env,
		},
	}

	registry.Lock()
	registry.r[id] = j
	registry.Unlock()

	return j
}

func (sc *singularityClient) GetJob(id string) orchestrator.Job {
	registry.RLock()
	j := registry.r[id]
	registry.RUnlock()
	return j
}

func (sc *singularityClient) SearchJobs() []orchestrator.Job {
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
		return err
	}
	j.Deploy.RequestID = reqID
	if err := j.client.registerDeploy(reqID, j); err != nil {
		return err
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
	addr := fmt.Sprintf("history/request/%s/deploy/%s", j.Deploy.RequestID, j.Deploy.ID)
	resp, err := j.client.get(addr)
	if err != nil {
		return "", fmt.Errorf("error while getting job %q state: %s", j.GetID(), err)
	}
	decoder := json.NewDecoder(resp.Body)
	deployHistory := &deployHistory{}
	if err = decoder.Decode(deployHistory); err != nil {
		return "", fmt.Errorf("error while decoding request body: %s", err)
	}
	return deployHistory.DeployResult.State, nil
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

func (sc *singularityClient) registerRequest(id string) error {
	body := fmt.Sprintf(requestTpl, id)
	_, err := sc.post("requests", body)
	if err != nil {
		return fmt.Errorf("error while registering request %q at %q: %s", body, sc.addr, err)
	}
	return nil
}

func (sc *singularityClient) registerDeploy(reqID string, job *singularityJob) error {
	body := job.String()
	_, err := sc.post("deploys", body)
	if err != nil {
		return fmt.Errorf("error while registering deploy %q at %q: %s", body, sc.addr, err)
	}
	return nil
}

func (sc *singularityClient) post(addr, body string) (*http.Response, error) {
	r := strings.NewReader(body)
	uri := fmt.Sprintf("%s/singularity/api/%s", sc.addr.String(), addr)
	req, err := http.NewRequest("POST", uri, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return sc.do(req)
}

func (sc *singularityClient) get(addr string) (*http.Response, error) {
	uri := fmt.Sprintf("%s/singularity/api/%s", sc.addr.String(), addr)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}
	return sc.do(req)
}

func (sc *singularityClient) do(req *http.Request) (*http.Response, error) {
	resp, err := sc.c.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		responseBody, _ := ioutil.ReadAll(resp.Body)
		return resp, fmt.Errorf("unexpected status code returned from %q: %d. Response body: %q",
			resp.Request.URL, resp.StatusCode, responseBody)
	}
	return resp, nil
}
