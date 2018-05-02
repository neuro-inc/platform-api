// +build integration

package singularity

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/orchestrator"
	statuses "github.com/neuromation/platform-api/api/v1/status"
)

var (
	testAddr    = "127.0.0.1:7099"
	testTimeout = time.Second
)

func TestSingularityJob_Start(t *testing.T) {
	sc := getClient(t)
	cont := container.Container{
		Image: "hello-world",
	}
	res := container.Resources{
		"cpus":     1,
		"memoryMb": 50,
	}
	j := sc.NewJob(cont, res)
	if err := j.Start(); err != nil {
		t.Fatalf("error while starting job: %s", err)
	}

	var deployID string
	maxWait := time.Second * 5
	done := time.Now().Add(maxWait)
	job := j.(*singularityJob)
	for time.Now().Before(done) {
		time.Sleep(time.Millisecond * 200)
		addr := fmt.Sprintf("requests/request/%s", job.Deploy.RequestID)
		resp, err := sc.get(addr)
		if err != nil {
			t.Fatalf("fail to get request state: %s", err)
		}
		decoder := json.NewDecoder(resp.Body)
		requestState := &requestResponse{}
		if err = decoder.Decode(requestState); err != nil {
			t.Fatalf("unexpected error while decoding request body: %s", err)
		}
		if requestState.State == "ACTIVE" {
			deployID = requestState.RequestDeployState.ActiveDeploy.DeployID
			break
		}
	}
	if len(deployID) == 0 {
		t.Fatalf("request didn't become ACTIVE for %v", maxWait)
	}

	maxWait = time.Second * 10
	done = time.Now().Add(maxWait)
	for time.Now().Before(done) {
		time.Sleep(time.Millisecond * 200)
		status, err := j.Status()
		if err != nil {
			t.Fatalf("unexpected status error: %s", err)
		}
		if status == statuses.STATUS_SUCCEEDED {
			return
		}
	}
	t.Fatalf("deploy %q didnt become SUCCEEDED for %v", deployID, maxWait)
}

func TestJobStatusPoller(t *testing.T) {
	sc := getClient(t)
	cont := container.Container{
		Image: "hello-world",
	}
	res := container.Resources{
		"cpus":     1,
		"memoryMb": 50,
	}
	j := sc.NewJob(cont, res)
	if err := j.Start(); err != nil {
		t.Fatalf("error while starting job: %s", err)
	}

	var deployID string
	maxWait := time.Second * 5
	done := time.Now().Add(maxWait)
	job := j.(*singularityJob)
	for time.Now().Before(done) {
		time.Sleep(time.Millisecond * 200)
		addr := fmt.Sprintf("requests/request/%s", job.Deploy.RequestID)
		resp, err := sc.get(addr)
		if err != nil {
			t.Fatalf("fail to get request state: %s", err)
		}
		decoder := json.NewDecoder(resp.Body)
		requestState := &requestResponse{}
		if err = decoder.Decode(requestState); err != nil {
			t.Fatalf("unexpected error while decoding request body: %s", err)
		}
		if requestState.State == "ACTIVE" {
			deployID = requestState.RequestDeployState.ActiveDeploy.DeployID
			break
		}
	}
	if len(deployID) == 0 {
		t.Fatalf("request didn't become ACTIVE for %v", maxWait)
	}

	jobStatusPoller := orchestrator.NewJobStatusPoller(job)
	jobStatus := statuses.NewJobStatus(
		statuses.NewGenericStatus(), jobStatusPoller)

	maxWait = time.Second * 10
	done = time.Now().Add(maxWait)
	for time.Now().Before(done) {
		time.Sleep(time.Millisecond * 200)
		err := jobStatusPoller.Update(jobStatus)
		if err != nil {
			t.Fatalf("unexpected status error: %s", err)
		}
		if jobStatus.IsSucceeded() {
			return
		}
	}
	t.Fatalf("deploy %q didnt become SUCCEEDED for %v", deployID, maxWait)
}

func getClient(t *testing.T) *singularityClient {
	t.Helper()
	c, err := NewClient("http://"+testAddr, "", testTimeout)
	if err != nil {
		t.Fatalf("fail to create new client: %s", err)
	}
	if err := c.Ping(time.Second * 10); err != nil {
		t.Fatalf("unable to establish connection: %s", err)
	}
	return c.(*singularityClient)
}
