// +build integration

package singularity

import (
	"encoding/json"
	"fmt"
	"github.com/neuromation/platform-api/api/v1/container"
	"net"
	"net/http"
	"testing"
	"time"
)

var (
	addr    = "127.0.0.1:7099"
	timeout = time.Second
)

func newClient(t *testing.T) *singularityClient {
	t.Helper()
	if err := waitReachable(addr); err != nil {
		t.Fatalf("singularity unreachable: %s", err)
	}
	c, err := NewClient("http://"+addr, timeout)
	if err != nil {
		t.Fatalf("fail to create new client: %s", err)
	}
	return c.(*singularityClient)
}

// waitReachable waits for hostport to became reachable for the maxWait time.
func waitReachable(addr string) error {
	maxWait := time.Second * 10
	done := time.Now().Add(maxWait)
	for time.Now().Before(done) {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("cannot connect %v for %v", addr, maxWait)
}

func TestGetState(t *testing.T) {
	c := newClient(t)
	resp, err := c.get("state")
	if err != nil {
		t.Fatalf("fail to get singularity state: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected sc %d; got %d", http.StatusOK, resp.StatusCode)
	}
}

func TestSingularityJob_Start(t *testing.T) {
	c := newClient(t)
	cont := container.Container{
		Image: "hello-world",
	}
	res := container.Resources{
		"cpus":     1,
		"memoryMb": 50,
	}
	j := c.NewJob(cont, res)
	if err := j.Start(); err != nil {
		t.Fatalf("error while starting job: %s", err)
	}

	var deployID string
	maxWait := time.Second * 5
	done := time.Now().Add(maxWait)
	for time.Now().Before(done) {
		time.Sleep(time.Millisecond * 200)
		addr := fmt.Sprintf("requests/request/%s", j.GetID())
		resp, err := c.get(addr)
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
		addr := fmt.Sprintf("history/request/%s/deploy/%s", j.GetID(), deployID)
		resp, err := c.get(addr)
		if err != nil {
			t.Fatalf("fail to get deploy state: %s", err)
		}
		decoder := json.NewDecoder(resp.Body)
		deployHistory := &deployHistory{}
		err = decoder.Decode(deployHistory)
		if err != nil {
			t.Fatalf("unexpected error while decoding request body: %s", err)
			return
		}
		if deployHistory.DeployResult.State == "SUCCEEDED" {
			return
		}
	}
	t.Fatalf("deploy %q didnt become SUCCEEDED for %v", deployID, maxWait)
}
