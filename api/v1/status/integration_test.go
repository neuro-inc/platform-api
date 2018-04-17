// +build integration

package status

import (
	"testing"
	"time"

	"github.com/neuromation/platform-api/api/v1/client/singularity"
	"github.com/neuromation/platform-api/api/v1/container"
)

var (
	addr    = "127.0.0.1:7099"
	timeout = time.Second
)

func TestModelStatusUpdate(t *testing.T) {
	c, err := singularity.NewClient("http://"+addr, timeout)
	if err != nil {
		t.Fatalf("fail to create new client: %s", err)
	}
	if err := c.Ping(time.Second * 10); err != nil {
		t.Fatalf("unable to establish connection: %s", err)
	}
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

	status := NewModelStatus(j.GetID(), "", c)
	maxWait := time.Second * 5
	done := time.Now().Add(maxWait)
	for time.Now().Before(done) {
		time.Sleep(time.Millisecond * 200)
		err = status.update()
		if err != nil {
			t.Fatalf("ModelStatus.update failed with: %s", err)
			break
		}
		if status.IsFinished() {
			break
		}
	}

	if !status.IsFinished() {
		t.Fatalf("the job %s is still in progress after %d s.",
			status.ModelId, maxWait)
	}
}
