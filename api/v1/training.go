package v1

import (
	"fmt"
	"net/http"
)

// RunTraining registers new request and deploy in singularity
func RunTraining(req *Request, cli OrchestratorClient) (OrchestratorJob, error) {
	if len(req.ModelName) > 0 {
		if _, ok := modelRegistry[req.ModelName]; !ok {
			return nil, fmt.Errorf("unknown model id %q", req.ModelName)
		}
	}
	if len(req.DataID) > 0 {
		if _, ok := storageRegistry[req.DataID]; !ok {
			return nil, fmt.Errorf("unknown storage id %q", req.DataID)
		}
	}

	job := cli.NewJob(req)
	if err := job.Start(); err != nil {
		return nil, fmt.Errorf("error while creating training: %s", err)
	}
	return job, nil
}

// ViewTraining proxies response about task from singularity
func ViewTraining(id string) (*http.Response, error) {
	panic("implement me")
	//return client.c.Get(fmt.Sprintf("%s/singularity/api/tasks/ids/request/%s", client.addr, id))
}
