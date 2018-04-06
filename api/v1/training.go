package v1

import (
	"fmt"
	"net/http"

	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/orchestrator"
)

type training struct {
	// TODO: rename `code` in API doc to smthng intuitive
	Container container.Container `json:"code"`
	Resources container.Resources `json:"resources"`

	ModelWeight string            `json:"model_weight,omitempty"`
	ModelName   string            `json:"model_name,omitempty"`
	DataID      string            `json:"data_id,omitempty"`
	Meta        map[string]string `json:"meta,omitempty"`
}

// runTraining starts a new training task accoridng to received req
func runTraining(tr *training) (orchestrator.Job, error) {
	// check modelName here to avoid exploiting registry with invalid requests
	if len(tr.ModelName) > 0 {
		if _, ok := modelRegistry[tr.ModelName]; !ok {
			return nil, fmt.Errorf("unknown model id %q", tr.ModelName)
		}
	}
	// check dataID here to avoid exploiting registry with invalid requests
	if len(tr.DataID) > 0 {
		if _, ok := storageRegistry[tr.DataID]; !ok {
			return nil, fmt.Errorf("unknown storage id %q", tr.DataID)
		}
	}
	job := client.NewJob(tr.Container, tr.Resources)
	if err := job.Start(); err != nil {
		return nil, fmt.Errorf("error while creating training: %s", err)
	}
	return job, nil
}

// ViewTraining proxies response about task from singularity
func ViewTraining(id string) (*http.Response, error) {
	panic("implement me")
}
