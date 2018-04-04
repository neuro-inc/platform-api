package v1

import (
	"fmt"
	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/orchestrator"
	"net/http"
)

type Training struct {
	// TODO: rename `code` in API doc to smthng intuitive
	Container container.Container `json:"code"`

	ModelWeight string             `json:"model_weight,omitempty"`
	ModelName   string             `json:"model_name,omitempty"`
	DataID      string             `json:"data_id,omitempty"`
	Resources   map[string]float64 `json:"resources"`
	Meta        map[string]string  `json:"meta,omitempty"`
}

//Code (container id or name?)
//Model weight and model `name (data_id)
//Data_id (dir\list?) -- id of storage object containing training input
//meta -- parameters to be passed to training container
//

//{
//"code": {
//"env": {
//"MODEL_PATH": "models"
//},
//"image": "registry.neuromation.io/neuromationorg/platformapi-dummy",
//"volumes": [
//{
//"hostPath": "/Users/romankhavronenko/image_temp",
//"containerPath": "/models",
//"mode": "RW"
//}
//]
//},
//"resources": {
//"cpus": 2,
//"memoryMb": 128
//}
//}

// runTraining starts a new training task accoridng to received req
func runTraining(tr *Training) (orchestrator.Job, error) {
	if len(tr.ModelName) > 0 {
		if _, ok := modelRegistry[tr.ModelName]; !ok {
			return nil, fmt.Errorf("unknown model id %q", tr.ModelName)
		}
	}
	if len(tr.DataID) > 0 {
		if _, ok := storageRegistry[tr.DataID]; !ok {
			return nil, fmt.Errorf("unknown storage id %q", tr.DataID)
		}
	}

	job := client.NewJob(tr.Container, tr.Resources)
	if err := job.Start(); err != nil {
		return nil, fmt.Errorf("error while creating training: %s", err)
	}
	return job, job.Start()
}

// ViewTraining proxies response about task from singularity
func ViewTraining(id string) (*http.Response, error) {
	panic("implement me")
	//return client.c.Get(fmt.Sprintf("%s/singularity/api/tasks/ids/request/%s", client.addr, id))
}
