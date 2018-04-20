package status

import (
	"encoding/json"

	"github.com/neuromation/platform-api/api/v1/orchestrator"
	"github.com/neuromation/platform-api/log"
)

type ModelStatus struct {
	GenericStatus
	ModelId string

	client orchestrator.Client
}

func NewModelStatus(modelId string, modelUrl string, client orchestrator.Client) *ModelStatus {
	return &ModelStatus{
		GenericStatus: NewGenericStatusWithHttpRedirectUrl(modelUrl),
		ModelId:       modelId,
		client:        client,
	}
}

func (ModelStatus) IsHttpRedirectSupported() bool {
	return true
}

func (ms ModelStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(publicStatusSchema{
		Id:         ms.Id(),
		StatusName: ms.StatusName(),
		ModelId:    ms.ModelId,
	})
}

func (ms *ModelStatus) update() error {
	jobId := ms.ModelId
	job := ms.client.GetJob(jobId)
	title, err := job.Status()
	if err != nil {
		return err
	}

	newStatusName := knownStatuses[title]
	log.Infof(
		"Updating status %s from %s to %s(%s).",
		ms.Id(), ms.StatusName(), newStatusName, title)

	ms.statusName = newStatusName

	return nil
}
