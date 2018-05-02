package status

import (
	"encoding/json"
)

type ModelStatus struct {
	JobStatus
	ModelId string
}

func NewModelStatus(modelId string, modelUrl string, poller JobStatusPoller) ModelStatus {
	return ModelStatus{
		JobStatus: NewJobStatus(
			NewGenericStatusWithHttpRedirectUrl(modelUrl), poller),
		ModelId: modelId,
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
