package status

import (
	"encoding/json"

	"github.com/neuromation/platform-api/api/v1/orchestrator"
	"github.com/neuromation/platform-api/log"
)

type BatchInferenceStatus struct {
	GenericStatus
	BatchInferenceID string

	client orchestrator.Client
}

func NewBatchInferenceStatus(bID string, url string, client orchestrator.Client) *BatchInferenceStatus {
	return &BatchInferenceStatus{
		GenericStatus:    NewGenericStatusWithHttpRedirectUrl(url),
		BatchInferenceID: bID,
		client:           client,
	}
}

func (BatchInferenceStatus) IsHttpRedirectSupported() bool {
	return true
}

func (bis BatchInferenceStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(publicStatusSchema{
		Id:               bis.Id(),
		StatusName:       bis.StatusName(),
		BatchInferenceID: bis.BatchInferenceID,
	})
}

func (bis *BatchInferenceStatus) update() error {
	jobId := bis.BatchInferenceID
	job := bis.client.GetJob(jobId)
	title, err := job.Status()
	if err != nil {
		return err
	}

	newStatusName := knownStatuses[title]
	log.Infof(
		"Updating status %s from %s to %s(%s).",
		bis.Id(), bis.StatusName(), newStatusName, title)

	bis.statusName = newStatusName

	return nil
}
