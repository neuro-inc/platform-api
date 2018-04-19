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

func NewBatchInferenceStatus(bID string, url string, client orchestrator.Client) BatchInferenceStatus {
	return BatchInferenceStatus{
		GenericStatus:    NewGenericStatusWithHttpRedirectUrl(url),
		BatchInferenceID: bID,
		client:           client,
	}
}

func (status BatchInferenceStatus) IsHttpRedirectSupported() bool {
	return true
}

func (status BatchInferenceStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(publicStatusSchema{
		Id:         status.Id(),
		StatusName: status.StatusName(),
		ModelId:    status.BatchInferenceID,
	})
}

func (status *BatchInferenceStatus) update() error {
	jobId := status.BatchInferenceID
	job := status.client.GetJob(jobId)
	title, err := job.Status()
	if err != nil {
		return err
	}

	// TODO (A Danshyn 04/16/18): must be moved, extract a function
	knownStatuses := map[string]StatusName{
		// NOTE: in case the resulting status is an empty or unknown
		// string, we assume that the status is PENDING
		"":                      STATUS_PENDING,
		"SUCCEEDED":             STATUS_SUCCEEDED,
		"WAITING":               STATUS_PENDING,
		"OVERDUE":               STATUS_FAILED,
		"FAILED":                STATUS_FAILED,
		"FAILED_INTERNAL_STATE": STATUS_FAILED,
		"CANCELING":             STATUS_PENDING,
		"CANCELED":              STATUS_FAILED,
	}

	newStatusName := knownStatuses[title]
	log.Infof(
		"Updating status %s from %s to %s(%s).",
		status.Id(), status.StatusName(), newStatusName, title)

	status.statusName = newStatusName

	return nil
}
