package status

import (
	"encoding/json"
)

type BatchInferenceStatus struct {
	JobStatus
	BatchInferenceID string
}

func NewBatchInferenceStatus(bID string, url string, poller JobStatusPoller) BatchInferenceStatus {
	return BatchInferenceStatus{
		JobStatus: NewJobStatus(
			NewGenericStatusWithHttpRedirectUrl(url), poller),
		BatchInferenceID: bID,
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
