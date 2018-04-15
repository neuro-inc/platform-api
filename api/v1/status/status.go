package status

import (
	"fmt"

	"github.com/satori/go.uuid"

	"github.com/neuromation/platform-api/api/v1/orchestrator"
)


type StatusName int

const (
	STATUS_PENDING StatusName = 0
	STATUS_SUCCEEDED StatusName = 1
	STATUS_FAILED StatusName = 2
)

func (name StatusName) String() string {
	// TODO: move outside
	// TODO: what happens if there is no such name
	names := [...]string {
		"PENDING",
		"SUCCEEDED",
		"FAILED",
	}

	return names[name]
}

func (name StatusName) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, name.String())), nil
}

type Status struct {
	Id string `json:"status_id"`
	StatusName StatusName `json:"status"`
}


func NewStatus() Status {
	id := uuid.NewV4().String()
	status := Status{
		Id: id,
		StatusName: STATUS_PENDING,
	}
	return status
}

func (status Status) IsRedirectionSupported() bool {
	return false
}

func (status Status) IsSucceeded() bool {
	return status.StatusName == STATUS_SUCCEEDED
}

func (status Status) IsFailed() bool {
	return status.StatusName == STATUS_FAILED
}

func (status Status) IsFinished() bool {
	return status.IsSucceeded() || status.IsFailed()
}


type ModelStatus struct {
	Status
	ModelId string `json:"model_id"`
}

func NewModelStatus(modelId string) ModelStatus {
	return ModelStatus{Status: NewStatus(), ModelId: modelId}
}

func (status ModelStatus) IsRedirectionSupported() bool {
	return true
}

func UpdateModelStatus(client orchestrator.Client, modelStatus *ModelStatus) error {
	// TODO: what if the expected job does not exist?
	jobId := modelStatus.ModelId
	job := client.GetJob(jobId)
	status, err := job.Status()
	if err != nil {
		return err
	}

	// TODO: must be moved
	knownStatuses := map[string]StatusName{
		"SUCCEEDED": STATUS_SUCCEEDED,
		"WAITING": STATUS_PENDING,
		"OVERDUE": STATUS_FAILED,
		"FAILED": STATUS_FAILED,
		"FAILED_INTERNAL_STATE": STATUS_FAILED,
		"CANCELING": STATUS_PENDING,
		"CANCELED": STATUS_FAILED,
	}

	// TODO: check presence first
	modelStatus.StatusName = knownStatuses[status]

	return nil
}


type StatusService interface {
	Create() *Status
	Get(id string) (*Status, error)
	Update() Status
	Delete(id string)
}


type InMemoryStatusService struct {
	statuses map[string]Status
}

func NewInMemoryStatusService() *InMemoryStatusService {
	service := new(InMemoryStatusService)
	service.statuses = make(map[string]Status)
	return service
}

func (service *InMemoryStatusService) Create() *Status {
	status := NewStatus()
	service.statuses[status.Id] = status
	return &status
}

func (service *InMemoryStatusService) Get(id string) (*Status, error) {
	status, ok := service.statuses[id]
	if !ok {
		return nil, fmt.Errorf("Status %s was not found", id)
	}
	return &status, nil
}

func (service *InMemoryStatusService) Update() Status {
	return Status{}
}

func (service *InMemoryStatusService) Delete(id string) {
	delete(service.statuses, id)
}

func NewStatusService() StatusService {
	service := NewInMemoryStatusService()
	return service
}
