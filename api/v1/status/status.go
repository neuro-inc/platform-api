package status

import (
	"encoding/json"
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

type Status interface {
	Id() string
	StatusName() StatusName
	IsRedirectionSupported() bool
	IsSucceeded() bool
	IsFailed() bool
	IsFinished() bool
}

type GenericStatus struct {
	id string
	statusName StatusName
}

func NewGenericStatus() GenericStatus {
	id := uuid.NewV4().String()
	status := GenericStatus{
		id: id,
		statusName: STATUS_PENDING,
	}
	return status
}

func (status GenericStatus) Id() string {
	return status.id
}

func (status GenericStatus) StatusName() StatusName {
	return status.statusName
}

func (status GenericStatus) IsRedirectionSupported() bool {
	return false
}

func (status GenericStatus) IsSucceeded() bool {
	return status.StatusName() == STATUS_SUCCEEDED
}

func (status GenericStatus) IsFailed() bool {
	return status.StatusName() == STATUS_FAILED
}

func (status GenericStatus) IsFinished() bool {
	return status.IsSucceeded() || status.IsFailed()
}

type publicStatusSchema struct {
	Id string `json:"status_id"`
	StatusName StatusName `json:"status"`
	ModelId string `json:"model_id,omitempty"`
}

func (status GenericStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(publicStatusSchema{
		Id: status.Id(),
		StatusName: status.StatusName(),
	})
}

type ModelStatus struct {
	GenericStatus
	ModelId string

	client orchestrator.Client
}

func NewModelStatus(modelId string, client orchestrator.Client) ModelStatus {
	return ModelStatus{
		GenericStatus: NewGenericStatus(),
		ModelId: modelId,
		client: client,
	}
}

func (status ModelStatus) IsRedirectionSupported() bool {
	return true
}

func (status ModelStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(publicStatusSchema{
		Id: status.Id(),
		StatusName: status.StatusName(),
		ModelId: status.ModelId,
	})
}

func (status *ModelStatus) update() error {
	jobId := status.ModelId
	job := status.client.GetJob(jobId)
	title, err := job.Status()
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
	status.statusName = knownStatuses[title]

	return nil
}

type StatusService interface {
	Set(Status) error
	Get(id string) (Status, error)
	Delete(id string)
}


type InMemoryStatusService struct {
	// TODO: lock
	statuses map[string]Status
}

func NewInMemoryStatusService() *InMemoryStatusService {
	service := new(InMemoryStatusService)
	service.statuses = make(map[string]Status)
	return service
}

func (service *InMemoryStatusService) Set(status Status) error {
	service.statuses[status.Id()] = status
	return nil
}

func (service *InMemoryStatusService) Get(id string) (Status, error) {
	status, ok := service.statuses[id]
	if !ok {
		return nil, fmt.Errorf("Status %s was not found", id)
	}
	
	if status.IsFinished() {
		return status, nil
	}

	switch statusCast := status.(type) {
	case ModelStatus:
		statusCast.update()
		status = statusCast
		service.Set(status)
	}
	return status, nil
}

func (service *InMemoryStatusService) Delete(id string) {
	delete(service.statuses, id)
}

func NewStatusService() StatusService {
	service := NewInMemoryStatusService()
	return service
}
