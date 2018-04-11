package v1

import (
	"fmt"
	"github.com/satori/go.uuid"
)


type StatusName int

const (
	STATUS_PENDING StatusName = 0
	STATUS_SUCCEEDED StatusName = 1
	STATUS_FAILED StatusName = 2
)

func (name StatusName) String() string {
	names := [...]string {
		"PENDING",
		"SUCCEEDED",
		"FAILED",
	}

	return names[name]
}

type Status struct {
	Id string
	Status StatusName
}


func NewStatus() Status {
	id := uuid.NewV4().String()
	status := Status{
		Id: id,
		Status: STATUS_PENDING,
	}
	return status
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
