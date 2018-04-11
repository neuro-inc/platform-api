package v1

import (
	"github.com/satori/go.uuid"
)


// TODO: implement status service that stores ID mapping in-memory

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


type StatusService interface {
	Create() Status
	Get(id string) Status
	Update() Status
	Delete(id string) Status
}


type InMemoryStatusService struct {
	statuses map[string]Status
}

func NewInMemoryStatusService() *InMemoryStatusService {
	service := new(InMemoryStatusService)
	service.statuses = make(map[string]Status)
	return service
}

func (service *InMemoryStatusService) Create() Status {
	id := uuid.Must(uuid.NewV4()).String()
	status := Status{
		Id: id,
		Status: STATUS_PENDING,
	}
	return status
}

func (service *InMemoryStatusService) Get(id string) Status {
	return Status{}
}

func (service *InMemoryStatusService) Update() Status {
	return Status{}
}

func (service *InMemoryStatusService) Delete(id string) Status {
	return Status{}
}

func NewStatusService() StatusService {
	service := NewInMemoryStatusService()
	return service
}
