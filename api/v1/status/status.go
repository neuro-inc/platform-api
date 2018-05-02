package status

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/satori/go.uuid"

	"github.com/neuromation/platform-api/log"
)

type StatusName int

const (
	STATUS_PENDING   StatusName = 0
	STATUS_SUCCEEDED StatusName = 1
	STATUS_FAILED    StatusName = 2
)

var status_names = map[StatusName]string{
	STATUS_PENDING:   "PENDING",
	STATUS_SUCCEEDED: "SUCCEEDED",
	STATUS_FAILED:    "FAILED",
}

func (name StatusName) String() string {
	return status_names[name]
}

func (name StatusName) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, name.String())), nil
}

type Status interface {
	Id() string
	StatusName() StatusName
	IsHttpRedirectSupported() bool
	HttpRedirectUrl() string
	IsSucceeded() bool
	IsFailed() bool
	IsFinished() bool
}

type GenericStatus struct {
	id              string
	statusName      StatusName
	httpRedirectUrl string
}

func NewGenericStatus() GenericStatus {
	return NewGenericStatusWithHttpRedirectUrl("")
}

func NewGenericStatusWithHttpRedirectUrl(url string) GenericStatus {
	id := uuid.NewV4().String()
	status := GenericStatus{
		id:              id,
		statusName:      STATUS_PENDING,
		httpRedirectUrl: url,
	}
	return status
}

func (gs GenericStatus) Id() string {
	return gs.id
}

func (gs GenericStatus) StatusName() StatusName {
	return gs.statusName
}

func (gs *GenericStatus) SetStatusName(name StatusName) {
	gs.statusName = name
}

func (GenericStatus) IsHttpRedirectSupported() bool {
	return false
}

func (gs GenericStatus) HttpRedirectUrl() string {
	return gs.httpRedirectUrl
}

func (gs GenericStatus) IsSucceeded() bool {
	return gs.StatusName() == STATUS_SUCCEEDED
}

func (gs GenericStatus) IsFailed() bool {
	return gs.StatusName() == STATUS_FAILED
}

func (gs GenericStatus) IsFinished() bool {
	return gs.IsSucceeded() || gs.IsFailed()
}

type publicStatusSchema struct {
	Id               string     `json:"status_id"`
	StatusName       StatusName `json:"status"`
	ModelId          string     `json:"model_id,omitempty"`
	BatchInferenceID string     `json:"batch_inference_id,omitempty"`
}

func (gs GenericStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(publicStatusSchema{
		Id:         gs.Id(),
		StatusName: gs.StatusName(),
	})
}

type JobStatus struct {
	GenericStatus
	poller JobStatusPoller
}

func NewJobStatus(gs GenericStatus, poller JobStatusPoller) JobStatus {
	return JobStatus{
		GenericStatus: gs,
		poller:        poller,
	}
}

func (js *JobStatus) update() error {
	return js.poller.Update(js)
}

type JobStatusPoller interface {
	Update(*JobStatus) error
}

type StatusService interface {
	Set(Status) error
	Get(id string) (Status, error)
	Delete(id string)
}

type InMemoryStatusService struct {
	sync.RWMutex
	statuses map[string]Status
}

func NewInMemoryStatusService() *InMemoryStatusService {
	service := new(InMemoryStatusService)
	service.statuses = make(map[string]Status)
	return service
}

func (ss *InMemoryStatusService) Set(status Status) error {
	ss.Lock()
	if oldStatus, ok := ss.statuses[status.Id()]; ok {
		log.Infof(
			"Updating status %s from %s to %s",
			status.Id(), oldStatus.StatusName(),
			status.StatusName())
	} else {
		log.Infof(
			"Adding new status %s %s",
			status.Id(), status.StatusName())
	}
	ss.statuses[status.Id()] = status
	ss.Unlock()
	return nil
}

func (ss *InMemoryStatusService) Get(id string) (Status, error) {
	ss.RLock()
	status, ok := ss.statuses[id]
	ss.RUnlock()
	if !ok {
		return nil, fmt.Errorf("Status %s was not found", id)
	}

	if status.IsFinished() {
		return status, nil
	}

	// NOTE (A Danshyn 04/16/18): the fact that ModelStatus updates itself
	// during its retrieval is rather an exception which will hold until
	// the Platform API starts tracking and polling job statuses itself
	// instead of proxying HTTP requests
	switch statusCast := status.(type) {
	case ModelStatus:
		statusCast.update()
		status = statusCast
		ss.Set(status)
	case BatchInferenceStatus:
		statusCast.update()
		status = statusCast
		ss.Set(status)
	}
	return status, nil
}

func (ss *InMemoryStatusService) Delete(id string) {
	ss.Lock()
	delete(ss.statuses, id)
	ss.Unlock()
}

func NewStatusService() StatusService {
	return NewInMemoryStatusService()
}
