package orchestrator

import (
	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/status"
	"time"
)

// Client allows creating, getting and searching for Jobs
type Client interface {
	NewJob(*container.Container, container.Resources) Job
	GetJob(string) Job
	SearchJobs() []Job
	Ping(duration time.Duration) error
}

// Job describes a common list of actions with Job
// Question: I'm aware of big interfaces. There is probability it will getting bigger
type Job interface {
	Start() error
	Stop() error
	Delete() error
	Status() (status.StatusName, error)
	GetID() string
}
