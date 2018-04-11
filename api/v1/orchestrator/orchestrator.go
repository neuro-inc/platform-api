package orchestrator

import (
	"github.com/neuromation/platform-api/api/v1/container"
	"time"
)

// Client allows creating, getting and serching for Jobs
type Client interface {
	NewJob(container.Container, container.Resources) Job
	GetJob() Job
	SearchJobs() []Job
	Ping(duration time.Duration) error
}

// Job describes a common list of actions wtih Job
// Question: I'm aware of big interfaces. There is probability it will getting bigger
type Job interface {
	Start() error
	Stop() error
	Delete() error
	Status() (string, error)
	GetID() string
}
