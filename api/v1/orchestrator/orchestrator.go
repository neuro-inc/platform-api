package orchestrator

import "github.com/neuromation/platform-api/api/v1/container"

type Client interface {
	NewJob(container.Container, map[string]float64) Job
	GetJob() Job
	SearchJobs() []Job
}

// Question: I'm aware of big interfaces. There is probability it will getting bigger
type Job interface {
	Start() error
	Stop() error
	Delete() error
	Status() (string, error)
	GetID() string
}
