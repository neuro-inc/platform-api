package v1

type OrchestratorClient interface {
	NewJob(request *Request) OrchestratorJob
	GetJob() OrchestratorJob
	SearchJobs() []OrchestratorJob
}

type OrchestratorJob interface {
	Start() error
	Stop() error
	Delete() error
	Status() (string, error)
	GetID() string
}
