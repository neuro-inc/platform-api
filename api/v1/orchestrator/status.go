package orchestrator

import (
	"github.com/neuromation/platform-api/api/v1/status"
	"github.com/neuromation/platform-api/log"
)

type JobStatusPollerImpl struct {
	job Job
}

func NewJobStatusPoller(job Job) status.JobStatusPoller {
	return &JobStatusPollerImpl{
		job: job,
	}
}

func (jspi *JobStatusPollerImpl) Update(js *status.JobStatus) error {
	statusName, err := jspi.job.Status()
	if err != nil {
		return err
	}

	log.Infof(
		"Updating job status %s from %s to %s.",
		js.Id(), js.StatusName(), statusName)

	js.SetStatusName(statusName)
	return nil
}
