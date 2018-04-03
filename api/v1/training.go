package v1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Training describes Training from API docs
type Training struct {
	Code      Code               `json:"code"`
	Model     string             `json:"model,omitempty"`
	DataID    string             `json:"data_id,omitempty"`
	Resources map[string]float64 `json:"resources"`
	Meta      map[string]string  `json:"meta,omitempty"`
}

// UnmarshalJSON implements Unmarshaler
func (t *Training) UnmarshalJSON(b []byte) error {
	type plain Training
	if err := json.Unmarshal(b, (*plain)(t)); err != nil {
		return err
	}
	if len(t.Model) > 0 {
		if _, ok := modelRegistry[t.Model]; !ok {
			return fmt.Errorf("unknown model id %q", t.Model)
		}
	}
	if len(t.DataID) > 0 {
		if _, ok := storageRegistry[t.DataID]; !ok {
			return fmt.Errorf("unknown storage id %q", t.DataID)
		}
	}
	return nil
}

// RunTraining registers new request and deploy in singularity
func RunTraining(tr *Training) (*Job, error) {
	j := &Job{
		Deploy: deploy{
			ID: fmt.Sprintf("platform_deploy_%d", time.Now().Nanosecond()),
			ContainerInfo: ContainerInfo{
				Type:   "DOCKER",
				Docker: tr.Code.docker,
			},
			Volumes:                    tr.Code.Volumes,
			Resources:                  tr.Resources,
			DeployHealthTimeoutSeconds: 300,
			Env: tr.Code.Env,
		},
	}
	if err := client.Deploy(j); err != nil {
		return nil, fmt.Errorf("error while running training: %s", err)
	}
	return j, nil
}

// ViewTraining proxies response about task from singularity
func ViewTraining(id string) (*http.Response, error) {
	return client.c.Get(fmt.Sprintf("%s/singularity/api/tasks/ids/request/%s", client.addr, id))
}
