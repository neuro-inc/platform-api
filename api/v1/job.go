package v1

import "encoding/json"

// Job wraps singularity Deploy
type Job struct {
	Deploy deploy `json:"deploy"`
}

// String implements the Stringer interface
func (j Job) String() string {
	b, err := json.Marshal(j)
	if err != nil {
		panic(err)
	}
	return string(b)
}

type deploy struct {
	RequestID                  string             `json:"requestId"`
	ID                         string             `json:"id"`
	Type                       string             `json:"type"`
	Volumes                    []Volume           `json:"volumes"`
	ContainerInfo              ContainerInfo      `json:"containerInfo"`
	Resources                  map[string]float64 `json:"resources"`
	DeployHealthTimeoutSeconds int                `json:"deployHealthTimeoutSeconds"`
}
