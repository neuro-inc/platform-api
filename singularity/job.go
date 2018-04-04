package singularity

import "encoding/json"

// String implements the Stringer interface
func (d Deploy) String() string {
	b, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	return string(b)
}

type Deploy struct {
	RequestID                  string             `json:"requestId"`
	ID                         string             `json:"id"`
	Type                       string             `json:"type"`
	Volumes                    []Volume           `json:"volumes"`
	ContainerInfo              ContainerInfo      `json:"containerInfo"`
	Resources                  map[string]float64 `json:"resources"`
	Env                        map[string]string  `json:"env"`
	DeployHealthTimeoutSeconds int                `json:"deployHealthTimeoutSeconds"`
}
