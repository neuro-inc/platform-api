package singularity

import (
	"github.com/neuromation/platform-api/api/v1/container"
)

type Deploy struct {
	RequestID                  string             `json:"requestId"`
	ID                         string             `json:"id"`
	Type                       string             `json:"type"`
	ContainerInfo              ContainerInfo      `json:"containerInfo"`
	Resources                  map[string]float64 `json:"resources"`
	Env                        map[string]string  `json:"env"`
	DeployHealthTimeoutSeconds int                `json:"deployHealthTimeoutSeconds"`
}

// ContainerInfo wrapper for singularity container
type ContainerInfo struct {
	Type   string          `json:"type"`
	Docker dockerContainer `json:"docker"`
	Volumes                    []container.Volume `json:"volumes"`
}

type dockerContainer struct {
	Image string `json:"image"`
}
