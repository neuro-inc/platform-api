package singularity

type (
	deploy struct {
		RequestID                  string             `json:"requestId"`
		ID                         string             `json:"id"`
		Type                       string             `json:"type"`
		ContainerInfo              containerInfo      `json:"containerInfo"`
		Resources                  map[string]float64 `json:"resources"`
		Env                        map[string]string  `json:"env"`
		DeployHealthTimeoutSeconds int                `json:"deployHealthTimeoutSeconds"`
	}

	// ContainerInfo wrapper for singularity container
	containerInfo struct {
		Type    string          `json:"type"`
		Docker  dockerContainer `json:"docker"`
		Volumes []Volume        `json:"volumes"`
	}

	Volume struct {
		ContainerPath string `json:"containerPath"`
		HostPath      string `json:"hostPath"`
		Mode          string `json:"mode"`
	}

	dockerContainer struct {
		Image string `json:"image"`
	}
)
