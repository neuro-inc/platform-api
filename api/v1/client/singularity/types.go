package singularity

type (
	requestResponse struct {
		Request            request            `json:"request"`
		State              string             `json:"state"`
		RequestDeployState requestDeployState `json:"requestDeployState"`
		ActiveDeploy       activeDeploy       `json:"activeDeploy"`
	}

	request struct {
		ID          string `json:"id"`
		RequestType string `json:"requestType"`
	}

	requestDeployState struct {
		RequestID    string       `json:"requestId"`
		ActiveDeploy activeDeploy `json:"activeDeploy"`
	}

	activeDeploy struct {
		RequestID string `json:"requestId"`
		DeployID  string `json:"deployId"`
		Timestamp int    `json:"timestamp"`
	}

	deployHistory struct {
		DeployResult deployResult `json:"deployResult"`
	}

	deployResult struct {
		State   string `json:"deployState"`
		Message string `json:"message"`
	}

	deploy struct {
		RequestID                  string             `json:"requestId"`
		ID                         string             `json:"id"`
		Command                    string             `json:"command,omitempty"`
		Shell                      bool               `json:"shell,omitempty"`
		Type                       string             `json:"type"`
		ContainerInfo              containerInfo      `json:"containerInfo"`
		Resources                  map[string]float64 `json:"resources"`
		Env                        map[string]string  `json:"env"`
		DeployHealthTimeoutSeconds int                `json:"deployHealthTimeoutSeconds"`
		URIs                       []*artifact        `json:"uris"`
	}

	artifact struct {
		URI        string `json:"uri"`
		Cache      bool   `json:"cache"`
		Executable bool   `json:"executable"`
		Extract    bool   `json:"extract"`
	}

	// ContainerInfo wrapper for singularity container
	containerInfo struct {
		Type    string          `json:"type"`
		Docker  dockerContainer `json:"docker"`
		Volumes []volume        `json:"volumes"`
	}

	volume struct {
		ContainerPath string `json:"containerPath"`
		HostPath      string `json:"hostPath"`
		Mode          string `json:"mode"`
	}

	dockerContainer struct {
		Image string `json:"image"`
	}
)
