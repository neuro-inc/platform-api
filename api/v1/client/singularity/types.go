package singularity

type requestResponse struct {
	Request            request            `json:"request"`
	State              string             `json:"state"`
	RequestDeployState requestDeployState `json:"requestDeployState"`
	ActiveDeploy       activeDeploy       `json:"activeDeploy"`
}

type request struct {
	ID          string `json:"id"`
	RequestType string `json:"requestType"`
}

type requestDeployState struct {
	RequestID    string       `json:"requestId"`
	ActiveDeploy activeDeploy `json:"activeDeploy"`
}

type activeDeploy struct {
	RequestID string `json:"requestId"`
	DeployID  string `json:"deployId"`
	Timestamp int    `json:"timestamp"`
}

type deployHistory struct {
	DeployResult deployResult `json:"deployResult"`
}

type deployResult struct {
	State   string `json:"deployState"`
	Message string `json:"message"`
}
