package v1

// Code is kinda docker container
type Code struct {
	// TODO: is always `docker` for now
	Type string `json:"docker"`

	Volumes []Volume `json:"volumes,omitempty"`

	docker
}

// ContainerInfo wrapper for singularity container
type ContainerInfo struct {
	Type   string `json:"type"`
	Docker docker `json:"docker"`
}

type docker struct {
	// The docker image that is going to be passed to the registry.
	Image string `json:"image"`
	// Allowing arbitrary parameters to be passed to docker CLI.
	// Note that anything passed to this field is not guaranteed
	// to be supported moving forward, as we might move away from
	// the docker CLI.
	Parameters map[string]string `json:"parameters,omitempty"`

	PortMappings []PortMapping `json:"portMappings,omitempty"`

	Network string `json:"network,omitempty"`
}

// PortMapping https://github.com/HubSpot/Singularity/blob/master/Docs/reference/api.md#model-SingularityDockerPortMapping
type PortMapping struct {
	HostPort      int `json:"hostPort"`
	ContainerPort int `json:"containerPort"`
}

// Volume https://github.com/HubSpot/Singularity/blob/master/Docs/reference/api.md#-singularityvolume
type Volume struct {
	HostPath      string `json:"hostPath"`
	ContainerPath string `json:"containerPath"`
	Mode          string `json:"mode,omitempty"`
}
