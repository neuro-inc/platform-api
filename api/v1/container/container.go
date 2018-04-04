package container

// Container struct describes abstract container
type Container struct {
	// The docker image that is going to be passed to the registry.
	Image string `json:"image"`
	// Allowing arbitrary parameters to be passed to docker CLI.
	// Note that anything passed to this field is not guaranteed
	// to be supported moving forward, as we might move away from
	// the docker CLI.
	Parameters map[string]string `json:"parameters,omitempty"`

	Env map[string]string `json:"env,omitempty"`

	PortMappings []PortMapping `json:"portMappings,omitempty"`

	Volumes []Volume `json:"volumes,omitempty"`

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

// Resources contains a map where key is the name of resource, and value - amount of resource
type Resources map[string]float64
