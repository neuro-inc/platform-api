package container

type (
	// Container struct describes abstract container
	Container struct {
		// The docker image that is going to be passed to the registry.
		Image string

		// Environment variables passed into container
		Env map[string]string

		Volumes []*Volume
	}

	// Volume describes docker Volume object
	Volume struct {
		ContainerPath string `json:"containerPath"`
		HostPath      string `json:"hostPath"`
		Mode          string `json:"mode"`
	}

	// Resources contains a map where key is the name of resource, and value - amount of resource
	Resources map[string]float64
)
