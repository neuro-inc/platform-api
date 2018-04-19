package container

type (
	// Container struct describes abstract container
	Container struct {
		// The docker image that is going to be passed to the registry.
		Image string `json:"image"`

		// Environment variables passed into container
		Env map[string]string `json:"env,omitempty"`

		// Storage describes RO bindings from some FS to container
		Storage []string `json:"storage,omitempty"`

		Volumes []*Volume
	}

	// Volume describes docker Volume object
	Volume struct {
		// From FS
		From string
		// To container
		To string
		// Mode permissions
		Mode string
	}

	// Resources contains a map where key is the name of resource, and value - amount of resource
	Resources map[string]float64
)
