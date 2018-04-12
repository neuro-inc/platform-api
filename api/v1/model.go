package v1

type model struct {
	// Image points to the docker image name
	Image string `json:"image"`
	// Storage describes RO bindings from some FS to container
	Storage []string `json:"storage,omitempty"`
	// Meta contains various configuration options
	Meta meta `json:"meta,omitempty"`
}

type meta struct {
	// Env contains map of environment variables passing to container
	Env map[string]string `json:"env"`
	// Resources contains list resources needed to run container
	Resources resources `json:"resources"`
}

type resources struct {
	// Cpus contains amount of requested cpus for container
	Cpus int `json:"cpus"`
	// MemoryMB contains amount of requested memory for container
	MemoryMB int `json:"memoryMb"`
}
