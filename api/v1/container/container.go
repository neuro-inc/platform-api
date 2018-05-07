package container

import (
	"encoding/json"
	"fmt"

	"github.com/neuromation/platform-api/api/v1/storage"
)

// path in the container where to mount external storage links
var containerStoragePath = "/var/storage"

// SetPath sets default mount path for volumes
func SetPath(path string) {
	containerStoragePath = path
}

// Container struct describes abstract container
type Container struct {
	// The docker image that is going to be passed to the registry.
	Image string `json:"image"`

	// CMD contains a shell command to execute inside of container
	CMD string `json:"CMD,omitempty"`

	// Environment variables passed into container
	Env map[string]string `json:"env,omitempty"`

	Volumes []*Volume
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (c *Container) UnmarshalJSON(data []byte) error {
	type plain Container
	if err := json.Unmarshal(data, (*plain)(c)); err != nil {
		return err
	}

	if len(c.Image) == 0 {
		return requiredError("image")
	}

	if len(c.Env) == 0 {
		c.Env = make(map[string]string)
	}

	return nil
}

// Volume describes docker Volume object
type Volume struct {
	// From FS
	From string
	// To container
	To string
	// Mode permissions
	Mode string
}

// VolumeRO describes Read-Only volume
type VolumeRO Volume

// UnmarshalJSON implements the json.Unmarshaler interface.
func (vro *VolumeRO) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	v, err := newROVolume(s, containerStoragePath)
	if err != nil {
		return err
	}
	*vro = VolumeRO(*v)
	return nil
}

// VolumeRW describes Read-Write volume
type VolumeRW Volume

// UnmarshalJSON implements the json.Unmarshaler interface.
func (vrw *VolumeRW) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	v, err := newRWVolume(s, containerStoragePath)
	if err != nil {
		return err
	}
	*vrw = VolumeRW(*v)
	return nil
}

func newROVolume(from, to string) (*Volume, error) {
	return newVolume(from, to, "RO")
}

func newRWVolume(from, to string) (*Volume, error) {
	return newVolume(from, to, "RW")
}

func newVolume(from, to, mode string) (*Volume, error) {
	pi, err := storage.Path(from)
	if err != nil {
		return nil, fmt.Errorf("invalid path %q: %s", from, err)
	}
	return &Volume{
		From: pi.Abs(),
		To:   fmt.Sprintf("%s/%s", to, pi.Relative()),
		Mode: mode,
	}, nil
}

// Resources contains a map where key is the name of resource, and value - amount of resource
type Resources map[string]float64

// UnmarshalJSON implements the json.Unmarshaler interface.
func (r *Resources) UnmarshalJSON(data []byte) error {
	var m map[string]float64
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	if len(m) == 0 {
		return requiredError("resources")
	}

	if _, ok := m["cpus"]; !ok {
		return requiredError("resources.cpus")
	}

	if _, ok := m["memoryMb"]; !ok {
		return requiredError("resources.memoryMb")
	}

	*r = m
	return nil
}

func requiredError(field string) error {
	return fmt.Errorf("field %q required to be set", field)
}
