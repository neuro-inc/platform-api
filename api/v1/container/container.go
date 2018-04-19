package container

import (
	"encoding/json"
	"fmt"
	"github.com/neuromation/platform-api/api/v1/storage"
)

// path in the container where to mount external storage links
var containerStoragePath = "/var/storage"

func SetPath(path string) {
	containerStoragePath = path
}

// Container struct describes abstract container
type Container struct {
	// The docker image that is going to be passed to the registry.
	Image string `json:"image"`

	// Environment variables passed into container
	Env map[string]string `json:"env,omitempty"`

	Volumes []*Volume
}

func (c *Container) UnmarshalJSON(data []byte) error {
	type plain Container
	if err := json.Unmarshal(data, (*plain)(c)); err != nil {
		return err
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

type VolumeRO Volume

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

type VolumeRW Volume

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

func (r *Resources) UnmarshalJSON(data []byte) error {
	var m map[string]float64
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	if len(m) == 0 {
		return fmt.Errorf("resources must be set")
	}

	if _, ok := m["cpus"]; !ok {
		return fmt.Errorf("resources.cpus param must be set")
	}

	if _, ok := m["memoryMb"]; !ok {
		return fmt.Errorf("resources.memoryMb param must be set")
	}

	*r = m
	return nil
}
