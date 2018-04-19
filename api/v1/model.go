package v1

import (
	"encoding/json"
	"fmt"

	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/storage"
)

type model struct {
	// TODO: rename `code` in API doc to smthng intuitive
	Container container.Container `json:"code"`
	Resources container.Resources `json:"resources"`

	// Storage URI where dataset sits
	DatasetStorageURI volumeRO `json:"dataset_storage_uri,omitempty"`

	// Storage URI where artifacts should be saved
	ResultStorageURI volumeRW `json:"result_storage_uri,omitempty"`

	Meta map[string]string `json:"meta,omitempty"`
}

func (m *model) UnmarshalJSON(data []byte) error {
	type plain model
	if err := json.Unmarshal(data, (*plain)(m)); err != nil {
		return err
	}

	if len(m.Resources) == 0 {
		return fmt.Errorf("resources must be set")
	}

	if _, ok := m.Resources["cpus"]; !ok {
		return fmt.Errorf("resources.cpus param must be set")
	}

	if _, ok := m.Resources["memoryMb"]; !ok {
		return fmt.Errorf("resources.memoryMb param must be set")
	}

	if len(m.Container.Env) == 0 {
		m.Container.Env = make(map[string]string)
	}

	if len(m.DatasetStorageURI.From) > 0 {
		v := container.Volume(m.DatasetStorageURI)
		m.Container.Volumes = append(m.Container.Volumes, &v)
		env := envName("PATH_DATASET")
		m.Container.Env[env] = m.DatasetStorageURI.To
	}
	if len(m.ResultStorageURI.From) > 0 {
		v := container.Volume(m.ResultStorageURI)
		m.Container.Volumes = append(m.Container.Volumes, &v)
		env := envName("PATH_RESULT")
		m.Container.Env[env] = m.ResultStorageURI.To
	}
	return nil
}

type volumeRO container.Volume

func (vro *volumeRO) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	v, err := newROVolume(s, containerStoragePath)
	if err != nil {
		return err
	}
	*vro = volumeRO(*v)
	return nil
}

type volumeRW container.Volume

func (vrw *volumeRW) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	v, err := newRWVolume(s, containerStoragePath)
	if err != nil {
		return err
	}
	*vrw = volumeRW(*v)
	return nil
}

func newROVolume(from, to string) (*container.Volume, error) {
	return newVolume(from, to, "RO")
}

func newRWVolume(from, to string) (*container.Volume, error) {
	return newVolume(from, to, "RW")
}

func newVolume(from, to, mode string) (*container.Volume, error) {
	pi, err := storage.Path(from)
	if err != nil {
		return nil, fmt.Errorf("invalid path %q: %s", from, err)
	}
	return &container.Volume{
		From: pi.Abs(),
		To:   fmt.Sprintf("%s/%s", to, pi.Relative()),
		Mode: mode,
	}, nil
}
