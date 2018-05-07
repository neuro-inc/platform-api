package v1

import (
	"encoding/json"

	"github.com/neuromation/platform-api/api/v1/container"
)

type model struct {
	Container *container.Container `json:"container"`
	Resources container.Resources  `json:"resources"`

	// Storage URI where dataset sits
	DatasetStorageURI container.VolumeRO `json:"dataset_storage_uri"`

	// Storage URI where artifacts should be saved
	ResultStorageURI container.VolumeRW `json:"result_storage_uri"`
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (m *model) UnmarshalJSON(data []byte) error {
	type plain model
	if err := json.Unmarshal(data, (*plain)(m)); err != nil {
		return err
	}

	if m.Container == nil {
		return requiredError("container")
	}

	if len(m.Resources) == 0 {
		return requiredError("resources")
	}

	if len(m.DatasetStorageURI.From) == 0 {
		return requiredError("dataset_storage_uri")
	}

	if len(m.ResultStorageURI.From) == 0 {
		return requiredError("result_storage_uri")
	}

	ds := container.Volume(m.DatasetStorageURI)
	m.Container.Volumes = append(m.Container.Volumes, &ds)
	m.Container.Env[envName("PATH_DATASET")] = m.DatasetStorageURI.To

	rs := container.Volume(m.ResultStorageURI)
	m.Container.Volumes = append(m.Container.Volumes, &rs)
	m.Container.Env[envName("PATH_RESULT")] = m.ResultStorageURI.To

	return nil
}
