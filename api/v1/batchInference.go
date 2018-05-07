package v1

import (
	"encoding/json"
	"github.com/neuromation/platform-api/api/v1/container"
)

type batchInference struct {
	Container *container.Container `json:"container"`
	Resources container.Resources  `json:"resources"`

	// Storage URI where dataset sits
	DatasetStorageURI container.VolumeRO `json:"dataset_storage_uri"`

	// Storage URI where model sits
	ModelStorageURI container.VolumeRO `json:"model_storage_uri"`

	// Storage URI where artifacts should be saved
	ResultStorageURI container.VolumeRW `json:"result_storage_uri"`
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (bi *batchInference) UnmarshalJSON(data []byte) error {
	type plain batchInference
	if err := json.Unmarshal(data, (*plain)(bi)); err != nil {
		return err
	}

	if bi.Container == nil {
		return requiredError("container")
	}

	if len(bi.Resources) == 0 {
		return requiredError("resources")
	}

	if len(bi.DatasetStorageURI.From) == 0 {
		return requiredError("dataset_storage_uri")
	}

	if len(bi.ResultStorageURI.From) == 0 {
		return requiredError("result_storage_uri")
	}

	if len(bi.ModelStorageURI.From) == 0 {
		return requiredError("model_storage_uri")
	}

	ds := container.Volume(bi.DatasetStorageURI)
	bi.Container.Volumes = append(bi.Container.Volumes, &ds)
	bi.Container.Env[envName("PATH_DATASET")] = bi.DatasetStorageURI.To

	rs := container.Volume(bi.ResultStorageURI)
	bi.Container.Volumes = append(bi.Container.Volumes, &rs)
	bi.Container.Env[envName("PATH_RESULT")] = bi.ResultStorageURI.To

	ms := container.Volume(bi.ModelStorageURI)
	bi.Container.Volumes = append(bi.Container.Volumes, &ms)
	bi.Container.Env[envName("PATH_MODEL")] = bi.ModelStorageURI.To

	return nil
}
