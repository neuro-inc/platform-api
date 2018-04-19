package v1

import (
	"encoding/json"
	"github.com/neuromation/platform-api/api/v1/container"
)

type batchInference struct {
	// TODO: rename `code` in API doc to smthng intuitive
	Container container.Container `json:"code"`
	Resources container.Resources `json:"resources"`

	// Storage URI where dataset sits
	DatasetStorageURI container.VolumeRO `json:"dataset_storage_uri"`

	// Storage URI where model sits
	ModelStorageURI container.VolumeRO `json:"model_storage_uri"`

	// Storage URI where artifacts should be saved
	ResultStorageURI container.VolumeRW `json:"result_storage_uri"`
}

func (bi *batchInference) UnmarshalJSON(data []byte) error {
	type plain batchInference
	if err := json.Unmarshal(data, (*plain)(bi)); err != nil {
		return err
	}

	if len(bi.DatasetStorageURI.From) == 0 {
		return errorRequired("dataset_storage_uri")
	}

	if len(bi.ResultStorageURI.From) == 0 {
		return errorRequired("result_storage_uri")
	}

	if len(bi.ModelStorageURI.From) == 0 {
		return errorRequired("model_storage_uri")
	}

	v := container.Volume(bi.DatasetStorageURI)
	bi.Container.Volumes = append(bi.Container.Volumes, &v)
	bi.Container.Env[envName("PATH_DATASET")] = bi.DatasetStorageURI.To

	v = container.Volume(bi.ResultStorageURI)
	bi.Container.Volumes = append(bi.Container.Volumes, &v)
	bi.Container.Env[envName("PATH_RESULT")] = bi.ResultStorageURI.To

	v = container.Volume(bi.ModelStorageURI)
	bi.Container.Volumes = append(bi.Container.Volumes, &v)
	bi.Container.Env[envName("PATH_MODEL")] = bi.ModelStorageURI.To

	return nil
}
