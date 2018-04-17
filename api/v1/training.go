package v1

import (
	"github.com/neuromation/platform-api/api/v1/container"
)

type training struct {
	// TODO: rename `code` in API doc to smthng intuitive
	Container container.Container `json:"code"`
	Resources container.Resources `json:"resources"`

	// Storage URI where dataset sits
	DatasetStorageURI string `json:"dataset_storage_uri,omitempty"`

	// Storage URI where model artifacts should be saved
	ModelStorageURI string `json:"model_storage_uri,omitempty"`

	Meta map[string]string `json:"meta,omitempty"`
}
