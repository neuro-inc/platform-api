package v1

import (
	"encoding/json"
	"fmt"

	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/storage"
)

type training struct {
	// TODO: rename `code` in API doc to smthng intuitive
	Container container.Container `json:"code"`
	Resources container.Resources `json:"resources"`

	Meta map[string]string `json:"meta,omitempty"`
}

func (t *training) UnmarshalJSON(data []byte) error {
	type plain training
	if err := json.Unmarshal(data, (*plain)(t)); err != nil {
		return err
	}
	for _, s := range t.Container.Storage {
		pi, err := storage.Path(s)
		if err != nil {
			return fmt.Errorf("invalid storage path: %s", err)
		}
		v := container.Volume{
			From: pi.Abs(),
			To:   "/var/marketplace/" + pi.Relative(),
			Mode: "RO",
		}
		t.Container.Volumes = append(t.Container.Volumes, v)
	}
	return nil
}
