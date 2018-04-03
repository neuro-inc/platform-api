package v1

import (
	"encoding/json"
	"fmt"
)

type Training struct {
	Code   string            `json:"code"`
	Model  string            `json:"model,omitempty"`
	DataID string            `json:"data_id,omitempty"`
	Meta   map[string]string `json:"meta,omitempty"`
}

func (t *Training) UnmarshalJSON(b []byte) error {
	type plain Training
	if err := json.Unmarshal(b, (*plain)(t)); err != nil {
		return err
	}

	if len(t.Model) > 0 {

	}
	if _, ok := modelRegistry[t.Model]; !ok {
		return fmt.Errorf("unknown model id %q", t.Model)
	}

	if _, ok := storageRegistry[t.Model]; !ok {
		return fmt.Errorf("unknown storage id %q", t.DataID)
	}

	return nil
}
