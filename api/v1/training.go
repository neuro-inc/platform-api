package v1

import (
	"encoding/json"
	"fmt"
)

type Training struct {
	Code   string            `json:"code"`
	Model  string            `json:"model"`
	DataID string            `json:"data_id"`
	Meta   map[string]string `json:"meta"`
}

func (t *Training) UnmarshalJSON(b []byte) error {
	type plain Training
	if err := json.Unmarshal(b, (*plain)(t)); err != nil {
		return err
	}

	if _, ok := modelRegistry[t.Model]; !ok {
		return fmt.Errorf("unknown model id %q", t.Model)
	}

	if _, ok := storageRegistry[t.Model]; !ok {
		return fmt.Errorf("unknown storage id %q", t.DataID)
	}

	return nil
}
