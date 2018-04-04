package v1

import (
	"encoding/json"
)

// Model describes Model from API doc
type Model struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"type"`
	Meta        map[string]string `json:"meta"`
}

// String implements the Stringer interface
func (m Model) String() string {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(b)
}

var modelRegistry = map[string]Model{
	"fc1834f7-56db-471a-bb15-76c452e2cfdd": {
		ID:          "fc1834f7-56db-471a-bb15-76c452e2cfdd",
		Name:        "perfectModel",
		Description: "100% accuracy",
		Meta:        make(map[string]string),
	},
}
