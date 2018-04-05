package v1

import (
	"encoding/json"
)

// storage descrives Storage from API docs
type storage struct {
	ID   string            `json:"id"`
	Name string            `json:"name"`
	Type string            `json:"type"`
	Meta map[string]string `json:"meta"`
}

// String implements the Stringer interface
func (s storage) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

var storageRegistry = map[string]storage{
	"av8475f7-56db-471a-bb15-76c324e2cfaa": {
		ID:   "av8475f7-56db-471a-bb15-76c324e2cfaa",
		Name: "data_storage",
		Type: "DIRECTORY",
		Meta: make(map[string]string),
	},
}
