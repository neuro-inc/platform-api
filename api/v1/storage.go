package v1

import (
	"encoding/json"
	"fmt"
	"io"
)

type Storage struct {
	ID   string            `json:"id"`
	Name string            `json:"name"`
	Type string            `json:"type"`
	Meta map[string]string `json:"meta"`
}

// String implements the Stringer interface
func (s Storage) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

var storageRegistry = map[string]Storage{
	"av8475f7-56db-471a-bb15-76c324e2cfaa": {
		ID:   "av8475f7-56db-471a-bb15-76c324e2cfaa",
		Name: "data_storage",
		Type: "DIRECTORY",
		Meta: make(map[string]string),
	},
}

func ListStorage(w io.Writer) {
	fmt.Fprint(w, "[")
	var i int
	for _, v := range storageRegistry {
		i++
		fmt.Fprint(w, v)
		if i < len(storageRegistry)-1 {
			fmt.Fprint(w, ",")
		}
	}
	fmt.Fprint(w, "]")
}
