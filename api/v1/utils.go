package v1

import (
	"encoding/json"
	"fmt"
	"io"
)

func decodeInto(rc io.ReadCloser, v interface{}) error {
	decoder := json.NewDecoder(rc)
	if err := decoder.Decode(v); err != nil {
		return fmt.Errorf("decoding error: %s", err)
	}
	rc.Close()
	return nil
}

func requiredError(field string) error {
	return fmt.Errorf("field %q required to be set", field)
}

func envName(name string) string {
	if len(envPrefix) == 0 {
		return name
	}
	return fmt.Sprintf("%s_%s", envPrefix, name)
}
