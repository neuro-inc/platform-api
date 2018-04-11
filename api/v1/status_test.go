package v1

import (
	"testing"
)


func TestStatusNameString(t *testing.T) {
	name := STATUS_SUCCEEDED
	if name.String() != "SUCCEEDED" {
		t.Fatal()
	}
}
