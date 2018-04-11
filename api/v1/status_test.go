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

func TestInMemoryStatusServiceCreateGet(t *testing.T) {
	service := NewInMemoryStatusService()
	status := service.Create()

	if len(status.Id) != 36 {
		t.Fatal()
	}

	if status.Status != STATUS_PENDING {
		t.Fatal()
	}

	statusId := status.Id
	status, err := service.Get(statusId)

	if err != nil {
		t.Fatal(err)
	}
	
	if status.Id != statusId {
		t.Fatal()
	}

	if status.Status != STATUS_PENDING {
		t.Fatal()
	}
}
