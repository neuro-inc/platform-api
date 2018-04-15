package status

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

	if status.StatusName != STATUS_PENDING {
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

	if status.StatusName != STATUS_PENDING {
		t.Fatal()
	}
}

func TestInMemoryStatusServiceGetFailure(t *testing.T) {
	service := NewInMemoryStatusService()

	status, err := service.Get("unknown id")

	if status != nil {
		t.Fatal()
	}

	if err == nil {
		t.Fatal()
	}

	if err.Error() != "Status unknown id was not found" {
		t.Fatal()
	}
}

func TestInMemoryStatusServiceDelete(t *testing.T) {
	service := NewInMemoryStatusService()
	status := service.Create()

	_, err := service.Get(status.Id)
	if err != nil {
		t.Fatal()
	}

	service.Delete(status.Id)

	_, err = service.Get(status.Id)
	if err == nil {
		t.Fatal()
	}
}
