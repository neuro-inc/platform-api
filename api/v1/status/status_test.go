package status

import (
	"encoding/json"
	"fmt"
	"testing"
)


func TestStatusNameString(t *testing.T) {
	name := STATUS_SUCCEEDED
	if name.String() != "SUCCEEDED" {
		t.Fatal()
	}
}

func TestNewStatus(t *testing.T) {
	status := NewStatus()
	// TODO: normal assertions in go?
	if len(status.Id) != 36 {
		t.Fatal()
	}
	if status.StatusName != STATUS_PENDING {
		t.Fatal()
	}
	if status.IsRedirectionSupported() {
		t.Fatal()
	}
	if status.IsSucceeded() {
		t.Fatal()
	}
	if status.IsFailed() {
		t.Fatal()
	}
	if status.IsFinished() {
		t.Fatal()
	}
}

func TestMarshaledStatus(t *testing.T) {
	status := NewStatus()
	status_json, err := json.Marshal(&status)
	if err != nil {
		t.Fatal(err)
	}

	status_json_str := string(status_json[:])
	expected_status_json_str := fmt.Sprintf(
		`{"status_id":"%s","status":"PENDING"}`, status.Id) 
	if status_json_str != expected_status_json_str {
		t.Fatal(status_json_str)
	}
}

func TestNewModelStatus(t *testing.T) {
	modelId := "someModelId"
	status := NewModelStatus(modelId)
	// TODO: normal assertions in go?
	if len(status.Id) != 36 {
		t.Fatal()
	}
	if status.StatusName != STATUS_PENDING {
		t.Fatal()
	}
	if status.ModelId != modelId {
		t.Fatal()
	}
	if !status.IsRedirectionSupported() {
		t.Fatal()
	}
	if status.IsSucceeded() {
		t.Fatal()
	}
	if status.IsFailed() {
		t.Fatal()
	}
	if status.IsFinished() {
		t.Fatal()
	}
}

func TestMarshaledModelStatus(t *testing.T) {
	modelId := "someModelId"
	status := NewModelStatus(modelId)
	status_json, err := json.Marshal(&status)
	if err != nil {
		t.Fatal(err)
	}

	status_json_str := string(status_json[:])
	expected_status_json_str := fmt.Sprintf(
		`{"status_id":"%s","status":"PENDING","model_id":"%s"}`,
		status.Id, modelId) 
	if status_json_str != expected_status_json_str {
		t.Fatal(status_json_str)
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
