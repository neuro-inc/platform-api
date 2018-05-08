package status

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/neuromation/platform-api/log"
)

func TestMain(m *testing.M) {
	log.SuppressOutput(true)
	retCode := m.Run()
	log.SuppressOutput(false)
	os.Exit(retCode)
}

func TestStatusNameString(t *testing.T) {
	name := STATUS_SUCCEEDED
	if name.String() != "SUCCEEDED" {
		t.Fatal()
	}
}

func TestStatusNameMarshalJSON(t *testing.T) {
	name := STATUS_SUCCEEDED
	name_json, err := json.Marshal(name)
	if err != nil {
		t.Fatal(err)
	}
	if string(name_json) != `"SUCCEEDED"` {
		t.Fatal()
	}
}

func TestNewGenericStatus(t *testing.T) {
	status := NewGenericStatus()
	// TODO: normal assertions in go?
	if len(status.Id()) != 36 {
		t.Fatal()
	}
	if status.StatusName() != STATUS_PENDING {
		t.Fatal()
	}
	if status.IsHttpRedirectSupported() {
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

func TestGenericStatusIsFinished(t *testing.T) {
	status := NewGenericStatus()
	status.statusName = STATUS_SUCCEEDED

	if !status.IsSucceeded() {
		t.Fatal()
	}

	if !status.IsFinished() {
		t.Fatal()
	}
}

func TestMarshaledStatus(t *testing.T) {
	status := NewGenericStatus()
	status_json, err := json.Marshal(&status)
	if err != nil {
		t.Fatal(err)
	}

	status_json_str := string(status_json[:])
	expected_status_json_str := fmt.Sprintf(
		`{"status_id":"%s","status":"PENDING"}`, status.Id())
	if status_json_str != expected_status_json_str {
		t.Fatal(status_json_str)
	}
}

type TestPoller struct{}

func (tp *TestPoller) Update(js *JobStatus) error {
	js.SetStatusName(STATUS_SUCCEEDED)
	return nil
}

func TestJobStatusUpdate(t *testing.T) {
	status := NewJobStatus(NewGenericStatus(), &TestPoller{})
	if status.StatusName() != STATUS_PENDING {
		t.Fatal()
	}
	status.update()
	if status.StatusName() != STATUS_SUCCEEDED {
		t.Fatal()
	}
}

func TestInMemoryStatusServiceSetGet(t *testing.T) {
	service := NewInMemoryStatusService()
	var status Status = NewGenericStatus()
	service.Set(status)

	statusId := status.Id()

	if len(statusId) != 36 {
		t.Fatal()
	}

	if status.StatusName() != STATUS_PENDING {
		t.Fatal()
	}

	status, err := service.Get(statusId)

	if err != nil {
		t.Fatal(err)
	}

	if status.Id() != statusId {
		t.Fatal()
	}

	if status.StatusName() != STATUS_PENDING {
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
	var status Status = NewGenericStatus()
	service.Set(status)

	_, err := service.Get(status.Id())
	if err != nil {
		t.Fatal()
	}

	service.Delete(status.Id())

	_, err = service.Get(status.Id())
	if err == nil {
		t.Fatal()
	}
}

func TestInMemoryStatusServiceGetModelStatus(t *testing.T) {
	service := NewInMemoryStatusService()
	modelId := "someId"
	modelUrl := "http://host/path"
	var status Status = NewModelStatus(modelId, modelUrl, &TestPoller{})
	service.Set(status)

	status, err := service.Get(status.Id())
	if err != nil {
		t.Fatal()
	}
	if !status.IsSucceeded() {
		t.Fatal()
	}
}

func TestInMemoryStatusServiceGetBatchInferenceStatus(t *testing.T) {
	service := NewInMemoryStatusService()
	biId := "someId"
	biUrl := "http://host/path"
	var status Status = NewModelStatus(biId, biUrl, &TestPoller{})
	service.Set(status)

	status, err := service.Get(status.Id())
	if err != nil {
		t.Fatal()
	}
	if !status.IsSucceeded() {
		t.Fatal()
	}
}

func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
	if a == b {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("%v != %v", a, b)
	}
	t.Fatal(message)
}
