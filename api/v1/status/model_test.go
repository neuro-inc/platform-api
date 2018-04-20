package status

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestNewModelStatus(t *testing.T) {
	modelId := "someModelId"
	modelUrl := "http://host/path"
	status := NewModelStatus(modelId, modelUrl, nil)

	assertEqual(t, len(status.Id()), 36, "statusID len must be 36")
	assertEqual(t, status.StatusName(), STATUS_PENDING, "")
	assertEqual(t, status.ModelId, modelId, "")
	assertEqual(t, status.IsHttpRedirectSupported(), true, "")
	assertEqual(t, status.HttpRedirectUrl(), modelUrl, "")
	assertEqual(t, status.IsSucceeded(), false, "")
	assertEqual(t, status.IsFailed(), false, "")
	assertEqual(t, status.IsFinished(), false, "")
}

func TestMarshaledModelStatus(t *testing.T) {
	modelId := "someModelId"
	status := NewModelStatus(modelId, "", nil)
	b, err := json.Marshal(&status)
	if err != nil {
		t.Fatal(err)
	}

	got := string(b)
	expected := fmt.Sprintf(
		`{"status_id":"%s","status":"PENDING","model_id":"%s"}`,
		status.Id(), modelId)
	if got != expected {
		t.Fatalf("got: %s/nexpected: %s", got, expected)
	}
}
