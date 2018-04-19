package status

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestNewBatchInferenceStatus(t *testing.T) {
	bID := "someBatchInferenceID"
	url := "http://host/path"
	status := NewBatchInferenceStatus(bID, url, nil)

	assertEqual(t, status.StatusName(), STATUS_PENDING, "")
	assertEqual(t, status.BatchInferenceID, bID, "")
	assertEqual(t, status.IsHttpRedirectSupported(), true, "")
	assertEqual(t, status.HttpRedirectUrl(), url, "")
	assertEqual(t, status.IsSucceeded(), false, "")
	assertEqual(t, status.IsFailed(), false, "")
	assertEqual(t, status.IsFinished(), false, "")
}

func TestMarshaledBatchInferenceStatus(t *testing.T) {
	bID := "someBatchInferenceID"
	status := NewBatchInferenceStatus(bID, "", nil)
	b, err := json.Marshal(&status)
	if err != nil {
		t.Fatal(err)
	}

	got := string(b)
	expected := fmt.Sprintf(
		`{"status_id":"%s","status":"PENDING","batch_inference_id":"%s"}`,
		status.Id(), bID)
	if got != expected {
		t.Fatalf("got: %s/nexpected: %s", got, expected)
	}
}
