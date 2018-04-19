// +build integration

package v1

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/neuromation/platform-api/api/v1/config"
	"github.com/neuromation/platform-api/log"
)

const testDir = "./testdata/temp"

func TestMain(m *testing.M) {
	// create userSpace dir according to fixture /testdata/fixtures/good.model.json
	if err := os.MkdirAll(testDir+"/userSpace", 0700); err != nil {
		log.Fatalf("unable to create dir %q: %s", testDir, err)
	}
	// create dir with dataset according to fixture /testdata/fixtures/good.model.json
	if err := os.MkdirAll(testDir+"/storage/people/dataset", 0700); err != nil {
		log.Fatalf("unable to create dir %q: %s", testDir, err)
	}
	retCode := m.Run()
	if err := os.RemoveAll(testDir); err != nil {
		log.Fatalf("cannot remove %q: %s", testDir, err)
	}
	os.Exit(retCode)
}

const testAddr = "http://127.0.0.1:8080"

func runAPI() {
	cfg := &config.Config{}
	err := envconfig.Process("neuro", cfg)
	if err != nil {
		log.Fatalf("error while parsing config: %s", err)
	}
	cfg.StorageBasePath = testDir + "/storage"
	go Serve(cfg)

	maxWait := time.Second * 10
	done := time.Now().Add(maxWait)
	for time.Now().Before(done) {
		resp, _ := http.DefaultClient.Get(testAddr)
		if resp != nil && resp.StatusCode == http.StatusOK {
			return
		}
		time.Sleep(time.Second)
	}
	log.Fatalf("Unable to run API")
}

func TestServe_Integration(t *testing.T) {
	runAPI()
	httpClient := newHttpClient()
	taskResultsDir := testDir + "/userSpace"
	files, err := ioutil.ReadDir(taskResultsDir)
	if err != nil {
		t.Fatalf("unable to read dir: %s", err)
	}
	if len(files) > 0 {
		t.Fatalf("userSpace must be empty; got %d files instead", len(files))
	}

	reqBody, err := ioutil.ReadFile("./testdata/fixtures/integration.model.json")
	if err != nil {
		t.Fatalf("unable to read fixture: %s", err)
	}
	r := bytes.NewReader(reqBody)
	resp, err := httpClient.Post(testAddr+"/models", "application/json", r)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if resp.StatusCode > 299 {
		responseBody, _ := ioutil.ReadAll(resp.Body)
		t.Fatalf("unexpceted status code received: %d; Response body: %s", resp.StatusCode, string(responseBody))
	}
	status := &struct {
		ID string `json:"status_id"`
	}{}
	if err := decodeInto(resp.Body, status); err != nil {
		t.Fatalf("unexpected error while decoding response body: %s", err)
	}

	checkState := func() bool {
		addr := fmt.Sprintf(testAddr+"/statuses/%s", status.ID)
		resp, err := httpClient.Get(addr)
		if err != nil {
			t.Fatalf("fail to get request state: %s", err)
		}
		jobState := &struct {
			Status string `json:"status"`
		}{}
		if err := decodeInto(resp.Body, jobState); err != nil {
			t.Fatalf("unexpected error while decoding status body: %s", err)
		}
		return jobState.Status == "SUCCEEDED"
	}
	maxWait := time.Second * 10
	done := time.Now().Add(maxWait)
	for time.Now().Before(done) {
		time.Sleep(time.Second)
		if checkState() {
			files, err = ioutil.ReadDir(taskResultsDir)
			if err != nil {
				t.Fatalf("unable to read dir: %s", err)
			}
			if len(files) == 5 {
				return
			}
		}
	}
	t.Fatalf("job doesn't finished for %v", maxWait)
}

func newHttpClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
