// +build integration

package v1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/neuromation/platform-api/api/v1/config"
	"github.com/neuromation/platform-api/log"
)

const testAddr = "http://127.0.0.1:8080"

func runAPI() {
	cfg := &config.Config{}
	err := envconfig.Process("neuro", cfg)
	if err != nil {
		log.Fatalf("error while parsing config: %s", err)
	}
	cfg.StorageBasePath = testDir + "/storage"
	if err := os.MkdirAll(cfg.StorageBasePath, 0700); err != nil {
		log.Fatalf("unable to create dir %q: %s", testDir, err)
	}
	go Serve(cfg)

	maxWait := time.Second * 10
	done := time.Now().Add(maxWait)
	client := newHttpClient()
	for time.Now().Before(done) {
		resp, _ := client.Get(testAddr)
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

	// create dir with dataset according to fixture /testdata/fixtures/good.model.json
	if err := os.MkdirAll(testDir+"/storage/people/dataset", 0700); err != nil {
		log.Fatalf("unable to create dir %q: %s", testDir, err)
	}

	checkStatus := func(id string) bool {
		addr := fmt.Sprintf(testAddr+"/statuses/%s", id)
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

	testCases := []struct {
		name string
		f    func(*testing.T)
	}{
		{
			"help",
			func(t *testing.T) {
				resp, err := httpClient.Get(testAddr)
				checkErr(t, err)
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("unexpected status code: %d; expected: %d", resp.StatusCode, http.StatusOK)
				}
				resp.Body.Close()
			},
		},
		{
			"unsupported path",
			func(t *testing.T) {
				resp, err := httpClient.Get(testAddr + "/whatever")
				checkErr(t, err)
				if resp.StatusCode != http.StatusNotFound {
					t.Fatalf("unexpected status code: %d; expected: %d", resp.StatusCode, http.StatusOK)
				}
				resp.Body.Close()
			},
		},
		{
			"view model",
			func(t *testing.T) {
				resp, err := httpClient.Get(testAddr + "/models/123")
				checkErr(t, err)
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("unexpected status code: %d; expected: %d", resp.StatusCode, http.StatusOK)
				}
				resp.Body.Close()
			},
		},
		{
			"corrupted model",
			func(t *testing.T) {
				body := fileReader(t, "integration.corrupted.json")
				resp, err := httpClient.Post(testAddr+"/models", "application/json", body)
				checkErr(t, err)

				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("unexpected sc: %d; expected: %d", resp.StatusCode, http.StatusBadRequest)
				}

				decoder := json.NewDecoder(resp.Body)
				r := &struct {
					Error string `json:"error"`
				}{}
				err = decoder.Decode(r)
				checkErr(t, err)
				resp.Body.Close()

				exp := "Bad model request"
				if exp != r.Error {
					t.Fatalf("expected %q; got %q", exp, r.Error)
				}
			},
		},
		{
			"corrupted batch-inference",
			func(t *testing.T) {
				body := fileReader(t, "integration.corrupted.json")
				resp, err := httpClient.Post(testAddr+"/batch-inference", "application/json", body)
				checkErr(t, err)

				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("unexpected sc: %d; expected: %d", resp.StatusCode, http.StatusBadRequest)
				}

				decoder := json.NewDecoder(resp.Body)
				r := &struct {
					Error string `json:"error"`
				}{}
				err = decoder.Decode(r)
				checkErr(t, err)
				resp.Body.Close()

				exp := "Bad batch-inference request"
				if exp != r.Error {
					t.Fatalf("expected %q; got %q", exp, r.Error)
				}
			},
		},
		{
			"model gif-generator",
			func(t *testing.T) {
				taskResultsDir := testDir + "/storage/userSpace/model/gif-generator"
				if err := os.MkdirAll(taskResultsDir, 0700); err != nil {
					log.Fatalf("unable to create dir %q: %s", testDir, err)
				}

				body := fileReader(t, "integration.model.json")
				resp, err := httpClient.Post(testAddr+"/models", "application/json", body)
				checkErr(t, err)

				id := getStatusIDFromResponse(t, resp)
				maxWait := time.Minute
				done := time.Now().Add(maxWait)
				for time.Now().Before(done) {
					time.Sleep(time.Second)
					if checkStatus(id) {
						assertFilesNumber(t, taskResultsDir, 1)
						return
					}
				}
				t.Fatalf("job doesn't finished for %v", maxWait)
			},
		},
		{
			"batch-inference gif-generator",
			func(t *testing.T) {
				taskResultsDir := testDir + "/storage/userSpace/batch-inference/gif-generator"
				if err := os.MkdirAll(taskResultsDir, 0700); err != nil {
					log.Fatalf("unable to create dir %q: %s", testDir, err)
				}

				body := fileReader(t, "integration.batch-inference.json")
				resp, err := httpClient.Post(testAddr+"/batch-inference", "application/json", body)
				checkErr(t, err)

				id := getStatusIDFromResponse(t, resp)
				maxWait := time.Minute
				done := time.Now().Add(maxWait)
				for time.Now().Before(done) {
					time.Sleep(time.Second)
					if checkStatus(id) {
						assertFilesNumber(t, taskResultsDir, 1)
						return
					}
				}
				t.Fatalf("job doesn't finished for %v", maxWait)
			},
		},
		{
			"CMD ubuntu",
			func(t *testing.T) {
				taskResultsDir := testDir + "/storage/userSpace/model/cmd"
				if err := os.MkdirAll(taskResultsDir, 0700); err != nil {
					log.Fatalf("unable to create dir %q: %s", testDir, err)
				}

				body := fileReader(t, "integration.model.cmd.json")
				resp, err := httpClient.Post(testAddr+"/models", "application/json", body)
				checkErr(t, err)

				id := getStatusIDFromResponse(t, resp)
				maxWait := 5 * time.Minute
				done := time.Now().Add(maxWait)
				for time.Now().Before(done) {
					time.Sleep(time.Second)
					if checkStatus(id) {
						output := fmt.Sprintf("%s/output.txt", taskResultsDir)
						fi, err := os.Stat(output)
						if err != nil {
							continue
						}
						if fi.Size() > 0 {
							return
						}
					}
				}
				t.Fatalf("job doesn't finished for %v", maxWait)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.f(t)
		})
	}
}

func newHttpClient() *http.Client {
	return &http.Client{
		Timeout: time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func getStatusIDFromResponse(t *testing.T, resp *http.Response) string {
	if resp.StatusCode > 299 {
		responseBody, _ := ioutil.ReadAll(resp.Body)
		t.Fatalf("unexpceted status code received: %d; Response body: %s", resp.StatusCode, string(responseBody))
	}
	s := &struct {
		ID string `json:"status_id"`
	}{}
	if err := decodeInto(resp.Body, s); err != nil {
		t.Fatalf("unexpected error while decoding response body: %s", err)
	}
	return s.ID
}

func fileReader(t *testing.T, src string) io.Reader {
	t.Helper()
	b, err := ioutil.ReadFile("./testdata/fixtures/" + src)
	if err != nil {
		t.Fatalf("unable to read fixture: %s", err)
	}
	return bytes.NewReader(b)
}

func checkErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func assertFilesNumber(t *testing.T, path string, expFilesNumber int) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		t.Fatalf("unable to read dir: %s", err)
	}
	filesNumber := len(files)
	if filesNumber != expFilesNumber {
		t.Fatalf("found %d of %d expected files in %s",
			filesNumber, expFilesNumber, path)
	}
}
