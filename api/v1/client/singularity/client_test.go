package singularity

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/neuromation/platform-api/api/v1/container"
)

func TestNewClient_Fail(t *testing.T) {
	_, err := NewClient("localhost", time.Second)
	if err == nil {
		t.Fatalf("expected to get error")
	}
	_, err = NewClient("", time.Second)
	if err == nil {
		t.Fatalf("expected to get error")
	}
}

func TestNewClient_Success(t *testing.T) {
	testCases := []struct {
		timeout time.Duration
		addr    string
	}{
		{
			time.Second,
			"http://localhost",
		},
		{
			time.Second * 10,
			"http://localhost:8080",
		},
		{
			time.Minute,
			"https://127.0.0.1",
		},
	}
	for _, tc := range testCases {
		c, err := NewClient(tc.addr, tc.timeout)
		if err != nil {
			t.Fatalf("expected client create successfully; got err: %s", err)
		}
		sc := c.(*singularityClient)
		if sc.c.Timeout != tc.timeout {
			t.Fatalf("expected clinet Timeout to be %v; got %v instead", tc.timeout, sc.c.Timeout)
		}
		if sc.addr.String() != tc.addr {
			t.Fatalf("expected client addr to be %q; got %q instead", tc.addr, sc.addr)
		}
	}

}

func TestGetPost(t *testing.T) {
	sc := fakeClient(t)
	testCases := []struct {
		name        string
		addr        string
		expectedErr string
	}{
		{
			"success",
			"test/success",
			"",
		},
		{
			"failure",
			"test/failure",
			"unexpected status code returned ",
		},
		{
			"timeout",
			"test/timeout",
			"net/http: request canceled (Client.Timeout exceeded while awaiting headers)",
		},
	}
	for _, tc := range testCases {
		checkErr := func(err error) {
			var gotErr string
			if err != nil {
				gotErr = err.Error()
			}
			if !strings.Contains(gotErr, tc.expectedErr) {
				t.Fatalf("expected to get err: %s; got instead: %q", tc.expectedErr, gotErr)
			}
		}
		t.Run("POST"+tc.name, func(t *testing.T) {
			_, err := sc.post(tc.addr, "{}")
			checkErr(err)
		})
		t.Run("GET"+tc.name, func(t *testing.T) {
			_, err := sc.get(tc.addr)
			checkErr(err)
		})
	}
}

func TestSingularityJob_Start2(t *testing.T) {
	sc := fakeClient(t)
	job := sc.NewJob(container.Container{}, container.Resources{})
	if err := job.Start(); err != nil {
		t.Fatalf("unexpected job.Start() error: %s", err)
	}
}

var serverState = &singularityState{
	r: make(map[string]struct{}),
}

func singularityHandler(rw http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/singularity/api/test/success":
		rw.WriteHeader(http.StatusOK)
	case "/singularity/api/test/failure":
		rw.WriteHeader(http.StatusInternalServerError)
	case "/singularity/api/test/timeout":
		time.Sleep(time.Second * 5)
	case "/singularity/api/deploys":
		if r.Method != "POST" {
			respondWith(rw, "non-POST request received", http.StatusBadRequest)
			return
		}
		decoder := json.NewDecoder(r.Body)
		deploy := &singularityJob{}
		err := decoder.Decode(deploy)
		if err != nil {
			respondWith(rw, "unable to decode deploy payload", http.StatusBadRequest)
			return
		}
		r.Body.Close()
		if len(deploy.Deploy.ID) < 1 {
			respondWith(rw, "got empty deploy ID", http.StatusBadRequest)
			return
		}
		if !serverState.get(deploy.Deploy.RequestID) {
			respondWith(rw, "no RequestID in server state. Probably /deploys was called before /requests", http.StatusBadRequest)
			return
		}
	case "/singularity/api/requests":
		if r.Method != "POST" {
			respondWith(rw, "non-POST request received", http.StatusBadRequest)
			return
		}
		decoder := json.NewDecoder(r.Body)
		request := &struct {
			ID          string `json:"id"`
			RequestType string `json:"requestType"`
		}{}
		err := decoder.Decode(request)
		if err != nil {
			respondWith(rw, "unable to decode request payload", http.StatusBadRequest)
			return
		}
		r.Body.Close()
		if len(request.ID) < 1 {
			respondWith(rw, "got empty request ID", http.StatusBadRequest)
			return
		}
		serverState.set(request.ID)
		expType := "RUN_ONCE"
		if request.RequestType != expType {
			respondWith(rw, fmt.Sprintf("got requestType %q; expected to be %q", request.RequestType, expType), http.StatusBadRequest)
			return
		}
	default:
		respondWith(rw, fmt.Sprintf("unsupported path: %s", r.URL.Path), http.StatusBadRequest)
	}
}

func respondWith(rw http.ResponseWriter, msg string, sc int) {
	rw.WriteHeader(sc)
	fmt.Fprint(rw, msg)
}

func fakeClient(t *testing.T) *singularityClient {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(singularityHandler))
	c, err := NewClient(s.URL, time.Second)
	if err != nil {
		t.Fatalf("expected to create client successfully; got err: %s", err)
	}
	return c.(*singularityClient)
}



type singularityState struct {
	sync.Mutex
	// r is a registry of requests, where key is unique id of received request
	r map[string]struct{}
}

func (r *singularityState) set(key string) {
	r.Lock()
	r.r[key] = struct{}{}
	r.Unlock()
}

func (r *singularityState) get(key string) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.r[key]
	return ok
}
