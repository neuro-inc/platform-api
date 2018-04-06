package singularity

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
		t.Run("POST" + tc.name, func(t *testing.T) {
			_, err := sc.post(tc.addr, "{}")
			checkErr(err)
		})
		t.Run("GET" + tc.name, func(t *testing.T) {
			_, err := sc.get(tc.addr)
			checkErr(err)
		})
	}
}

func singularityHandler(rw http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/singularity/api/test/success":
		rw.WriteHeader(http.StatusOK)
	case "/singularity/api/test/failure":
		rw.WriteHeader(http.StatusInternalServerError)
	case "/singularity/api/test/timeout":
		time.Sleep(time.Second * 5)
	}
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
