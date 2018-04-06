package singularity

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
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

func TestPost_Success(t *testing.T) {
	sc := fakeClient(t)
	payload := "test post request"
	resp, err := sc.post("TestPost_Success", payload)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if string(b) != payload {
		t.Fatalf("got unexpected response: %q; expected: %q", string(b), payload)
	}
}

func TestPost_Fail(t *testing.T) {
	sc := fakeClient(t)
	_, err := sc.post("TestPost_Fail", "")
	if err == nil {
		t.Fatalf("expected to get error")
	}
}

func TestGet_Success(t *testing.T) {
	sc := fakeClient(t)
	resp, err := sc.get("TestGet_Success")
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected to get %d; got %d instead", http.StatusOK, resp.StatusCode)
	}
}

func TestGet_Fail(t *testing.T) {
	sc := fakeClient(t)
	resp, err := sc.get("TestGet_Fail")
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("expected to get not %d; got %d instead", http.StatusOK, resp.StatusCode)
	}
}

func singularityHandler(rw http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		switch r.URL.Path {
		case "/singularity/api/TestPost_Success":
			b, _ := ioutil.ReadAll(r.Body)
			rw.Write(b)
		case "/singularity/api/TestPost_Fail":
			rw.WriteHeader(http.StatusInternalServerError)
		}
		return
	}
	if r.Method == "GET" {
		switch r.URL.Path {
		case "/singularity/api/TestGet_Success":
			rw.WriteHeader(http.StatusOK)
		case "/singularity/api/TestGet_Fail":
			rw.WriteHeader(http.StatusInternalServerError)
		}
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
