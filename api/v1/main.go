package v1

import (
	"net/http"
	"time"
)

var (
	client http.Client
	addr   string
)

func Init(addr string, timeout time.Duration) {
	addr = addr
	client = http.Client{
		Timeout: timeout,
	}
}

func doRequest() {
	//client.Do()
}
