package main

import (
	"github.com/neuromation/platform-api/log"
	"net/http"
	"crypto/tls"
	"time"
	"net"
	"fmt"
)

var listenAddr string

func main() {
	log.Infof("Initing...")

	ln, err := net.Listen("tcp4", listenAddr)
	if err != nil {
		log.Fatalf("cannot listen for %q: %s", listenAddr, err)
	}
	s := &http.Server{
		TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler)),
		Handler:      http.HandlerFunc(handler),
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
		IdleTimeout:  time.Minute * 10,
	}
	log.Fatalf("HTTP server error on %s: %s", listenAddr, s.Serve(ln))
}

func handler(rw http.ResponseWriter, req *http.Request) {
	rw.WriteHeader(http.StatusOK)
	fmt.Fprint(rw, "Hello world!")
}