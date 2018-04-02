package main

import (
	"github.com/neuromation/platform-api/log"
	"net/http"
	"time"
	"net"
	"fmt"
	"github.com/kelseyhightower/envconfig"

	"github.com/neuromation/platform-api/config"
)

var listenAddr string

func main() {
	log.Infof("Initing...")
	var cfg config.Config
	err := envconfig.Process("neuro", &cfg)
	if err != nil {
		log.Fatalf("error while parsing config: %s", err)
	}

	ln, err := net.Listen("tcp4", listenAddr)
	if err != nil {
		log.Fatalf("cannot listen for %q: %s", listenAddr, err)
	}
	s := &http.Server{
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