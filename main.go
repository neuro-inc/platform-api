package main

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/neuromation/platform-api/config"
	"github.com/neuromation/platform-api/log"
)

func main() {
	log.Infof("Initing...")
	var cfg config.Config
	err := envconfig.Process("neuro", &cfg)
	if err != nil {
		log.Fatalf("error while parsing config: %s", err)
	}

	ln, err := net.Listen("tcp4", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("cannot listen for %q: %s", cfg.ListenAddr, err)
	}
	s := &http.Server{
		Handler:      http.HandlerFunc(handler),
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
		IdleTimeout:  time.Minute * 10,
	}
	log.Infof("Initing done. Listens on %q", cfg.ListenAddr)
	log.Fatalf("HTTP server error on %s: %s", cfg.ListenAddr, s.Serve(ln))
}

func handler(rw http.ResponseWriter, req *http.Request) {
	rw.WriteHeader(http.StatusOK)
	fmt.Fprint(rw, "Hello world!")
}
