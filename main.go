package main

import (
	"net"
	"net/http"
	"time"

	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/kelseyhightower/envconfig"
	api "github.com/neuromation/platform-api/api/v1"
	"github.com/neuromation/platform-api/config"
	"github.com/neuromation/platform-api/log"
)

func main() {
	log.Infof("Initing...")
	cfg := &config.Config{}
	err := envconfig.Process("neuro", cfg)
	if err != nil {
		log.Fatalf("error while parsing config: %s", err)
	}

	//api.Init(cfg.SingularityAddr, cfg.SingularityTimeout)

	log.Infof("Initing done. Listens on %q", cfg.ListenAddr)
	serve(cfg)
}

func serve(cfg *config.Config) {
	router := httprouter.New()
	router.GET("/", index)
	router.GET("/models", listModels)
	router.GET("/storage", listStorage)

	ln, err := net.Listen("tcp4", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("cannot listen for %q: %s", cfg.ListenAddr, err)
	}
	s := &http.Server{
		Handler:      router,
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
		IdleTimeout:  time.Minute * 10,
	}

	log.Fatalf("HTTP server error on %s: %s", cfg.ListenAddr, s.Serve(ln))
}

func index(rw http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	fmt.Fprintln(rw, "Available endpoints:")
	fmt.Fprintln(rw, "GET /models")
	fmt.Fprintln(rw, "GET /storage")
}

func listModels(rw http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	api.ListModels(rw)
}

func listStorage(rw http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	api.ListStorage(rw)
}
