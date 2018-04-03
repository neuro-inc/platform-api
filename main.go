package main

import (
	"net"
	"net/http"
	"time"

	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/kelseyhightower/envconfig"
	api "github.com/neuromation/platform-api/api/v1"
	"github.com/neuromation/platform-api/config"
	"github.com/neuromation/platform-api/log"
	"io/ioutil"
)

func main() {
	log.Infof("Initing...")
	cfg := &config.Config{}
	err := envconfig.Process("neuro", cfg)
	if err != nil {
		log.Fatalf("error while parsing config: %s", err)
	}

	api.Init(cfg.SingularityAddr, cfg.SingularityTimeout)

	log.Infof("Initing done. Listens on %q", cfg.ListenAddr)
	serve(cfg)
}

func serve(cfg *config.Config) {
	router := httprouter.New()
	router.GET("/", index)
	router.GET("/models", listModels)
	router.GET("/storage", listStorage)
	router.POST("/trainings", createTraining)
	router.GET("/trainings/:id", viewTraining)

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
	fmt.Fprintln(rw, "POST /trainings")
	fmt.Fprintln(rw, "GET /trainings/%id")
}

func listModels(rw http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	api.ListModels(rw)
}

func listStorage(rw http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	api.ListStorage(rw)
}

func viewTraining(rw http.ResponseWriter, _ *http.Request, params httprouter.Params) {
	resp, err := api.ViewTraining(params.ByName("id"))
	if err != nil {
		respondWithError(rw, err)
		return
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		respondWithError(rw, err)
		return
	}
	resp.Body.Close()
	respondWith(rw, http.StatusOK, string(b))
}

func createTraining(rw http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	decoder := json.NewDecoder(req.Body)
	tr := &api.Training{}
	err := decoder.Decode(tr)
	if err != nil {
		respondWithError(rw, err)
		return
	}
	defer req.Body.Close()

	job, err := api.RunTraining(tr)
	if err != nil {
		respondWithError(rw, err)
		return
	}
	respondWith(rw, http.StatusOK, fmt.Sprintf("{\"job_id\": %q}", job.Deploy.RequestID))
}
