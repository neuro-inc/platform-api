package v1

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"path/filepath"

	"github.com/julienschmidt/httprouter"
	"github.com/neuromation/platform-api/api/v1/client/singularity"
	"github.com/neuromation/platform-api/api/v1/config"
	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/orchestrator"
	"github.com/neuromation/platform-api/api/v1/storage"
	"github.com/neuromation/platform-api/log"
	"time"
)

// client - shared instance of orchestrator client
var client orchestrator.Client

// Serve starts serving web-server for accepting requests
func Serve(cfg *config.Config) error {
	log.Infof("Starting...")
	ln, err := net.Listen("tcp4", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("cannot listen for %q: %s", cfg.ListenAddr, err)
	}
	client, err = singularity.NewClient(cfg.SingularityAddr, cfg.WriteTimeout)
	if err != nil {
		return fmt.Errorf("error while creating orchestrator client: %s", err)
	}
	if err := client.Ping(time.Second * 10); err != nil {
		return fmt.Errorf("client unable to establish connection: %s", err)
	}
	if err := storage.Init(cfg.StorageBasePath); err != nil {
		return fmt.Errorf("storage error: %s", err)
	}

	r := httprouter.New()
	r.GET("/", showHelp)
	r.GET("/models", listModels)
	r.POST("/trainings", createTraining)
	r.GET("/trainings/:id", viewTraining)
	s := &http.Server{
		Handler:      r,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}
	log.Infof("Started successfully. Listening on %q", cfg.ListenAddr)
	return s.Serve(ln)
}

func showHelp(rw http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	fmt.Fprintln(rw, "Available endpoints:")
	fmt.Fprintln(rw, "GET /models")
	fmt.Fprintln(rw, "POST /trainings")
	fmt.Fprintln(rw, "GET /trainings/%id")
}

func listModels(rw http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	fmt.Fprint(rw, "[")
	var i int
	for _, v := range modelRegistry {
		i++
		fmt.Fprint(rw, v)
		if i < len(modelRegistry)-1 {
			fmt.Fprint(rw, ",")
		}
	}
	fmt.Fprint(rw, "]")
}

func viewTraining(rw http.ResponseWriter, _ *http.Request, params httprouter.Params) {
	resp, err := ViewTraining(params.ByName("id"))
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
	tr := &training{}
	if err := decodeInto(req.Body, tr); err != nil {
		respondWithError(rw, err)
		return
	}

	// mount userSpace to `/var/storage`
	userSpacePath, err := filepath.Abs("./testData/userSpace")
	if err != nil {
		respondWithError(rw, fmt.Errorf("unable to find abs path %q: %s", userSpacePath, err))
		return
	}
	us := container.Storage{
		From: userSpacePath,
		To:   "/var/userSpace",
	}
	tr.Container.Storage = append(tr.Container.Storage, us)

	job := client.NewJob(tr.Container, tr.Resources)
	if err := job.Start(); err != nil {
		respondWithError(rw, fmt.Errorf("error while creating training: %s", err))
		return
	}
	respondWithSuccess(rw, fmt.Sprintf("{\"job_id\": %q}", job.GetID()))
}
