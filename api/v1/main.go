package v1

import (
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/neuromation/platform-api/api/v1/client/singularity"
	"github.com/neuromation/platform-api/api/v1/config"
	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/handlers"
	"github.com/neuromation/platform-api/api/v1/orchestrator"
	"github.com/neuromation/platform-api/api/v1/status"
	"github.com/neuromation/platform-api/api/v1/storage"
	"github.com/neuromation/platform-api/log"
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
		return fmt.Errorf("error while creating client: %s", err)
	}
	if err := client.Ping(time.Second * 10); err != nil {
		return fmt.Errorf("error while establishing connection: %s", err)
	}
	if err := storage.Init(cfg.StorageBasePath); err != nil {
		return fmt.Errorf("error while initing storage: %s", err)
	}

	statusService := status.NewStatusService()

	r := httprouter.New()
	r.GET("/", showHelp)
	r.GET("/models", listModels)
	r.POST("/trainings", createTraining)
	r.GET("/training/:id", viewTraining)
	r.GET("/status/training/:id", viewTrainingStatus)
	r.GET("/statuses/:id", handlers.ViewStatus(client, statusService))
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
	fmt.Fprintln(rw, "GET /status/training/:id")
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

func viewTrainingStatus(rw http.ResponseWriter, _ *http.Request, params httprouter.Params) {
	job := client.GetJob(params.ByName("id"))
	if job == nil {
		respondWithError(rw, fmt.Errorf("unable to find job %q", params.ByName("id")))
		return
	}
	status, err := job.Status()
	if err != nil {
		respondWithError(rw, fmt.Errorf("error while getting status for job %q: %s", params.ByName("id"), err))
		return
	}
	respondWith(rw, http.StatusOK, fmt.Sprintf(`{"status": %q}`, status))
}

func viewTraining(rw http.ResponseWriter, _ *http.Request, params httprouter.Params) {
	panic("implement me")
}

var userSpacePath = "./api/v1/testData/userSpace"

func createTraining(rw http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	tr := &training{}
	if err := decodeInto(req.Body, tr); err != nil {
		respondWithError(rw, err)
		return
	}

	// mount userSpace to `/var/user`
	// must be retrieved from userInfo in future
	path, err := filepath.Abs(userSpacePath)
	if err != nil {
		respondWithError(rw, fmt.Errorf("unable to find abs path %q: %s", path, err))
		return
	}
	us := container.Volume{
		From: path,
		To:   "/var/user",
		Mode: "RW",
	}
	tr.Container.Volumes = append(tr.Container.Volumes, us)

	job := client.NewJob(tr.Container, tr.Resources)
	if err := job.Start(); err != nil {
		respondWithError(rw, fmt.Errorf("error while creating training: %s", err))
		return
	}
	respondWithSuccess(rw, fmt.Sprintf("{\"job_id\": %q}", job.GetID()))
}
