package v1

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/neuromation/platform-api/api/v1/client/singularity"
	"github.com/neuromation/platform-api/api/v1/config"
	"github.com/neuromation/platform-api/api/v1/handlers"
	"github.com/neuromation/platform-api/api/v1/orchestrator"
	"github.com/neuromation/platform-api/api/v1/status"
	"github.com/neuromation/platform-api/api/v1/storage"
	"github.com/neuromation/platform-api/log"
)

var (
	// client - shared instance of orchestrator client
	client orchestrator.Client

	envPrefix string

	containerStoragePath string
)

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

	envPrefix = cfg.EnvPrefix
	containerStoragePath = cfg.ContainerStoragePath

	statusService := status.NewStatusService()

	r := httprouter.New()
	r.GET("/", showHelp)
	r.POST("/models", createModel(client, statusService))
	r.GET("/models/:id", viewTraining)
	r.GET("/statuses/:id", handlers.ViewStatus(statusService))

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
	fmt.Fprintln(rw, "POST /models")
	fmt.Fprintln(rw, "GET /models/%id")
	fmt.Fprintln(rw, "GET /statuses/:id")
}

func viewTraining(rw http.ResponseWriter, _ *http.Request, params httprouter.Params) {
	model := &struct {
		ModelId string `json:"model_id"`
	}{
		ModelId: params.ByName("id"),
	}
	payload, err := json.Marshal(model)
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	rw.Write(payload)
}

func createModel(jobClient orchestrator.Client, statusService status.StatusService) httprouter.Handle {
	return func(rw http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		m := &model{}
		if err := decodeInto(req.Body, m); err != nil {
			respondWithError(rw, err)
			return
		}

		job := client.NewJob(m.Container, m.Resources)
		if err := job.Start(); err != nil {
			respondWithError(rw, fmt.Errorf("error while creating training: %s", err))
			return
		}

		modelId := job.GetID()
		modelUrl := handlers.GenerateModelURLFromRequest(req, modelId)
		status := status.NewModelStatus(modelId, modelUrl.String(), client)
		if err := statusService.Set(status); err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		payload, err := json.Marshal(status)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		location := handlers.GenerateStatusURLFromRequest(req, status.Id())
		rw.Header().Set("Location", location.String())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusAccepted)
		rw.Write(payload)
	}
}

func envName(name string) string {
	if len(envPrefix) == 0 {
		return name
	}
	return fmt.Sprintf("%s_%s", envPrefix, name)
}
