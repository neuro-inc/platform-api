package v1

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/neuromation/platform-api/api/v1/client/singularity"
	"github.com/neuromation/platform-api/api/v1/config"
	"github.com/neuromation/platform-api/api/v1/container"
	"github.com/neuromation/platform-api/api/v1/errors"
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
)

// Serve starts serving web-server for accepting requests
func Serve(cfg *config.Config) error {
	log.Infof("Starting...")
	log.Infof("Configuration: \n %s", cfg)
	ln, err := net.Listen("tcp4", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("cannot listen for %q: %s", cfg.ListenAddr, err)
	}
	if err := storage.Init(cfg.StorageBasePath); err != nil {
		return fmt.Errorf("error while initing storage: %s", err)
	}
	client, err = singularity.NewClient(cfg.SingularityAddr, cfg.PrivateDockerRegistryPath, cfg.WriteTimeout)
	if err != nil {
		return fmt.Errorf("error while creating client: %s", err)
	}
	if err := client.Ping(cfg.ClientConnectTimeout); err != nil {
		return fmt.Errorf("error while establishing connection: %s", err)
	}

	// set default path for container volumes
	container.SetPath(cfg.ContainerStoragePath)

	envPrefix = cfg.EnvPrefix
	statusService := status.NewStatusService()

	r := httprouter.New()
	r.GET("/", showHelp)
	r.POST("/models", createModel(client, statusService))
	r.POST("/batch-inference", createBatchInference(client, statusService))
	r.GET("/models/:id", viewModel)
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

func viewModel(rw http.ResponseWriter, _ *http.Request, params httprouter.Params) {
	model := &struct {
		ModelID string `json:"model_id"`
	}{
		ModelID: params.ByName("id"),
	}
	payload, err := json.Marshal(model)
	if err != nil {
		errors.Respond(rw, http.StatusInternalServerError, "Model processing error", err)
		return
	}
	rw.WriteHeader(http.StatusOK)
	rw.Write(payload)
}

func createBatchInference(jobClient orchestrator.Client, statusService status.StatusService) httprouter.Handle {
	return func(rw http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		bi := &batchInference{}
		if err := decodeInto(req.Body, bi); err != nil {
			errors.Respond(rw, http.StatusBadRequest, "Bad batch-inference request", err)
			return
		}

		job := client.NewJob(bi.Container, bi.Resources)
		if err := job.Start(); err != nil {
			errors.Respond(rw, http.StatusInternalServerError, "Unable to run batch-inference", err)
			return
		}

		bID := job.GetID()
		url := handlers.GenerateBatchInferenceURLFromRequest(req, bID)
		jobStatusPoller := orchestrator.NewJobStatusPoller(job)
		s := status.NewBatchInferenceStatus(
			bID, url.String(), jobStatusPoller)
		if err := statusService.Set(s); err != nil {
			errors.Respond(rw, http.StatusInternalServerError, "Unable to update batch-inference's status", err)
			return
		}

		payload, err := json.Marshal(s)
		if err != nil {
			errors.Respond(rw, http.StatusInternalServerError, "Batch-inference processing error", err)
			return
		}

		location := handlers.GenerateStatusURLFromRequest(req, s.Id())
		rw.Header().Set("Location", location.String())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusAccepted)
		rw.Write(payload)
	}
}

func createModel(jobClient orchestrator.Client, statusService status.StatusService) httprouter.Handle {
	return func(rw http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		m := &model{}
		if err := decodeInto(req.Body, m); err != nil {
			errors.Respond(rw, http.StatusBadRequest, "Bad model request", err)
			return
		}

		job := client.NewJob(m.Container, m.Resources)
		if err := job.Start(); err != nil {
			errors.Respond(rw, http.StatusInternalServerError, "Unable to run model", err)
			return
		}

		modelID := job.GetID()
		modelURL := handlers.GenerateModelURLFromRequest(req, modelID)
		jobStatusPoller := orchestrator.NewJobStatusPoller(job)
		status := status.NewModelStatus(
			modelID, modelURL.String(), jobStatusPoller)
		if err := statusService.Set(status); err != nil {
			errors.Respond(rw, http.StatusInternalServerError, "Unable to update model's status", err)
			return
		}

		payload, err := json.Marshal(status)
		if err != nil {
			errors.Respond(rw, http.StatusInternalServerError, "Model processing error", err)
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
