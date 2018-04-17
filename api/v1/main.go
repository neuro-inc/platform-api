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
	"github.com/neuromation/platform-api/api/v1/orchestrator"
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

	r := httprouter.New()
	r.GET("/", showHelp)
	r.POST("/models", createModel)
	r.GET("/status/model/:id", viewModelStatus)
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
	fmt.Fprintln(rw, "GET /status/model/:id")
}

func viewModelStatus(rw http.ResponseWriter, _ *http.Request, params httprouter.Params) {
	job := client.GetJob(params.ByName("id"))
	if job == nil {
		respondWithError(rw, fmt.Errorf("unable to find model %q", params.ByName("id")))
		return
	}
	status, err := job.Status()
	if err != nil {
		respondWithError(rw, fmt.Errorf("error while getting status for model %q: %s", params.ByName("id"), err))
		return
	}
	respondWith(rw, http.StatusOK, fmt.Sprintf(`{"status": %q}`, status))
}

func createModel(rw http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	m := &model{}
	if err := decodeInto(req.Body, m); err != nil {
		respondWithError(rw, err)
		return
	}

	c := container.Container{
		Image: m.Image,
		Env:   m.Meta.Env,
	}
	// mount user default volume
	v, err := getUserVolume()
	if err != nil {
		respondWithError(rw, fmt.Errorf("error while getting user volume: %s", err))
		return
	}
	c.Volumes = append(c.Volumes, v)

	for _, s := range m.Storage {
		pi, err := storage.Path(s)
		if err != nil {
			respondWithError(rw, fmt.Errorf("invalid storage path: %s", err))
			return
		}
		v := &container.Volume{
			HostPath:      pi.Abs(),
			ContainerPath: "/var/marketplace/" + pi.Relative(),
			Mode:          "RO",
		}
		c.Volumes = append(c.Volumes, v)
	}

	r := make(container.Resources)
	r["cpus"] = float64(m.Meta.Resources.Cpus)
	r["memoryMb"] = float64(m.Meta.Resources.MemoryMB)

	job := client.NewJob(c, r)
	if err := job.Start(); err != nil {
		respondWithError(rw, fmt.Errorf("error while creating training: %s", err))
		return
	}
	respondWith(rw, http.StatusAccepted, fmt.Sprintf("{\"model_id\": %q}", job.GetID()))
}

// TODO: must be retrieved from user's prfoile in future
var userSpacePath = "./api/v1/testData/userSpace"

func getUserVolume() (*container.Volume, error) {
	path, err := filepath.Abs(userSpacePath)
	if err != nil {
		return nil, err
	}
	return &container.Volume{
		HostPath:      path,
		ContainerPath: "/var/user",
		Mode:          "RW",
	}, nil
}
