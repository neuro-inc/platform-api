package v1

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/neuromation/platform-api/api/v1/client/singularity"
	"github.com/neuromation/platform-api/api/v1/config"
	"github.com/neuromation/platform-api/api/v1/orchestrator"
	"github.com/neuromation/platform-api/log"
)

// client - shared instance of orchestrator client
var client orchestrator.Client

// Serve starts serving web-server for accepting requests
func Serve(cfg *config.Config) error {
	ln, err := net.Listen("tcp4", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("cannot listen for %q: %s", cfg.ListenAddr, err)
	}

	client, err = singularity.NewClient(cfg.SingularityAddr, cfg.WriteTimeout)
	if err != nil {
		return fmt.Errorf("error while creating orchestrator client: %s", err)
	}

	router := httprouter.New()
	router.GET("/", showHelp)
	router.GET("/models", listModels)
	router.GET("/storage", listStorage)
	router.POST("/trainings", createTraining)
	router.GET("/trainings/:id", viewTraining)

	s := &http.Server{
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}
	return s.Serve(ln)
}

func showHelp(rw http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	fmt.Fprintln(rw, "Available endpoints:")
	fmt.Fprintln(rw, "GET /models")
	fmt.Fprintln(rw, "GET /storage")
	fmt.Fprintln(rw, "POST /trainings")
	fmt.Fprintln(rw, "GET /trainings/%id")
}

// TODO: dry
func listModels(rw http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	fmt.Fprint(rw, "[")
	var i int
	for _, v := range modelRegistry {
		i++
		fmt.Fprint(rw, v)
		if i < len(storageRegistry)-1 {
			fmt.Fprint(rw, ",")
		}
	}
	fmt.Fprint(rw, "]")
}

func listStorage(rw http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	fmt.Fprint(rw, "[")
	var i int
	for _, v := range storageRegistry {
		i++
		fmt.Fprint(rw, v)
		if i < len(storageRegistry)-1 {
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
	decoder := json.NewDecoder(req.Body)
	training := &Training{}
	err := decoder.Decode(training)
	if err != nil {
		respondWithError(rw, err)
		return
	}
	defer req.Body.Close()
	job, err := runTraining(training)
	if err != nil {
		respondWithError(rw, err)
		return
	}
	respondWith(rw, http.StatusOK, fmt.Sprintf("{\"job_id\": %q}", job.GetID()))
}
