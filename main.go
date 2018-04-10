package main

import (
	"github.com/kelseyhightower/envconfig"
	api "github.com/neuromation/platform-api/api/v1"
	"github.com/neuromation/platform-api/api/v1/config"
	"github.com/neuromation/platform-api/log"
)

func main() {
	log.Infof("Initing...")
	cfg := &config.Config{}
	err := envconfig.Process("platformapi", cfg)
	if err != nil {
		log.Fatalf("error while parsing config: %s", err)
	}
	log.Infof("Initing done. Listening on %q", cfg.ListenAddr)
	log.Fatalf("HTTP server error on %s: %s", cfg.ListenAddr, api.Serve(cfg))
}
