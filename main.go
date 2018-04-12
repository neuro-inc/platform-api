package main

import (
	"github.com/kelseyhightower/envconfig"
	api "github.com/neuromation/platform-api/api/v1"
	"github.com/neuromation/platform-api/api/v1/config"
	"github.com/neuromation/platform-api/log"
)

func main() {
	cfg := &config.Config{}
	err := envconfig.Process("platformapi", cfg)
	if err != nil {
		log.Fatalf("error while parsing config: %s", err)
	}
	log.Fatalf("API serving error: %s", api.Serve(cfg))
}
