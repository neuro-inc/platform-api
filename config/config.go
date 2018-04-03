package config

import "time"

// Config contains description of app configured variables
// TODO: frankly, it's too excess way to describe configuration
// I'd prefer a way which stnd `flag` package provides
type Config struct {
	// Addr to listen for incoming requests
	ListenAddr string `default:":8080"`

	// Singularity app addr to proxy requests
	SingularityAddr string `default:"http://127.0.0.1:7099"`

	// Singularity timeout before cancelling request
	SingularityTimeout time.Duration `default:"1m"`
}
