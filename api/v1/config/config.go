package config

import (
	"fmt"
	"reflect"
	"time"
)

// Config contains description of app configured variables
// TODO: frankly, it's too excess way to describe configuration
// I'd prefer a way which stnd `flag` package provides
type Config struct {
	// Addr to listen for incoming requests
	ListenAddr string `default:":8080"`

	// Singularity app addr to proxy requests
	SingularityAddr string `default:"http://127.0.0.1:7099"`

	// Server timeouts
	ReadTimeout  time.Duration `default:"1m"`
	WriteTimeout time.Duration `default:"1m"`
	IdleTimeout  time.Duration `default:"10m"`

	// StorageBasePath is the parent path for all `storage` mounts
	StorageBasePath string

	// ContainerStoragePath is a path in the container where to mount external storage links
	ContainerStoragePath string `default:"/var/storage"`

	// EnvPrefix contains a prefix for environment variable names
	EnvPrefix string

	// PrivateDockerRegistryPath contains a path to archived docker config
	// to access private docker registry
	//
	// @see http://mesosphere.github.io/marathon/docs/native-docker-private-registry.html
	PrivateDockerRegistryPath string `default:"file:///etc/docker.tar.gz"`
}

func (c Config) String() string {
	var res string
	v := reflect.ValueOf(c)
	for i := 0; i < v.NumField(); i++ {
		value := v.Field(i)
		typeField := v.Type().Field(i)
		res += fmt.Sprintf("    %s: %s\n", typeField.Name, value.Interface())
	}
	return res
}
