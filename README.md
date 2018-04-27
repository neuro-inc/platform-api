[![codecov](https://codecov.io/gh/neuromation/platform-api/branch/master/graph/badge.svg?token=UhSf3Bzfe0)](https://codecov.io/gh/neuromation/platform-api)
# platform-api

`Platform API` provides API for Neuromation MLaaS platform.

## Dependencies

* [lint](https://github.com/golang/lint) - linter for Go source code
* [dep](https://github.com/golang/dep) - dependency manager

## How to install

* If you don't have Go installed on your system - follow [this guide](https://golang.org/doc/install).

* Clone project into your `$GOPATH/src/github.com/neuromation/platform-api` directory.

* Run `make run` command to run application

* Run `make test` command to test application

## Configuring

The platform can be configured with environment variables. To avoid possible conflicts variables must be prefixed with `PLATFORMAPI_`.
Possible params:
* PLATFORMAPI_STORAGEBASEPATH - The parent path for all `storage` mounts (`required`)
* PLATFORMAPI_PRIVATEDOCKERREGISTRYPATH - Contains a path to archived docker config to access private docker registry (required for private docker registry)
* PLATFORMAPI_LISTENADDR - Addr to listen for incoming requests (default:":8080")
* PLATFORMAPI_SINGULARITYADDR - Singularity app addr to proxy requests (default:"http://127.0.0.1:7099")
* PLATFORMAPI_READTIMEOUT - Platform read timeout (default:"1m")
* PLATFORMAPI_WRITETIMEOUT - Platform write timeout (default:"1m")
* PLATFORMAPI_IDLETIMEOUT - Platform idle timeout (default:"10m")
* PLATFORMAPI_CONTAINERSTORAGEPATH - Path in the container where to mount external storage links (default:"/var/storage")
* PLATFORMAPI_ENVPREFIX - EnvPrefix contains a prefix for environment variable names

Required variables example:
```
PLATFORMAPI_STORAGEBASEPATH=/fileStorage/dev/storage
PLATFORMAPI_PRIVATEDOCKERREGISTRYPATH=file:///etc/docker.tar.gz
```

## Environment varialbes inside of task containers

The platform expose mounted volumes path as env variables inside of containers:
* PATH_RESULT - Storage URI where artifacts should be saved (ReadWrite volume)
* PATH_DATASET - Storage URI where dataset sits (ReadOnly volume)
* PATH_MODEL - Storage URI where model sits (ReadOnly volume)

## Exmaples

Examples of jobs could be found in [fixtures](https://github.com/neuromation/platform-api/tree/master/api/v1/testdata/fixtures) with prefix `integration.`