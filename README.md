# platform-api

`Platform API` provides API for Neuromation MLaaS platform.

## How to install

* If you don't have Go installed on your system - follow [this guide](https://golang.org/doc/install).

* Install [dep](https://github.com/golang/dep) - dependency manager

* Clone project into your `$GOPATH/src/github.com/neuromation/platform-api` directory.

* Run `make run` command to run application

## How to run tests

* You should set environment variable DOCKER_SECRET with value of your docker credentials.
could be found in `~/.docker/config.json`

* Run `make test` command to test application