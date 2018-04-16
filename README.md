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

## build RAML file for api

install api-console

```
npm install -g api-console-cli
```

run api-console service

```
api-console build raml/platform-api.raml
api-console server build
```
