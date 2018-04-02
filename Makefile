pkgs = $(shell go list ./...)

format:
	go fmt $(pkgs)
	gofmt -w -s .

build:
	go build

test: build
	go test -race $(pkgs)

run: build
	./platform-api